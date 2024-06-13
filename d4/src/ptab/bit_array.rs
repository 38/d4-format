use super::*;
use d4_framefile::{Blob, Directory};
use smallvec::{smallvec, SmallVec};
use std::fs::File;
use std::io::Result;
use std::sync::{Arc, Mutex};

use super::DecodeResult;
use crate::dict::{Dictionary, EncodeResult};
use crate::header::Header;

pub trait PrimaryTableMode: Sized {
    type HandleType: Send + ?Sized;
    fn get_mapping_address(this: &mut PrimaryTable<Self>) -> Result<*const u8>;
}

pub struct Reader;
pub struct Writer;
impl PrimaryTableMode for Reader {
    type HandleType = dyn AsRef<[u8]> + Send;
    fn get_mapping_address(this: &mut PrimaryTable<Self>) -> Result<*const u8> {
        let mapping_handle = if let Some(ref handle) = this.mapping_handle {
            handle
        } else {
            this.mapping_handle = Some(Arc::new(Mutex::new(this.data.mmap()?)));
            this.mapping_handle.as_ref().unwrap()
        };
        Ok(mapping_handle.lock().unwrap().as_ref().as_ptr())
    }
}

impl PrimaryTableMode for Writer {
    type HandleType = dyn AsMut<[u8]> + Send;
    fn get_mapping_address(this: &mut PrimaryTable<Self>) -> Result<*const u8> {
        let mapping_handle = if let Some(ref handle) = this.mapping_handle {
            handle
        } else {
            this.mapping_handle = Some(Arc::new(Mutex::new(this.data.mmap_mut()?)));
            this.mapping_handle.as_ref().unwrap()
        };
        Ok(mapping_handle.lock().unwrap().as_mut().as_ptr())
    }
}

pub struct PrimaryTable<M: PrimaryTableMode> {
    bit_width: usize,
    dictionary: Dictionary,
    data: Blob<File>,
    mapping_handle: Option<Arc<Mutex<M::HandleType>>>,
}

pub struct PartialPrimaryTable<M: PrimaryTableMode> {
    mapping_handle: Arc<Mutex<M::HandleType>>,
    dictionary: Dictionary,
    bit_width: usize,
    addr_start: usize,
    chunk_size: usize,
    name: String,
    start: u32,
    end: u32,
}
impl<M: PrimaryTableMode> PartialPrimaryTable<M> {
    pub fn region(&self) -> (&str, u32, u32) {
        (&self.name, self.start, self.end)
    }
    pub fn dict(&self) -> &Dictionary {
        &self.dictionary
    }
}

impl<M: PrimaryTableMode> PartialPrimaryTable<M> {
    #[allow(clippy::wrong_self_convention)]
    pub fn to_codec(&mut self) -> PrimaryTableCodec<M> {
        let base_offset = self.start as usize;
        let bit_width = self.bit_width;
        let dict = self.dictionary.clone();
        let slice =
            unsafe { std::slice::from_raw_parts_mut(self.addr_start as *mut u8, self.chunk_size) };
        PrimaryTableCodec {
            memory: slice,
            base_offset,
            bit_width,
            dict,
            mask: (1u32 << bit_width) - 1,
            _handle: self.mapping_handle.clone(),
        }
    }
}

impl<M: PrimaryTableMode> PrimaryTable<M> {
    #[allow(clippy::ptr_offset_with_cast)]
    pub fn split_chunk(
        &mut self,
        header: &Header,
        mut max_chunk_size: Option<usize>,
    ) -> Result<Vec<PartialPrimaryTable<M>>> {
        let data = M::get_mapping_address(self)?;

        if let Some(ref mut max_chunk_size) = max_chunk_size {
            *max_chunk_size -= *max_chunk_size % 8;
        }

        let bit_width = self.bit_width;

        let mut ret = vec![];
        let mut offset = 0;
        for chrom in header.chrom_list.iter() {
            let mut size = chrom.size;
            let mut start = 0;
            while size > 0 {
                let chunk_size = match max_chunk_size {
                    Some(size_limit) if size_limit < size => size_limit,
                    _ => size,
                };
                let nbytes = (chunk_size * bit_width + 7) / 8;
                ret.push(PartialPrimaryTable {
                    mapping_handle: self.mapping_handle.clone().unwrap(),
                    addr_start: unsafe { data.offset(offset as isize) as usize },
                    chunk_size: nbytes,
                    name: chrom.name.clone(),
                    start,
                    bit_width,
                    end: start + chunk_size as u32,
                    dictionary: self.dictionary.clone(),
                });
                size -= chunk_size;
                offset += nbytes;
                start += chunk_size as u32;
            }
        }
        Ok(ret)
    }
}
impl PrimaryTable<Writer> {
    pub(crate) fn create(directory: &mut Directory<File>, header: &Header) -> Result<Self> {
        let size = header.primary_table_size();
        let data = directory.create_blob(".ptab", size)?;
        Ok(PrimaryTable {
            dictionary: header.dictionary.clone(),
            bit_width: header.dictionary.bit_width(),
            data,
            mapping_handle: None,
        })
    }
}

impl PrimaryTable<Reader> {
    pub(crate) fn open(root_dir: &mut Directory<File>, header: &Header) -> Result<Self> {
        let chunk = root_dir.open_blob(PRIMARY_TABLE_NAME)?;
        Ok(PrimaryTable {
            dictionary: header.dictionary.clone(),
            bit_width: header.dictionary.bit_width(),
            data: chunk,
            mapping_handle: None,
        })
    }
}

pub struct PrimaryTableCodec<M: PrimaryTableMode> {
    _handle: Arc<Mutex<M::HandleType>>,
    base_offset: usize,
    bit_width: usize,
    mask: u32,
    memory: &'static mut [u8],
    dict: Dictionary,
}

pub struct MatrixDecoder {
    decoders: Vec<PrimaryTableCodec<Reader>>,
    state: DecoderParameter,
}

struct DecoderParameter {
    base_offset: usize,
    rule_base: usize,
    pointers: SmallVec<[*const u8; 32]>,
    shift: SmallVec<[usize; 32]>,
    delta: SmallVec<[usize; 32]>,
}

impl DecoderParameter {
    #[inline(always)]
    fn read_value(&mut self, idx: usize) -> u32 {
        let shift = self.shift[self.rule_base + idx];
        let data: &u32 = unsafe { &*(*self.pointers.get_unchecked(idx) as *const u32) };
        data >> shift
    }

    #[inline(always)]
    fn step_foward(&mut self) {
        for (i, pointer) in self.pointers.iter_mut().enumerate() {
            *pointer = unsafe { pointer.add(self.delta[self.rule_base + i]) };
        }
        self.rule_base += self.pointers.len();
        if self.rule_base >= self.delta.len() {
            self.rule_base -= self.delta.len();
        }
    }

    fn move_to_pos(&mut self, pos: usize, decoders: &[PrimaryTableCodec<Reader>]) {
        assert!(pos >= self.base_offset);
        if self.base_offset != pos {
            let n_period = (pos - self.base_offset) / 8;
            for (pointer, decoder) in self.pointers.iter_mut().zip(decoders) {
                *pointer = unsafe { pointer.add(n_period * decoder.bit_width) };
            }
            for _ in 0..(pos - self.base_offset) % 8 {
                self.step_foward();
            }
            self.base_offset = pos;
        }
    }

    fn new(decoders: &[PrimaryTableCodec<Reader>]) -> Self {
        let pointers = decoders
            .iter()
            .map(|enc| &enc.memory[0] as *const u8)
            .collect::<SmallVec<_>>();

        let mut shift = smallvec![];
        let mut delta = smallvec![];

        for idx in 0..8 {
            for encoder in decoders.iter() {
                let offset = idx;
                delta.push((offset + 1) * encoder.bit_width / 8 - offset * encoder.bit_width / 8);
                shift.push((offset * encoder.bit_width) % 8);
            }
        }

        Self {
            base_offset: decoders[0].base_offset,
            rule_base: 0,
            pointers,
            shift,
            delta,
        }
    }
}

impl MatrixDecoder {
    pub fn is_zero_sized(&self) -> bool {
        self.decoders.iter().all(|enc| enc.bit_width == 0)
    }
    pub fn new<'a, T: IntoIterator<Item = &'a mut PartialPrimaryTable<Reader>>>(
        decoders: T,
    ) -> Self {
        let decoders: Vec<_> = decoders.into_iter().map(|p| p.make_decoder()).collect();
        let state = DecoderParameter::new(&decoders);
        assert!(!decoders.is_empty());
        Self { decoders, state }
    }

    #[inline(always)]
    pub fn decode(&self, pos: u32, buf: &mut Vec<DecodeResult>) {
        buf.clear();
        for idx in 0..self.decoders.len() {
            buf.push(self.decoders[idx].decode(pos as usize));
        }
    }

    pub fn decode_block<H: FnMut(u32, &[DecodeResult]) -> bool>(
        &mut self,
        left: u32,
        right: u32,
        mut handle: H,
    ) {
        self.state.move_to_pos(left as usize, &self.decoders);

        let n_inputs = self.decoders.len();
        let mut result_buf = SmallVec::<[_; 16]>::with_capacity(n_inputs);

        for pos in left..right {
            result_buf.clear();
            for (idx, enc) in self.decoders.iter().enumerate() {
                let code = self.state.read_value(idx) & enc.mask;
                let result = if let Some(value) = enc.dict.decode_value(code) {
                    if code == enc.mask {
                        DecodeResult::Maybe(value)
                    } else {
                        DecodeResult::Definitely(value)
                    }
                } else {
                    DecodeResult::Maybe(0)
                };
                result_buf.push(result);
            }
            self.state.step_foward();
            if !handle(pos, &result_buf) {
                break;
            }
        }
        self.state.base_offset = right as usize;
    }
}

impl PrimaryTableCodec<Reader> {
    #[inline(always)]
    pub fn decode(&self, offset: usize) -> DecodeResult {
        if self.bit_width == 0 {
            return DecodeResult::Maybe(self.dict.first_value());
        }
        let actual_offset = offset - self.base_offset;
        let start: &u32 =
            unsafe { std::mem::transmute(&self.memory[actual_offset * self.bit_width / 8]) };
        let value = (*start >> ((actual_offset * self.bit_width) % 8)) & self.mask;
        if value == (1 << self.bit_width) - 1 {
            return DecodeResult::Maybe(self.dict.decode_value(value).unwrap_or(0));
        }
        match self.dict.decode_value(value) {
            Some(value) => DecodeResult::Definitely(value),
            None => DecodeResult::Maybe(0),
        }
    }
}

impl PrimaryTableCodec<Writer> {
    #[inline(always)]
    pub fn encode(&mut self, offset: usize, value: i32) -> bool {
        if self.bit_width == 0 {
            return matches!(
                self.dict.encode_value(value),
                EncodeResult::DictionaryIndex(_)
            );
        }
        let actual_offset = offset - self.base_offset;
        let start: &mut u32 =
            unsafe { std::mem::transmute(&mut self.memory[actual_offset * self.bit_width / 8]) };
        match self.dict.encode_value(value) {
            EncodeResult::DictionaryIndex(idx) => {
                *start |= idx << (actual_offset * self.bit_width % 8);
                true
            }
            _ => {
                *start |= ((1 << self.bit_width) - 1) << (actual_offset * self.bit_width % 8);
                false
            }
        }
    }
}
impl Encoder for PrimaryTableCodec<Writer> {
    #[inline(always)]
    fn encode(&mut self, offset: usize, value: i32) -> bool {
        PrimaryTableCodec::<Writer>::encode(self, offset, value)
    }
}
impl PTablePartitionWriter for PartialPrimaryTable<Writer> {
    type EncoderType = PrimaryTableCodec<Writer>;
    fn make_encoder(&mut self) -> Self::EncoderType {
        PartialPrimaryTable::to_codec(self)
    }
    fn region(&self) -> (&str, u32, u32) {
        PartialPrimaryTable::region(self)
    }
    fn can_encode(&self, value: i32) -> bool {
        matches!(
            self.dictionary.encode_value(value),
            EncodeResult::DictionaryIndex(_)
        )
    }
    fn bit_width(&self) -> usize {
        self.dictionary.bit_width()
    }
}
impl PrimaryTableWriter for PrimaryTable<Writer> {
    type Partition = PartialPrimaryTable<Writer>;
    fn create(directory: &mut Directory<File>, header: &Header) -> Result<Self> {
        PrimaryTable::<Writer>::create(directory, header)
    }

    fn split(
        &mut self,
        header: &Header,
        size_limit: Option<usize>,
    ) -> Result<Vec<Self::Partition>> {
        self.split_chunk(header, size_limit)
    }
}

impl Decoder for PrimaryTableCodec<Reader> {
    #[inline(always)]
    fn decode(&mut self, offset: usize) -> DecodeResult {
        PrimaryTableCodec::<Reader>::decode(self, offset)
    }

    fn decode_block<F: DecodeBlockHandle>(&mut self, pos: usize, count: usize, mut handle: F) {
        if self.bit_width == 0 {
            for pos in pos..pos + count {
                handle.handle(pos, DecodeResult::Maybe(self.dict.first_value()));
            }
        } else {
            let actual_offset = pos - self.base_offset;
            let mut addr_delta = [0usize; 8];
            let mut shift = [0usize; 8];
            let mask = self.mask;

            for idx in 1..9 {
                let offset = actual_offset + idx;
                addr_delta[idx % 8] =
                    offset * self.bit_width / 8 - (offset - 1) * self.bit_width / 8;
                shift[idx % 8] = offset * self.bit_width % 8;
            }

            shift[0] = (actual_offset * self.bit_width) % 8;
            let mut start = &self.memory[actual_offset * self.bit_width / 8] as *const u8;
            start = unsafe { start.offset(-(addr_delta[0] as isize)) };

            for idx in 0..count {
                start = unsafe { start.add(addr_delta[idx % 8]) };
                let value = unsafe { &*(start as *const u32) };
                let value = (value >> shift[idx % 8]) & mask;
                let result = if value == mask {
                    DecodeResult::Maybe(self.dict.decode_value(mask).unwrap_or(0))
                } else {
                    match self.dict.decode_value(value) {
                        Some(value) => DecodeResult::Definitely(value),
                        None => DecodeResult::Maybe(0),
                    }
                };
                handle.handle(pos + idx, result);
            }
        }
    }
}

impl PrimaryTablePartReader for PartialPrimaryTable<Reader> {
    type DecoderType = PrimaryTableCodec<Reader>;
    fn bit_width(&self) -> usize {
        self.bit_width
    }
    fn make_decoder(&mut self) -> Self::DecoderType {
        PartialPrimaryTable::to_codec(self)
    }
    fn region(&self) -> (&str, u32, u32) {
        PartialPrimaryTable::region(self)
    }
    fn default_value(&self) -> Option<i32> {
        if self.dictionary.bit_width() == 0 {
            Some(self.dictionary.first_value())
        } else {
            None
        }
    }
}
impl PrimaryTableReader for PrimaryTable<Reader> {
    type Partition = PartialPrimaryTable<Reader>;
    fn create(directory: &mut Directory<File>, header: &Header) -> Result<Self> {
        PrimaryTable::open(directory, header)
    }

    fn split(
        &mut self,
        header: &Header,
        size_limit: Option<usize>,
    ) -> Result<Vec<Self::Partition>> {
        self.split_chunk(header, size_limit)
    }
}
#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_split_handle_send() {
        fn check_send<T: Send>() {}
        check_send::<PartialPrimaryTable<Writer>>();
    }
}
