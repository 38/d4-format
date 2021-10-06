use super::*;
use d4_framefile::mode::{AccessMode, ReadOnly, ReadWrite};
use d4_framefile::{Blob, Directory};
use std::fs::File;
use std::io::Result;
use std::sync::{Arc, Mutex};

use super::DecodeResult;
use crate::dict::{Dictionary, EncodeResult};
use crate::header::Header;

pub trait PrimaryTableMode: Sized {
    type ChunkMode: AccessMode;
    type HandleType: Send + ?Sized;
    fn ensure_handle(this: &mut PrimaryTable<Self>) -> Result<()>;
    fn get_mapping_address(this: &mut PrimaryTable<Self>) -> *const u8;
}

pub struct Reader;
pub struct Writer;
impl PrimaryTableMode for Reader {
    type ChunkMode = ReadOnly;
    type HandleType = dyn AsRef<[u8]> + Send;
    fn ensure_handle(this: &mut PrimaryTable<Self>) -> Result<()> {
        if this.mapping_handle.is_none() {
            this.mapping_handle = Some(Arc::new(Mutex::new(this.data.mmap()?)));
        }
        Ok(())
    }
    fn get_mapping_address(this: &mut PrimaryTable<Self>) -> *const u8 {
        let data = this
            .mapping_handle
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .as_ref()
            .as_ptr();
        data
    }
}

impl PrimaryTableMode for Writer {
    type ChunkMode = ReadWrite;
    type HandleType = dyn AsMut<[u8]> + Send;
    fn ensure_handle(this: &mut PrimaryTable<Self>) -> Result<()> {
        if this.mapping_handle.is_none() {
            this.mapping_handle = Some(Arc::new(Mutex::new(this.data.mmap_mut()?)));
        }
        Ok(())
    }

    fn get_mapping_address(this: &mut PrimaryTable<Self>) -> *const u8 {
        let data = this
            .mapping_handle
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .as_mut()
            .as_ptr();
        data
    }
}

pub struct PrimaryTable<M: PrimaryTableMode> {
    bit_width: usize,
    dictionary: Dictionary,
    data: Blob<'static, M::ChunkMode, File>,
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
    pub fn to_codec(&mut self) -> PrimaryTableCodec<M> {
        let base_offset = self.start as usize;
        let bit_width = self.bit_width;
        let dict = self.dictionary.clone();
        let slice = unsafe {
            std::slice::from_raw_parts_mut(std::mem::transmute(self.addr_start), self.chunk_size)
        };
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
        M::ensure_handle(self)?;

        let data = M::get_mapping_address(self);

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
    pub(crate) fn create(
        directory: &mut Directory<'static, ReadWrite, File>,
        header: &Header,
    ) -> Result<Self> {
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
    pub(crate) fn open(
        root_dir: &mut Directory<'static, ReadOnly, File>,
        header: &Header,
    ) -> Result<Self> {
        let chunk = root_dir.open_blob(".ptab")?;
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
    encoders: Vec<PrimaryTableCodec<Reader>>,
}

impl MatrixDecoder {
    pub fn is_zero_sized(&self) -> bool {
        self.encoders.iter().all(|enc| enc.bit_width == 0)
    }
    pub fn new<'a, T: IntoIterator<Item = &'a mut PartialPrimaryTable<Reader>>>(
        encoders: T,
    ) -> Self {
        let encoders: Vec<_> = encoders.into_iter().map(|p| p.make_decoder()).collect();
        assert!(!encoders.is_empty());
        Self {
            encoders,
        }
    }
    pub fn decode_block<H: FnMut(u32, &[DecodeResult]) -> bool>(
        &self,
        left: u32,
        right: u32,
        mut handle: H,
    ) {
        let mut states = self
            .encoders
            .iter()
            .map(|enc| {
                let offset = left as usize - enc.base_offset;
                let base_addr = &enc.memory[offset * enc.bit_width / 8] as *const u8;
                base_addr
            })
            .collect::<Vec<_>>();
        
        let mut shift: Vec<usize> = vec![];
        let mut addr_diff: Vec<usize> = vec![];

        for idx in 0..8 {
            for encoder in self.encoders.iter() {
                let offset = left as usize - encoder.base_offset + idx;
                addr_diff.push((offset + 1) * encoder.bit_width / 8 - offset * encoder.bit_width / 8);
                shift.push((offset * encoder.bit_width) % 8);
            }
        }

        let mut result_buf = Vec::with_capacity(states.len());
        let n_inputs = states.len();
        let rule_cap = n_inputs * 8;
        let mut rule_base = 0;

        for pos in left..right {
            result_buf.clear();
            for (idx, (enc, mem)) in self.encoders.iter().zip(&mut states).enumerate() {
                let shift = shift[rule_base + idx];
                let data: &u32 = unsafe { std::mem::transmute(*mem) };
                let code = (*data >> shift) & enc.mask;
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
                *mem = unsafe { mem.add(addr_diff[rule_base + idx]) };
            }
            if !handle(pos, &result_buf) {
                return;
            }
            rule_base += n_inputs;
            if rule_base >= rule_cap {
                rule_base = 0;
            }
        }
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
impl PTableWriter for PrimaryTable<Writer> {
    type Partition = PartialPrimaryTable<Writer>;
    fn create(
        directory: &mut Directory<'static, ReadWrite, File>,
        header: &Header,
    ) -> Result<Self> {
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
    #[inline(always)]
    fn decode_block<F: FnMut(usize, DecodeResult)>(
        &mut self,
        pos: usize,
        count: usize,
        mut handle: F,
    ) {
        if self.bit_width == 0 {
            for pos in pos..pos + count {
                handle(pos, DecodeResult::Maybe(self.dict.first_value()));
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
                handle(pos + idx, result);
            }
        }
    }
}

impl PTablePartitionReader for PartialPrimaryTable<Reader> {
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
}
impl PTableReader for PrimaryTable<Reader> {
    type Partition = PartialPrimaryTable<Reader>;
    fn create(directory: &mut Directory<'static, ReadOnly, File>, header: &Header) -> Result<Self> {
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
