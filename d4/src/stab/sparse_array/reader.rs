use super::record::Record;
use super::record_block::RecordBlock;
use std::cell::RefCell;
use std::marker::PhantomData;

use super::CompressionMethod;

pub(crate) fn assemble_incomplete_records<'a, R: Record>(
    incomplete_data: &mut Vec<u8>,
    extra: &'a [u8],
    buffer: &mut Vec<RecordBlock<'a, R>>,
) -> &'a [u8] {
    if !incomplete_data.is_empty() {
        let bytes_needed = R::SIZE - incomplete_data.len();
        incomplete_data.extend_from_slice(&extra[..bytes_needed]);
        let record = *unsafe { std::mem::transmute::<_, &R>(&incomplete_data[0]) };
        buffer.push(RecordBlock::Record(record));
        incomplete_data.clear();
        return &extra[bytes_needed..];
    }
    extra
}

pub(crate) fn load_compressed_frame<'a, R: Record>(
    frame: &'a [u8],
    first: bool,
    buffer: &mut Vec<RecordBlock<'a, R>>,
) {
    let (is_compressed, first_pos, last_pos, block_count, data) = if first {
        (
            frame[0] == 0,
            u32::from_le_bytes([frame[1], frame[2], frame[3], frame[4]]),
            u32::from_le_bytes([frame[5], frame[6], frame[7], frame[8]]),
            u32::from_le_bytes([frame[9], frame[10], frame[11], frame[12]]),
            &frame[13..],
        )
    } else {
        (
            true,
            u32::from_le_bytes([frame[0], frame[1], frame[2], frame[3]]),
            u32::from_le_bytes([frame[4], frame[5], frame[6], frame[7]]),
            u32::from_le_bytes([frame[8], frame[9], frame[10], frame[11]]),
            &frame[12..],
        )
    };
    if is_compressed {
        buffer.push(RecordBlock::CompressedBlock {
            raw: data,
            start: first_pos,
            limit: last_pos,
            block_count: block_count as usize,
            unused: 0,
            decompressed: RefCell::new(vec![]),
        });
    } else {
        buffer.push(RecordBlock::Block(unsafe {
            std::slice::from_raw_parts(data.as_ptr() as *const R, block_count as usize)
        }));
    }
}
pub(crate) fn load_frame<'a, R: Record>(
    frame: &'a [u8],
    mut excess: Vec<u8>,
    buffer: &mut Vec<RecordBlock<'a, R>>,
) -> Vec<u8> {
    let data = assemble_incomplete_records(&mut excess, frame, buffer);
    let rem = data.len() % R::SIZE;
    if data.len() > R::SIZE {
        buffer.push(RecordBlock::Block(unsafe {
            std::slice::from_raw_parts(data.as_ptr() as *const R, (data.len() - rem) / R::SIZE)
        }));
    }
    excess.clear();
    excess.extend_from_slice(&data[data.len() - rem..]);
    excess
}

pub(crate) struct RecordBlockParsingState<R: Record> {
    compression: CompressionMethod,
    excess: Vec<u8>,
    first: bool,
    skip_bytes: usize,
    _phantom: PhantomData<R>,
}

impl<R: Record> RecordBlockParsingState<R> {
    pub fn first_record_offset(&self) -> usize {
        let size: usize = std::mem::size_of::<R>();
        (size - self.excess.len()) % size
    }
    pub fn parse_frame<'a>(&mut self, data: &'a [u8], buf: &mut Vec<RecordBlock<'a, R>>) {
        let data = &data[self.skip_bytes..];
        match self.compression {
            CompressionMethod::NoCompression => {
                self.excess = load_frame(data, std::mem::take(&mut self.excess), buf);
            }
            CompressionMethod::Deflate(_) => {
                load_compressed_frame(data, self.first, buf);
            }
        }
        self.skip_bytes = 0;
        self.first = false;
    }

    pub fn set_skip_bytes(mut self, bytes: usize) -> Self {
        self.skip_bytes = bytes;
        self
    }

    pub fn new(compression: CompressionMethod) -> Self {
        Self {
            compression,
            excess: Default::default(),
            first: true,
            skip_bytes: 0,
            _phantom: Default::default(),
        }
    }

    pub fn set_is_first_frame(mut self, value: bool) -> Self {
        self.first = value;
        self
    }
}

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
pub use mapped_io::*;

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
mod mapped_io {
    use std::{collections::HashMap, fs::File, io::Result, marker::PhantomData, sync::Arc};

    use d4_framefile::{mapped::MappedDirectory, Directory};

    use crate::{
        stab::{
            sparse_array::{record_block::RecordBlock, Record},
            RecordIterator, SecondaryTablePartReader, SecondaryTableReader, SparseArraryMetadata,
            SECONDARY_TABLE_METADATA_NAME, SECONDARY_TABLE_NAME,
        },
        Header,
    };

    use super::RecordBlockParsingState;

    /// The reader for simple sparse array based secondary table
    pub struct SparseArrayReader<R: Record> {
        s_table_root: Arc<MappedDirectory>,
        _p: PhantomData<R>,
    }

    /// The parallel partial reader for simple sparse array based secondary table
    pub struct SparseArrayPartReader<R: Record> {
        // We need to hold the mapped memory,
        // thus we have to hold a ticket of the root directory to prevent
        // the mapped memory from being unmapped
        _root: Arc<MappedDirectory>,
        // As the directory ticket has been hold for the entire lifetime
        // for this object plus we never leak any reference of record to
        // the other part of the code. So the 'static lifetime won't be
        // seen by any other code. By doing so, there's no difference for
        // the object to have 'static lifetime or whatever lifetime self have.
        // Thus, we cast the record block to have static lifetime.
        records: Vec<RecordBlock<'static, R>>,
        cursor: (usize, usize),
        last_pos: Option<u32>,
        next: Option<R>,
    }

    impl<R: Record> SparseArrayReader<R> {
        fn load_metadata(&mut self) -> Option<SparseArraryMetadata> {
            let metadata = self
                .s_table_root
                .open_stream(SECONDARY_TABLE_METADATA_NAME)?;
            let metadata = String::from_utf8_lossy(
                {
                    let mut buf = Vec::new();
                    metadata.copy_content(&mut buf);
                    buf
                }
                .as_ref(),
            )
            .to_string();
            let actual_data = metadata.trim_end_matches(|c| c == '\0');
            serde_json::from_str(actual_data).ok()
        }
        fn load_record_blocks(&mut self) -> HashMap<String, Vec<RecordBlock<'_, R>>> {
            let metadata = self.load_metadata().unwrap();

            let mut record_blocks: HashMap<String, Vec<RecordBlock<'_, R>>> = HashMap::new();

            for stream in metadata.streams() {
                let chr = &stream.chr;
                // For each stream under the stab directory
                let stream = self.s_table_root.open_stream(&stream.id).unwrap();

                let mut next_frame = Some(stream.get_primary_frame());

                let mut state = RecordBlockParsingState::new(metadata.compression);
                let buffer = record_blocks.entry(chr.to_string()).or_default();

                while let Some(this_frame) = next_frame {
                    state.parse_frame(this_frame.as_ref(), buffer);
                    next_frame = this_frame.next_frame();
                }

                // After we have done this stream, we need to strip the last invalid records if there is
                if let Some(record_block) = record_blocks.get_mut(chr) {
                    if let Some(RecordBlock::Block(data)) = record_block.last_mut() {
                        if let Some((idx, _)) =
                            data.iter().enumerate().find(|(_, rec)| !rec.is_valid())
                        {
                            *data = &data[..idx];
                        }
                        if data.is_empty() {
                            record_block.pop();
                        }
                    }
                }
            }
            record_blocks
        }
    }

    impl<R: Record> SparseArrayPartReader<R> {
        #[inline(always)]
        fn load_cache(&mut self, inc: bool) {
            if inc {
                self.cursor.1 += 1;
                if self.cursor.1 >= self.records[self.cursor.0].count() {
                    self.cursor.0 += 1;
                    self.cursor.1 = 0;
                }
                self.last_pos = self.next.map(|what| what.effective_range().1);
            } else {
                self.last_pos = None;
            }
            if self.cursor.0 < self.records.len() {
                self.next = Some(self.records[self.cursor.0].get(self.cursor.1));
            }
        }
        #[inline(never)]
        #[allow(clippy::never_loop)]
        fn seek(&mut self, pos: u32, seq_hint: bool) -> Option<i32> {
            let idx = loop {
                if self.cursor.0 < self.records.len() && seq_hint {
                    let (_, r) = self.records[self.cursor.0].range();
                    if pos < r {
                        break self.cursor.0;
                    }
                    if self.cursor.0 + 1 < self.records.len()
                        && pos < self.records[self.cursor.0 + 1].range().1
                    {
                        break self.cursor.0 + 1;
                    }
                }
                break match self
                    .records
                    .binary_search_by_key(&pos, |block| block.get(0).effective_range().0)
                {
                    Ok(idx) => idx,
                    Err(idx) if idx > 0 && idx < self.records.len() => idx - 1,
                    _ => {
                        if self.records.last().map_or(false, |last_block| {
                            pos < last_block.get(last_block.count() - 1).effective_range().1
                        }) {
                            self.records.len() - 1
                        } else {
                            return None;
                        }
                    }
                };
            };

            let len = self.records[idx].count();

            match self.records[idx]
                .as_ref()
                .binary_search_by_key(&pos, |rec| rec.effective_range().1)
            {
                Ok(blk_idx) if blk_idx < len - 1 => {
                    let item = self.records[idx].get(blk_idx + 1);
                    self.cursor = (idx, blk_idx + 1);
                    if item.in_range(pos) {
                        self.load_cache(!item.in_range(pos + 1));
                        Some(item.value())
                    } else {
                        self.load_cache(false);
                        None
                    }
                }
                Err(blk_idx) if blk_idx < len => {
                    let item = self.records[idx].get(blk_idx);
                    self.cursor = (idx, blk_idx);
                    if item.in_range(pos) {
                        self.load_cache(!item.in_range(pos + 1));
                        Some(item.value())
                    } else {
                        self.load_cache(false);
                        None
                    }
                }
                _ => None,
            }
        }
    }

    impl<R: Record> SecondaryTablePartReader for SparseArrayPartReader<R> {
        type IteratorState = (usize, usize);
        fn iter(&self) -> RecordIterator<'_, Self> {
            RecordIterator(self, self.cursor)
        }
        fn next_record(&self, state: &mut Self::IteratorState) -> Option<(u32, u32, i32)> {
            if let Some(record) = self
                .records
                .get(state.0)
                .and_then(|block| block.as_ref().get(state.1))
            {
                state.1 += 1;
                if state.1 >= self.records[state.0].count() {
                    state.0 += 1;
                    state.1 = 0;
                }
                let (from, to) = record.effective_range();
                let value = record.value();
                return Some((from, to, value));
            }
            None
        }
        fn seek_state(&self, pos: u32) -> (usize, usize) {
            let block_idx = match self
                .records
                .binary_search_by_key(&pos, |block| block.get(0).effective_range().0)
            {
                Ok(idx) => idx,
                Err(idx) => {
                    if idx > 0 {
                        idx - 1
                    } else {
                        0
                    }
                }
            };

            if block_idx >= self.records.len() {
                return (0, 0);
            }

            let len = self.records[block_idx].count();

            match self.records[block_idx]
                .as_ref()
                .binary_search_by_key(&pos, |rec| rec.effective_range().1)
            {
                Err(rec_idx) => {
                    if rec_idx < len {
                        (block_idx, rec_idx)
                    } else {
                        (block_idx + 1, 0)
                    }
                }
                Ok(rec_idx) => {
                    if rec_idx + 1 < len {
                        (block_idx, rec_idx + 1)
                    } else {
                        (block_idx + 1, 0)
                    }
                }
            }
        }
        #[inline(always)]
        fn decode(&mut self, pos: u32) -> Option<i32> {
            if let Some(ref next) = self.next {
                if next.in_range(pos) {
                    let ret = next.value();
                    if !next.in_range(pos + 1) {
                        self.load_cache(true);
                    }
                    return Some(ret);
                }
            }
            if let Some(ref next) = self.next {
                if self.last_pos.map_or(false, |last_pos| {
                    last_pos < pos && pos < next.effective_range().0
                }) {
                    // Since in this case, we definitely know there's nothing could be found
                    return None;
                }
            }
            self.seek(pos, true)
        }
    }
    struct PartitionContext<'a, R: Record> {
        chrom: &'a str,
        left: u32,
        right: u32,
        blocks: Vec<RecordBlock<'a, R>>,
    }

    impl<'a, R: Record> PartitionContext<'a, R> {
        fn new(chrom: &'a str, left: u32, right: u32) -> Self {
            Self {
                chrom,
                left,
                right,
                blocks: vec![],
            }
        }

        fn is_overlapping(&self, chrom: &str, left: u32, right: u32) -> bool {
            chrom == self.chrom && !(self.right < left || right < self.left)
        }
    }

    impl<R: Record> SecondaryTableReader for SparseArrayReader<R> {
        type Partition = SparseArrayPartReader<R>;
        fn create(root: &mut Directory<File>, _header: &Header) -> Result<Self> {
            Ok(Self {
                s_table_root: Arc::new(root.map_directory(SECONDARY_TABLE_NAME)?),
                _p: PhantomData,
            })
        }

        fn split(&mut self, partitions: &[(&str, u32, u32)]) -> Result<Vec<Self::Partition>> {
            let root = self.s_table_root.clone();
            let mut record_blocks = self.load_record_blocks();

            let mut displacement: Vec<_> = (0..partitions.len()).collect();
            displacement.sort_by_key(move |idx| partitions[*idx]);
            let mut partitions: Vec<_> = partitions
                .iter()
                .map(|(c, l, r)| PartitionContext::new(c, *l, *r))
                .collect();
            partitions.sort_by_key(|item: &PartitionContext<'_, R>| (item.chrom, item.left));

            let mut chroms: Vec<_> = record_blocks.keys().map(ToOwned::to_owned).collect();
            chroms.sort();

            let mut current_part_id = 0;

            // For each block, we need figure out how we can split it.
            for (chrom, mut block) in chroms.into_iter().flat_map(move |chrom| {
                record_blocks
                    .remove(&chrom)
                    .unwrap()
                    .into_iter()
                    .map(move |block| (chrom.clone(), block))
            }) {
                let (block_min, block_max) = block.range();
                while current_part_id < partitions.len()
                    && (partitions[current_part_id].chrom < chrom.as_str()
                        || partitions[current_part_id].right < block_min)
                {
                    current_part_id += 1;
                }
                if current_part_id >= partitions.len() {
                    break;
                }

                let mut cur_part = &mut partitions[current_part_id];

                while cur_part.is_overlapping(&chrom, block_min, block_max) {
                    let (left, right) =
                        (block_min.max(cur_part.left), block_max.min(cur_part.right));

                    if right - left == 0 {
                        break;
                    }

                    let mut left_idx = if block_min == left {
                        0
                    } else {
                        match block.binary_search_by_key(left, |r| r.effective_range().0) {
                            Ok(idx) => idx,
                            Err(idx) => idx,
                        }
                    };
                    if left_idx > 0 && left < block.get(left_idx - 1).effective_range().1 {
                        left_idx -= 1;
                    }
                    let right_idx = if block_max == right {
                        block.count()
                    } else {
                        match block.binary_search_by_key(right, |r| r.effective_range().1) {
                            Ok(idx) => idx + 1,
                            Err(idx) => idx,
                        }
                    };

                    if block_min != left {
                        let left_rec = block.get(left_idx);
                        let (left_rec_min, left_rec_max) = left_rec.effective_range();
                        if right < left_rec_max {
                            if left_rec_min < right {
                                cur_part.blocks.push(RecordBlock::Record(
                                    left_rec
                                        .limit_left(left)
                                        .unwrap()
                                        .limit_right(right)
                                        .unwrap(),
                                ));
                            }
                        } else {
                            cur_part
                                .blocks
                                .push(RecordBlock::Record(left_rec.limit_left(left).unwrap()));
                        }
                        left_idx += 1;
                    }

                    if left_idx < right_idx {
                        let (mut head, mut tail) = (None, None);

                        if left_idx != 0 {
                            let mut buf = None;
                            block.split_by_size(left_idx, &mut None, &mut buf);
                            block = buf.unwrap();
                        }

                        let right_record = if right == block_max || right_idx >= block.count() {
                            None
                        } else {
                            let right_record = block.get(right_idx);
                            let (right_record_min, _) = right_record.effective_range();

                            if right < right_record_min {
                                None
                            } else {
                                right_record.limit_right(right)
                            }
                        };

                        block.split_by_size(right_idx - left_idx, &mut head, &mut tail);

                        if let Some(head) = head {
                            cur_part.blocks.push(head);
                        }

                        if let Some(right_block) = right_record {
                            cur_part.blocks.push(RecordBlock::Record(right_block));
                        }

                        if let Some(tail) = tail {
                            block = tail;
                        } else {
                            break;
                        }
                    }

                    if right == cur_part.right {
                        current_part_id += 1;
                    } else {
                        break;
                    }
                    if let Some(next_part) = partitions.get_mut(current_part_id) {
                        cur_part = next_part;
                    } else {
                        break;
                    }
                }
            }
            let mut buffer: Vec<_> = displacement
                .into_iter()
                .zip(partitions.into_iter().map(|part| SparseArrayPartReader {
                    _root: root.clone(),
                    records: unsafe { std::mem::transmute(part.blocks) },
                    cursor: (0, 0),
                    next: None,
                    last_pos: None,
                }))
                .collect();
            buffer.sort_by_key(|item| item.0);
            Ok(buffer.into_iter().map(|item| item.1).collect())
        }
    }
}
