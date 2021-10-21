use d4_framefile::mapped::{MappedDirectory, MappedStreamFrame};
use d4_framefile::mode::ReadOnly;
use d4_framefile::Directory;

use super::record::Record;
use super::record_block::RecordBlock;
use super::SimpleKvMetadata;
use crate::stab::{RecordIterator, STablePartitionReader, STableReader};
use crate::Header;

use std::collections::HashMap;
use std::fs::File;
use std::io::Result;
use std::marker::PhantomData;
use std::sync::Arc;

use std::cell::RefCell;

/// The reader for simple sparse array based secondary table
pub struct SimpleKeyValueReader<R: Record> {
    s_table_root: Arc<MappedDirectory>,
    _p: PhantomData<R>,
}

/// The parallel partial reader for simple sparse array based secondary table
pub struct SimpleKeyValuePartialReader<R: Record> {
    _root: Arc<MappedDirectory>,
    records: Vec<RecordBlock<'static, R>>,
    cursor: (usize, usize),
    last_pos: Option<u32>,
    next: Option<R>,
}

impl<R: Record> SimpleKeyValueReader<R> {
    fn load_metadata(&mut self) -> Option<SimpleKvMetadata> {
        let metadata = self.s_table_root.get(".metadata")?;
        let metadata = String::from_utf8_lossy(metadata.copy_content().as_ref()).to_string();
        let actual_data = metadata.trim_end_matches(|c| c == '\0');
        serde_json::from_str(actual_data).ok()
    }
    fn assemble_incomplete_records<'a>(
        incomplete_data: &mut Vec<u8>,
        extra: &'a [u8],
        buffer: &mut Vec<RecordBlock<R>>,
    ) -> &'a [u8] {
        if !incomplete_data.is_empty() {
            let bytes_needed = R::SIZE - incomplete_data.len();
            incomplete_data.extend_from_slice(&extra[..bytes_needed]);
            buffer.push(RecordBlock::Record(*unsafe {
                std::mem::transmute::<_, &R>(&incomplete_data[0])
            }));
            incomplete_data.clear();
            return &extra[bytes_needed..];
        }
        extra
    }

    fn load_compressed_frame(
        frame: &MappedStreamFrame,
        first: bool,
        buffer: &mut Vec<RecordBlock<R>>,
    ) {
        let (is_compressed, first_pos, last_pos, block_count, data) = if first {
            (
                frame.data[0] == 0,
                u32::from_le_bytes([frame.data[1], frame.data[2], frame.data[3], frame.data[4]]),
                u32::from_le_bytes([frame.data[5], frame.data[6], frame.data[7], frame.data[8]]),
                u32::from_le_bytes([
                    frame.data[9],
                    frame.data[10],
                    frame.data[11],
                    frame.data[12],
                ]),
                &frame.data[13..],
            )
        } else {
            (
                true,
                u32::from_le_bytes([frame.data[0], frame.data[1], frame.data[2], frame.data[3]]),
                u32::from_le_bytes([frame.data[4], frame.data[5], frame.data[6], frame.data[7]]),
                u32::from_le_bytes([frame.data[8], frame.data[9], frame.data[10], frame.data[11]]),
                &frame.data[12..],
            )
        };
        if is_compressed {
            buffer.push(RecordBlock::CompressedBlock {
                raw: unsafe { std::mem::transmute(data) },
                start: first_pos,
                limit: last_pos,
                block_count: block_count as usize,
                unused: 0,
                decompressed: RefCell::new(vec![]),
            });
        } else {
            buffer.push(RecordBlock::Block(unsafe { std::mem::transmute(data) }));
        }
    }
    fn load_frame(
        frame: &MappedStreamFrame,
        mut excess: Vec<u8>,
        buffer: &mut Vec<RecordBlock<R>>,
    ) -> Vec<u8> {
        let data = Self::assemble_incomplete_records(&mut excess, &frame.data, buffer);
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
    fn load_record_blocks(&mut self) -> HashMap<String, Vec<RecordBlock<R>>> {
        let metadata = self.load_metadata().unwrap();

        let mut record_blocks: HashMap<String, Vec<RecordBlock<R>>> = HashMap::new();

        for stream in metadata.streams() {
            let chr = &stream.chr;
            // For each stream under the stab directory
            let stream = self.s_table_root.get(&stream.id).unwrap();
            let mut this_frame = Some(stream.get_primary_frame());

            // It's possible that one record is in the middle of frame boundary, thus we should have an extra buffer that
            // tracking the incomplete records
            let mut excess = vec![];
            let mut first = true;
            while let Some(frame) = this_frame {
                match metadata.compression {
                    super::compression::CompressionMethod::NoCompression => {
                        excess = Self::load_frame(
                            frame,
                            excess,
                            record_blocks.entry(chr.to_string()).or_default(),
                        );
                    }
                    super::compression::CompressionMethod::Deflate(_) => {
                        Self::load_compressed_frame(
                            frame,
                            first,
                            record_blocks.entry(chr.to_string()).or_default(),
                        );
                    }
                }
                this_frame = frame.next_frame();
                first = false;
            }

            // After we have done this stream, we need to strip the last invalid records if there is
            if let Some(record_block) = record_blocks.get_mut(chr) {
                if let Some(RecordBlock::Block(data)) = record_block.last_mut() {
                    if let Some((idx, _)) = data.iter().enumerate().find(|(_, rec)| !rec.is_valid())
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

impl<R: Record> SimpleKeyValuePartialReader<R> {
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

impl<R: Record> STablePartitionReader for SimpleKeyValuePartialReader<R> {
    type IteratorState = (usize, usize);
    fn iter(&self) -> RecordIterator<Self> {
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

impl<R: Record> STableReader for SimpleKeyValueReader<R> {
    type Partition = SimpleKeyValuePartialReader<R>;
    fn create(root: &mut Directory<'static, ReadOnly, File>, _header: &Header) -> Result<Self> {
        Ok(Self {
            s_table_root: Arc::new(root.map_directory(".stab")?),
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
        partitions.sort_by_key(|item: &PartitionContext<R>| (item.chrom, item.left));

        let mut chroms: Vec<_> = record_blocks.keys().map(ToOwned::to_owned).collect();
        chroms.sort();

        let mut current_part_id = 0;

        // For each block, we need figure out how we can split it.
        for (chrom, mut block) in chroms
            .into_iter()
            .map(move |chrom| {
                record_blocks
                    .remove(&chrom)
                    .unwrap()
                    .into_iter()
                    .map(move |block| (chrom.clone(), block))
            })
            .flatten()
        {
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
                let (left, right) = (block_min.max(cur_part.left), block_max.min(cur_part.right));

                if right - left == 0 {
                    break;
                }

                // FIXME: at this point, if there's an interval that is covers two partition, this will cause problems, since the
                // split point might be problematic
                let mut left_idx = if block_min == left {
                    0
                } else {
                    match block.binary_search_by_key(left, |r| r.effective_range().0) {
                        Ok(idx) => idx,
                        Err(idx) => idx,
                    }
                };
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
            .zip(
                partitions
                    .into_iter()
                    .map(|part| SimpleKeyValuePartialReader {
                        _root: root.clone(),
                        records: unsafe { std::mem::transmute(part.blocks) },
                        cursor: (0, 0),
                        next: None,
                        last_pos: None,
                    }),
            )
            .collect();
        buffer.sort_by_key(|item| item.0);
        Ok(buffer.into_iter().map(|item| item.1).collect())
    }
}
