use framefile::mapped::MappedDirectory;
use framefile::mode::ReadWrite;
use framefile::{Directory, Stream};

use std::sync::Arc;

use super::*;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::marker::PhantomData;

#[derive(Serialize, Deserialize)]
struct SimpleKVMetadata {
    format: String,
    record_format: String,
    partitions: Vec<(String, u32, u32)>,
}

pub struct SimpleKeyValueWriter<R: Record = SingleRecord>(
    Directory<'static, ReadWrite, File>,
    PhantomData<R>,
);
impl<R: Record> STableWriter for SimpleKeyValueWriter<R> {
    type Partition = SimpleKeyValuePartialWriter<R>;
    fn create(root: &mut Directory<'static, ReadWrite, File>, _header: &Header) -> Result<Self> {
        Ok(SimpleKeyValueWriter(
            root.new_stream_cluster(".stab")?,
            PhantomData,
        ))
    }
    fn split(
        &mut self,
        partitions: &[(&str, u32, u32)],
    ) -> Result<Vec<SimpleKeyValuePartialWriter<R>>> {
        let metadata = SimpleKVMetadata {
            format: "SimpleKV".to_string(),
            record_format: R::FORMAT_NAME.to_string(),
            partitions: {
                partitions
                    .iter()
                    .map(|(chr, start, end)| (chr.to_string(), *start, *end))
                    .collect()
            },
        };
        let mut metadata_stream = self.0.new_variant_length_stream(".metadata", 512)?;
        metadata_stream.write(serde_json::to_string(&metadata).unwrap().as_bytes())?;
        Ok(partitions
            .iter()
            .enumerate()
            .map(|(idx, _)| SimpleKeyValuePartialWriter {
                stream: self
                    .0
                    .new_variant_length_stream(format!("{}", idx).as_ref(), 512)
                    .unwrap(),
                pending_record: None,
            })
            .collect())
    }
}
pub struct SimpleKeyValuePartialWriter<R: Record> {
    stream: Stream<'static, ReadWrite, File>,
    pending_record: Option<R>,
}

impl<R: Record> STablePartitionWriter for SimpleKeyValuePartialWriter<R> {
    #[inline(always)]
    fn flush(&mut self) -> Result<()> {
        if let Some(record) = self.pending_record {
            let buffer = record.as_bytes();
            self.stream.write_with_alloc_callback(buffer, |s| {
                s.disable_pre_alloc();
                s.double_frame_size(2 * 1024 * 1024);
            })?;
            self.pending_record = None;
        }
        Ok(())
    }
    fn encode(&mut self, pos: u32, value: i32) -> Result<()> {
        if let Some(new_pending) = R::encode(self.pending_record.as_mut(), pos, value) {
            self.flush()?;
            self.pending_record = Some(new_pending);
        }
        Ok(())
    }
}

pub struct SimpleKeyValueReader<R: Record = SingleRecord> {
    s_table_root: Arc<MappedDirectory>,
    _p: PhantomData<R>,
}

impl<R: Record> SimpleKeyValueReader<R> {
    fn load_record_blocks(&mut self) -> Result<HashMap<String, Vec<RecordBlock<R>>>> {
        let metadata = self.s_table_root.get(".metadata").unwrap();
        let metadata = String::from_utf8_lossy(metadata.copy_content().as_ref()).to_string();
        let actual_data = metadata.trim_end_matches(|c| c == '\0');
        let metadata: SimpleKVMetadata = serde_json::from_str(actual_data).unwrap();
        let mut record_blocks: HashMap<String, Vec<RecordBlock<R>>> = HashMap::new();
        for (idx, (chr, _begin, _end)) in metadata.partitions.iter().enumerate() {
            let stream = self.s_table_root.get(format!("{}", idx).as_ref()).unwrap();
            let mut this_frame = Some(stream.get_primary_frame());
            let mut excess = vec![];
            while let Some(frame) = this_frame {
                let mut data = &frame.data;
                if !excess.is_empty() {
                    let bytes_needed = R::SIZE - excess.len();
                    excess.extend_from_slice(&data[..bytes_needed]);
                    record_blocks.entry(chr.to_string()).or_insert(vec![]).push(
                        RecordBlock::Record(*unsafe { std::mem::transmute::<_, &R>(&excess[0]) }),
                    );
                    data = &data[bytes_needed..];
                }
                let rem = data.len() % R::SIZE;
                if data.len() > R::SIZE {
                    record_blocks.entry(chr.to_string()).or_insert(vec![]).push(
                        RecordBlock::Block(unsafe {
                            std::slice::from_raw_parts(
                                data.as_ptr() as *const R,
                                (data.len() - rem) / R::SIZE,
                            )
                        }),
                    );
                }
                excess.clear();
                excess.extend_from_slice(&data[data.len() - rem..]);
                this_frame = frame.next_frame();
            }
            let should_pop =
                if let Some(record) = record_blocks.get_mut(chr).and_then(|l| l.last_mut()) {
                    match record {
                        RecordBlock::Block(data) => {
                            let mut valid_count = 0;
                            for i in 0..data.len() {
                                if !data[i].is_valid() {
                                    break;
                                }
                                valid_count += 1;
                            }
                            *data = &data[..valid_count];
                        }
                        _ => {}
                    }
                    record.count() == 0
                } else {
                    false
                };
            if should_pop {
                record_blocks.get_mut(chr).unwrap().pop();
            }
        }
        Ok(record_blocks)
    }
}

impl<R: Record> STableReader for SimpleKeyValueReader<R> {
    type Partition = SimpleKeyValuePartialReader<R>;
    fn create(root: &mut Directory<'static, ReadOnly, File>, _header: &Header) -> Result<Self> {
        Ok(Self {
            s_table_root: Arc::new(root.map_cluster_ro(".stab")?),
            _p: PhantomData,
        })
    }
    fn split(&mut self, partitions: &[(&str, u32, u32)]) -> Result<Vec<Self::Partition>> {
        let record_blocks = self.load_record_blocks()?;

        let mut displacement: Vec<_> = (0..partitions.len()).collect();
        displacement.sort_by_key(move |idx| partitions[*idx]);
        let mut partitions: Vec<(&str, u32, u32)> = partitions.to_owned();
        partitions.sort();

        let mut part_blocks: Vec<Vec<RecordBlock<R>>> = partitions.iter().map(|_| vec![]).collect();

        let mut chroms: Vec<&String> = record_blocks.keys().collect();
        chroms.sort();

        let mut idx = 0;

        for chrom in chroms {
            if partitions.len() <= idx {
                break;
            }
            while partitions.len() > idx && partitions[idx].0 < chrom {
                idx += 1;
            }

            if partitions.len() <= idx || partitions[idx].0 != chrom {
                continue;
            }
            for block in record_blocks[chrom].iter() {
                let first = block.get(0).effective_range().0;
                let last = block.get(block.count() - 1).effective_range().1;
                let mut split_plan: Vec<(usize, (usize, u32), (usize, u32))> = vec![];
                while idx < partitions.len()
                    && partitions[idx].0 == chrom
                    && partitions[idx].1 < last
                {
                    let left = partitions[idx].1.max(first);
                    let right = partitions[idx].2.min(last);
                    let left_idx = match block
                        .as_ref()
                        .binary_search_by_key(&left, |r| r.effective_range().0)
                    {
                        Ok(idx) => idx,
                        Err(idx) => idx,
                    };
                    let right_idx = match block
                        .as_ref()
                        .binary_search_by_key(&right, |r| r.effective_range().1 - 1)
                    {
                        Ok(idx) => idx,
                        Err(idx) => idx,
                    };
                    if left_idx < right_idx {
                        split_plan.push((idx, (left_idx, left), (right_idx, right)));
                    }
                    if right == last {
                        break;
                    }
                    idx += 1;
                }
                let mut split_point: Vec<(usize, u32)> = vec![];
                for (_, left, right) in split_plan.iter() {
                    if split_point.last().map_or(true, |v| v != left) {
                        split_point.push(*left);
                        if left != right {
                            split_point.push(*right);
                        }
                    }
                }

                match block {
                    RecordBlock::Block(data) => {
                        let split: Vec<_> = split_point
                            .iter()
                            .zip(split_point.iter().skip(1))
                            .map(|(left, right)| {
                                let mut ret = vec![];
                                let mut l_idx = left.0;
                                let r_idx = right.0;
                                if data[l_idx].effective_range().0 != left.1 {
                                    data[l_idx]
                                        .limit_left(left.1)
                                        .map(|x| ret.push(RecordBlock::Record(x)));
                                    l_idx += 1;
                                }
                                ret.push(RecordBlock::Block(unsafe {
                                    std::slice::from_raw_parts(
                                        data[l_idx..].as_ptr() as *const R,
                                        r_idx - l_idx,
                                    )
                                }));
                                if r_idx < data.len() && data[r_idx].effective_range().1 != right.1
                                {
                                    data[r_idx]
                                        .limit_right(right.1)
                                        .map(|x| ret.push(RecordBlock::Record(x)));
                                }
                                (left, right, ret)
                            })
                            .collect();
                        let mut i = 0;
                        for (l, r, part) in split {
                            while split_plan[i].1 == split_plan[i].2 {
                                i += 1;
                            }
                            if split_plan[i].1 == *l && split_plan[i].2 == *r {
                                for block in part {
                                    part_blocks[split_plan[i].0].push(block);
                                }
                            }
                            i += 1;
                        }
                    }
                    RecordBlock::Record(r) => {
                        for (part_idx, left, right) in split_plan {
                            if let Some(Some(result)) =
                                r.limit_left(left.1).map(|x| x.limit_right(right.1))
                            {
                                part_blocks[part_idx].push(RecordBlock::Record(result));
                            }
                        }
                    }
                }
            }
        }

        let mut buffer: Vec<_> = displacement
            .into_iter()
            .zip(
                part_blocks
                    .into_iter()
                    .map(|records| SimpleKeyValuePartialReader {
                        _root: self.s_table_root.clone(),
                        records,
                        cursor: (0, 0),
                        next: None,
                    }),
            )
            .collect();
        buffer.sort_by_key(|item| item.0);
        Ok(buffer.into_iter().map(|item| item.1).collect())
    }
}

pub trait Record: Sized + Copy + Send + 'static {
    const FORMAT_NAME: &'static str;
    const SIZE: usize = std::mem::size_of::<Self>();
    fn effective_range(&self) -> (u32, u32);
    fn limit_left(&self, new_left: u32) -> Option<Self>;
    fn limit_right(&self, new_right: u32) -> Option<Self>;
    #[inline(always)]
    fn in_range(&self, pos: u32) -> bool {
        let (a, b) = self.effective_range();
        a <= pos && pos < b
    }
    fn value(&self) -> i32;
    fn encode(this: Option<&mut Self>, pos: u32, value: i32) -> Option<Self>;
    #[inline(always)]
    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(std::mem::transmute(self), Self::SIZE) }
    }
    fn is_valid(&self) -> bool;
}

#[repr(packed)]
#[derive(Clone, Copy)]
pub struct RangeRecord {
    left: u32,
    size_enc: u16,
    value: i32,
}

impl Record for RangeRecord {
    const FORMAT_NAME: &'static str = "range";

    #[inline(always)]
    fn effective_range(&self) -> (u32, u32) {
        (
            self.left.to_le() - 1,
            self.left.to_le() + self.size_enc.to_le() as u32,
        )
    }
    #[inline(always)]
    fn limit_left(&self, new_left: u32) -> Option<Self> {
        if new_left >= self.effective_range().1 {
            None
        } else {
            Some(Self {
                left: (new_left + 1).max(self.left),
                size_enc: (self.effective_range().1 - new_left - 1) as u16,
                value: self.value,
            })
        }
    }
    #[inline(always)]
    fn limit_right(&self, new_right: u32) -> Option<Self> {
        if new_right <= self.effective_range().0 {
            None
        } else {
            Some(Self {
                left: self.left,
                size_enc: (new_right - self.effective_range().0 - 1) as u16,
                value: self.value,
            })
        }
    }
    #[inline(always)]
    fn value(&self) -> i32 {
        self.value.to_le()
    }

    #[inline(always)]
    fn encode(this: Option<&mut Self>, pos: u32, value: i32) -> Option<Self> {
        if let Some(this) = this {
            if this.value == value && this.left + this.size_enc as u32 == pos && this.size_enc != !0
            {
                this.size_enc += 1;
                return None;
            }
        }
        Some(Self {
            left: pos + 1,
            size_enc: 0,
            value,
        })
    }

    fn is_valid(&self) -> bool {
        self.left > 0
    }
}

#[repr(packed)]
#[derive(Clone, Copy)]
pub struct SingleRecord {
    pos: u32,
    val: i32,
}

impl Record for SingleRecord {
    const FORMAT_NAME: &'static str = "single";

    #[inline(always)]
    fn effective_range(&self) -> (u32, u32) {
        (self.pos.to_le() - 1, self.pos.to_le())
    }
    #[inline(always)]
    fn limit_left(&self, new_left: u32) -> Option<Self> {
        let new_left = new_left + 1;
        if new_left != self.pos {
            None
        } else {
            Some(*self)
        }
    }
    #[inline(always)]
    fn limit_right(&self, new_right: u32) -> Option<Self> {
        let new_right = new_right + 1;
        if new_right != self.pos + 1 {
            None
        } else {
            Some(*self)
        }
    }
    #[inline(always)]
    fn value(&self) -> i32 {
        self.val.to_le()
    }

    #[inline(always)]
    fn encode(_this: Option<&mut Self>, pos: u32, value: i32) -> Option<Self> {
        Some(Self {
            pos: pos + 1,
            val: value,
        })
    }

    fn is_valid(&self) -> bool {
        self.pos > 0
    }
}
enum RecordBlock<'a, R: Record> {
    Block(&'a [R]),
    Record(R),
}
impl<'a, R: Record> AsRef<[R]> for RecordBlock<'a, R> {
    fn as_ref(&self) -> &[R] {
        match self {
            Self::Block(blk) => blk,
            Self::Record(rec) => std::slice::from_ref(rec),
        }
    }
}
impl<'a, R: Record> RecordBlock<'a, R> {
    fn count(&self) -> usize {
        match self {
            Self::Block(what) => what.len(),
            Self::Record(_) => 1,
        }
    }
    fn get(&self, idx: usize) -> R {
        match self {
            Self::Block(what) => what[idx],
            Self::Record(what) => *what,
        }
    }
}

pub struct SimpleKeyValuePartialReader<R: Record> {
    _root: Arc<MappedDirectory>,
    records: Vec<RecordBlock<'static, R>>,
    cursor: (usize, usize),
    next: Option<R>,
}

impl<R: Record> SimpleKeyValuePartialReader<R> {
    fn load_cache(&mut self, inc: bool) {
        if inc {
            self.cursor.1 += 1;
            if self.cursor.1 >= self.records[self.cursor.0].count() {
                self.cursor.0 += 1;
                self.cursor.1 = 0;
            }
        }
        if self.cursor.0 < self.records.len() {
            self.next = Some(self.records[self.cursor.0].get(self.cursor.1));
        }
    }
}

impl<R: Record> STablePartitionReader for SimpleKeyValuePartialReader<R> {
    type IteratorState = (usize, usize);
    fn next_record(&self, state: &mut Self::IteratorState) -> Option<(u32, u32, i32)> {
        if let Some(record) = self
            .records
            .get(state.0)
            .map_or(None, |block| block.as_ref().get(state.1))
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
        let idx = match self
            .records
            .binary_search_by_key(&pos, |block| block.get(0).effective_range().0)
        {
            Ok(idx) => idx,
            Err(idx) if idx > 0 && idx < self.records.len() => idx - 1,
            _ => return None,
        };

        let len = self.records[idx].count();

        match self.records[idx]
            .as_ref()
            .binary_search_by_key(&pos, |rec| rec.effective_range().1)
        {
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
