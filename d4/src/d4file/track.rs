use smallvec::SmallVec;

use super::D4TrackReader;
use crate::{
    ptab::{
        BitArrayPartReader, BitArrayReader, DecodeBlockHandle, DecodeResult, Decoder,
        MatrixDecoder, PrimaryTablePartReader, PrimaryTableReader,
    },
    stab::{SecondaryTablePartReader, SecondaryTableReader},
    task::{IntoTaskVec, Task, TaskContext, TaskOutputVec},
};

use std::{
    cmp::Ordering,
    collections::BinaryHeap,
    io::{Error, ErrorKind, Result},
    iter::Once,
    ops::{Deref, DerefMut},
};

fn adjust_down<T, Cmp: Fn(&T, &T) -> Ordering>(heap: &mut [T], mut idx: usize, cmp: Cmp) {
    while idx < heap.len() {
        let mut min_idx = idx;
        if idx * 2 + 1 < heap.len() && cmp(&heap[min_idx], &heap[idx * 2 + 1]).is_gt() {
            min_idx = idx * 2 + 1;
        }
        if idx * 2 + 2 < heap.len() && cmp(&heap[min_idx], &heap[idx * 2 + 2]).is_gt() {
            min_idx = idx * 2 + 2;
        }
        if min_idx == idx {
            break;
        }
        heap.swap(min_idx, idx);
        idx = min_idx;
    }
}

fn adjust_up<T, Cmp: Fn(&T, &T) -> Ordering>(heap: &mut [T], mut idx: usize, cmp: Cmp) {
    while idx > 0 && cmp(&heap[(idx - 1) / 2], &heap[idx]).is_gt() {
        heap.swap((idx - 1) / 2, idx);
        idx = (idx - 1) / 2;
    }
}

fn scan_partition_impl<RT, DS, F>(handles: &mut [DS], mut func: F)
where
    RT: Iterator<Item = i32> + ExactSizeIterator,
    DS: DataScanner<RT>,
    F: FnMut(u32, u32, &mut [&mut DS]),
{
    if handles.is_empty() {
        return;
    }

    handles.sort_unstable_by_key(|a| a.get_range());

    let mut active_heap: Vec<&mut DS> = vec![];
    let cmp = |a: &&mut DS, b: &&mut DS| a.get_range().1.cmp(&b.get_range().1);
    let mut last_end = handles[0].get_range().0;
    let mut handle_iter = handles.iter_mut();

    loop {
        let handle = handle_iter.next();
        let pos = handle.as_ref().map_or(u32::MAX, |h| h.get_range().0);
        // First, we need to pop all the previously active handles that will be deactivaited
        while let Some(top) = active_heap.first() {
            let this_end = top.get_range().1;
            if pos < this_end {
                break;
            }

            if this_end != last_end {
                let this_begin = top.get_range().0.max(last_end);
                func(this_begin, this_end, &mut active_heap);
                last_end = this_end;
            }

            let heap_size = active_heap.len();
            active_heap.swap(0, heap_size - 1);
            active_heap.pop();
            adjust_down(&mut active_heap, 0, cmp);
        }
        if let Some(handle) = handle {
            handle.init();
            if let Some(top) = active_heap.first() {
                let this_begin = top.get_range().0.max(last_end);
                func(this_begin, pos, &mut active_heap);
                last_end = pos;
            }
            let idx = active_heap.len();
            active_heap.push(handle);
            adjust_up(&mut active_heap, idx, cmp);
        } else {
            break;
        }
    }
}

/// Code that used to scan a multi-track D4 file
pub trait DataScanner<RowType: Iterator<Item = i32> + ExactSizeIterator> {
    #[inline(always)]
    fn init(&mut self) {}
    /// Get the range this data scanner want to scan. Please note all the data scanner doesn't across the chromosome boundary
    /// so we don't specify the chromosome, as it's implied by "current chromosome", which is defined by the MultiTrackPartitionReader
    fn get_range(&self) -> (u32, u32);
    fn feed_row(&mut self, pos: u32, row: &mut RowType) -> bool;
    fn feed_rows(&mut self, begin: u32, end: u32, row: &mut RowType) -> bool;
}

/// A reader that scans one partition within a chromosome
pub trait MultiTrackPartitionReader {
    /// The type for each row
    type RowType: Iterator<Item = i32> + ExactSizeIterator;
    /// Scan the partition with a group of scanners
    fn scan_partition<S: DataScanner<Self::RowType>>(&mut self, handles: &mut [S]);
    fn chrom(&self) -> &str;
    fn begin(&self) -> u32;
    fn end(&self) -> u32;
}

/// Trait for any type that has ability to read multi-track data
pub trait MultiTrackReader {
    /// The type for partition reader
    type PartitionType: MultiTrackPartitionReader;
    /// Split a multi track reader into different partitions
    fn split(&mut self, size_limit: Option<usize>) -> Result<Vec<Self::PartitionType>>;
    /// Create a task on this reader
    fn run_tasks<RS, T>(&mut self, tasks: RS) -> Result<TaskOutputVec<T::Output>>
    where
        Self: Sized,
        T: Task<<Self::PartitionType as MultiTrackPartitionReader>::RowType>,
        RS: IntoTaskVec<<Self::PartitionType as MultiTrackPartitionReader>::RowType, T>,
        Self::PartitionType: Send,
    {
        Ok(TaskContext::new(self, tasks.into_task_vec())?.run())
    }
}

pub struct D4FilePartition<P: PrimaryTableReader, S: SecondaryTableReader> {
    primary: P::Partition,
    secondary: S::Partition,
}

struct ScanPartitionBlockHandler<'a, 'b, S: SecondaryTableReader, DS> {
    secondary: &'a mut S::Partition,
    active_handles: &'a mut [&'b mut DS],
}

impl<'a, 'b, S: SecondaryTableReader, DS> DecodeBlockHandle
    for ScanPartitionBlockHandler<'a, 'b, S, DS>
where
    DS: DataScanner<Once<i32>>,
{
    #[inline(always)]
    fn handle(&mut self, pos: usize, res: DecodeResult) {
        let value = match res {
            DecodeResult::Definitely(value) => value,
            DecodeResult::Maybe(back) => {
                if let Some(value) = self.secondary.decode(pos as u32) {
                    value
                } else {
                    back
                }
            }
        };
        for handle in self.active_handles.iter_mut() {
            handle.feed_row(pos as u32, &mut std::iter::once(value));
        }
    }
}

impl<P: PrimaryTableReader, S: SecondaryTableReader> MultiTrackPartitionReader
    for D4FilePartition<P, S>
{
    type RowType = Once<i32>;

    fn scan_partition<DS: DataScanner<Self::RowType>>(&mut self, handles: &mut [DS]) {
        let default_primary_value = self.primary.default_value();
        let mut decoder = self.primary.make_decoder();

        scan_partition_impl(handles, |part_left, part_right, active_handles| {
            if let Some(default_value) = default_primary_value {
                let iter = self.secondary.seek_iter(part_left);
                let mut last_right = part_left;
                for (mut left, mut right, value) in iter {
                    left = left.max(part_left);
                    right = right.min(part_right).max(left);
                    for handle in active_handles.iter_mut() {
                        if last_right < left {
                            handle.feed_rows(last_right, left, &mut std::iter::once(default_value));
                        }
                        handle.feed_rows(left, right, &mut std::iter::once(value));
                    }
                    last_right = right;
                    if right == part_right {
                        break;
                    }
                }
                if last_right < part_right {
                    for handle in active_handles.iter_mut() {
                        handle.feed_rows(
                            last_right,
                            part_right,
                            &mut std::iter::once(default_value),
                        );
                    }
                }
            } else {
                let block_handler = ScanPartitionBlockHandler::<S, DS> {
                    secondary: &mut self.secondary,
                    active_handles,
                };
                decoder.decode_block(
                    part_left as usize,
                    (part_right - part_left) as usize,
                    block_handler,
                );
            }
        });
    }

    fn chrom(&self) -> &str {
        self.primary.region().0
    }

    fn begin(&self) -> u32 {
        self.primary.region().1
    }

    fn end(&self) -> u32 {
        self.primary.region().2
    }
}

/// Of course single track reader can be unified here
impl<P: PrimaryTableReader, S: SecondaryTableReader> MultiTrackReader for D4TrackReader<P, S> {
    type PartitionType = D4FilePartition<P, S>;
    fn split(&mut self, size_limit: Option<usize>) -> Result<Vec<Self::PartitionType>> {
        Ok(self
            .split(size_limit)?
            .into_iter()
            .map(|(primary, secondary)| D4FilePartition { primary, secondary })
            .collect())
    }
}

pub struct D4MatrixReader<S: SecondaryTableReader> {
    tracks: Vec<D4TrackReader<BitArrayReader, S>>,
}

pub struct D4MatrixReaderPartition<S: SecondaryTableReader> {
    primary: Vec<BitArrayPartReader>,
    secondary: Vec<S::Partition>,
}
#[derive(Default)]
pub struct MatrixRow {
    data_buf: Vec<i32>,
    read_idx: usize,
}

impl Deref for MatrixRow {
    type Target = Vec<i32>;
    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.data_buf
    }
}

impl DerefMut for MatrixRow {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data_buf
    }
}

impl Iterator for MatrixRow {
    type Item = i32;
    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.read_idx;
        self.read_idx += 1;
        self.data_buf.get(idx).copied()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let sz = self.data_buf.len() - self.read_idx;
        (sz, Some(sz))
    }
}

impl ExactSizeIterator for MatrixRow {}
impl<S: SecondaryTableReader> D4MatrixReaderPartition<S> {
    fn decode_secondary<H: FnMut(u32, &mut MatrixRow)>(
        &mut self,
        pos: u32,
        ncols: usize,
        decode_buf: &mut MatrixRow,
        data: &[DecodeResult],
        mut handle: H,
    ) {
        for i in 0..ncols {
            decode_buf[i] = match data[i] {
                DecodeResult::Definitely(value) => value,
                DecodeResult::Maybe(value_from_1st) => {
                    if let Some(value_from_2nd) = self.secondary[i].decode(pos) {
                        value_from_2nd
                    } else {
                        value_from_1st
                    }
                }
            };
        }
        handle(pos, decode_buf);
    }
    fn decode_block_impl<H: FnMut(u32, &mut MatrixRow)>(
        &mut self,
        mut handle: H,
        ncols: usize,
        part_left: u32,
        part_right: u32,
        decoder: &mut MatrixDecoder,
    ) {
        let mut decode_buf = MatrixRow::default();
        decode_buf.resize(ncols, 0);
        if part_right - part_left > 1000 {
            decoder.decode_block(part_left, part_right, |pos, data| {
                self.decode_secondary(pos, ncols, &mut decode_buf, data, &mut handle);
                true
            });
        } else {
            let mut data = Vec::with_capacity(ncols);
            for pos in part_left..part_right {
                decoder.decode(pos, &mut data);
                self.decode_secondary(pos, ncols, &mut decode_buf, &data, &mut handle);
            }
        }
    }
    fn scan_per_base<H>(
        &mut self,
        part_left: u32,
        part_right: u32,
        active_handles: &mut [&mut H],
        decoder: &mut MatrixDecoder,
    ) where
        H: DataScanner<<Self as MultiTrackPartitionReader>::RowType>,
    {
        self.decode_block_impl(
            |pos, buf| {
                for handle in active_handles.iter_mut() {
                    buf.read_idx = 0;
                    handle.feed_row(pos, buf);
                }
            },
            self.secondary.len(),
            part_left,
            part_right,
            decoder,
        );
    }
    fn scan_per_interval<H>(
        &mut self,
        part_left: u32,
        part_right: u32,
        active_handles: &mut [&mut H],
    ) where
        H: DataScanner<<Self as MultiTrackPartitionReader>::RowType>,
    {
        let default_values: Vec<_> = self
            .primary
            .iter()
            .map(|p| {
                let dict = p.dict();
                dict.first_value()
            })
            .collect();
        let mut iters = SmallVec::<[_; 8]>::with_capacity(self.secondary.len());

        for sec_tab in self.secondary.iter_mut() {
            iters.push(sec_tab.seek_iter(part_left));
        }

        // Event(idx, start, value)
        let mut event_heap = BinaryHeap::new();

        for (track_id, reader) in iters.iter_mut().enumerate() {
            if let Some((begin, end, value)) = reader.next() {
                if begin < part_right {
                    event_heap.push((begin, value, track_id));
                    event_heap.push((end, default_values[track_id], track_id))
                }
            }
        }

        let mut data_buf = MatrixRow::default();
        data_buf.extend(&default_values);
        let mut cur_pos = part_left;
        let mut last_event_pos = part_left;

        while let Some(event) = event_heap.pop() {
            if event.1 == default_values[event.2] {
                if let Some(next) = iters[event.2].next() {
                    if next.0 < part_right {
                        event_heap.push((next.0, next.2, event.2));
                        event_heap.push((next.1, default_values[event.2], event.2));
                    }
                }
            }
            if last_event_pos != event.0 {
                if cur_pos < last_event_pos {
                    for handle in active_handles.iter_mut() {
                        data_buf.read_idx = 0;
                        handle.feed_rows(cur_pos, last_event_pos, &mut data_buf);
                    }
                }
                cur_pos = last_event_pos;
            }

            data_buf[event.2] = event.1;
            last_event_pos = event.0;
        }

        if cur_pos < last_event_pos {
            for handle in active_handles.iter_mut() {
                data_buf.read_idx = 0;
                handle.feed_rows(cur_pos, last_event_pos, &mut data_buf);
            }
        }
    }
}

impl<S: SecondaryTableReader> MultiTrackPartitionReader for D4MatrixReaderPartition<S> {
    type RowType = MatrixRow;

    fn scan_partition<DS: DataScanner<Self::RowType>>(&mut self, handles: &mut [DS]) {
        let mut decoder = MatrixDecoder::new(self.primary.as_mut_slice());
        let per_base = !decoder.is_zero_sized();

        scan_partition_impl(handles, |begin, end, active_handles| {
            if per_base {
                self.scan_per_base(begin, end, active_handles, &mut decoder);
            } else {
                self.scan_per_interval(begin, end, active_handles)
            }
        });
    }

    fn chrom(&self) -> &str {
        self.primary[0].region().0
    }

    fn begin(&self) -> u32 {
        self.primary[0].region().1
    }

    fn end(&self) -> u32 {
        self.primary[0].region().2
    }
}

impl<S: SecondaryTableReader> D4MatrixReader<S> {
    pub fn chrom_regions(&self) -> Vec<(&str, u32, u32)> {
        self.tracks[0].chrom_regions()
    }
    pub fn new<T: IntoIterator<Item = D4TrackReader<BitArrayReader, S>>>(
        tracks: T,
    ) -> Result<Self> {
        let tracks: Vec<_> = tracks.into_iter().collect();

        if tracks.is_empty() {
            return Err(Error::new(
                ErrorKind::Other,
                "MatrixReader only supports non-empty input",
            ));
        }

        let first_track_chrom = tracks[0].header().chrom_list();

        for track in tracks.iter().skip(1) {
            let chrom = track.header().chrom_list();
            if chrom != first_track_chrom {
                return Err(Error::new(
                    ErrorKind::Other,
                    "Inconsistent reference genome in matrix",
                ));
            }
        }
        Ok(Self { tracks })
    }
}

impl<S: SecondaryTableReader> MultiTrackReader for D4MatrixReader<S> {
    type PartitionType = D4MatrixReaderPartition<S>;

    fn split(&mut self, size_limit: Option<usize>) -> Result<Vec<Self::PartitionType>> {
        let mut primary_table_decoders: Vec<Vec<_>> = vec![];
        let mut secondary_tables: Vec<Vec<_>> = vec![];

        let partition = self
            .tracks
            .iter_mut()
            .map(|track| D4TrackReader::split(track, size_limit).unwrap());

        for track_parts in partition {
            primary_table_decoders.resize_with(track_parts.len(), Default::default);
            secondary_tables.resize_with(track_parts.len(), Default::default);
            for (idx, (pt, st)) in track_parts.into_iter().enumerate() {
                primary_table_decoders[idx].push(pt);
                secondary_tables[idx].push(st);
            }
        }

        Ok(primary_table_decoders
            .into_iter()
            .zip(secondary_tables)
            .map(|(pts, sts)| {
                let primary = pts;
                let secondary = sts;
                D4MatrixReaderPartition::<S> { primary, secondary }
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestScanner {
        range: (u32, u32),
    }

    impl<RT> DataScanner<RT> for TestScanner
    where
        RT: Iterator<Item = i32> + ExactSizeIterator,
     {
        fn get_range(&self) -> (u32, u32) {
            self.range
        }

        fn init(&mut self) {}

        fn feed_row(&mut self, _pos: u32, _row: &mut RT) -> bool {
            true
        }

        fn feed_rows(&mut self, _begin: u32, _end: u32, _row: &mut RT) -> bool {
            true            
        }
    }

    #[test]
    fn fail_adjust_up() {

        // This set of ranges will crash if parent in adjust_up is selected as "x" instead of "x-1"
        // It is used as is simply as it was hard to boil this down further to a smaller test case
        let ranges = vec![
            (686509, 763194),
            (686606, 764595),
            (686639, 764595),
            (686659, 762714),
            (686674, 764595),
            (686675, 764595),
            (686682, 764595),
            (686689, 763120),
            (686731, 762597),
            (706399, 764595),
            (718904, 737202),
            (747700, 748140),
        ];

        let mut handles: Vec<TestScanner> = ranges
            .into_iter()
            .map(|range| TestScanner {
                range
            })
            .collect();

        let mut results = Vec::new();
        
        let func = |begin: u32, end: u32, active: &mut [&mut TestScanner]| {
            results.push((begin, end, active.len()));
        };

        scan_partition_impl::<std::vec::IntoIter<i32>, TestScanner, _>(&mut handles, func);

        assert!(!results.is_empty());

        // The bug led to one interval being in the reversed order
        for (begin, end, _active_count) in results {
            assert!(end > begin);
        }
    }
}
