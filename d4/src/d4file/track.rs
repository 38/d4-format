use super::D4TrackReader;
use crate::{
    ptab::{
        BitArrayPartReader, BitArrayReader, DecodeResult, Decoder, MatrixDecoder,
        PTablePartitionReader, PTableReader,
    },
    stab::{STablePartitionReader, STableReader},
};

use std::{
    collections::BinaryHeap,
    io::{Error, ErrorKind, Result},
    iter::Once,
    ops::{Deref, DerefMut},
};

/// Code that used to scan a multi-track D4 file
pub trait DataScanner<RowType: Iterator<Item = i32> + ExactSizeIterator> {
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
}

pub struct D4FilePartition<P: PTableReader, S: STableReader> {
    primary: P::Partition,
    secondary: S::Partition,
}

impl<P: PTableReader, S: STableReader> MultiTrackPartitionReader for D4FilePartition<P, S> {
    type RowType = Once<i32>;

    fn scan_partition<DS: DataScanner<Self::RowType>>(&mut self, handles: &mut [DS]) {
        let per_base = self.primary.bit_width() > 0;
        let mut decoder = self.primary.make_decoder();

        // First, we need to determine all the break points defined by each scanner
        let mut break_points: Vec<_> = handles
            .iter()
            .map(|x| {
                let (start, end) = x.get_range();
                std::iter::once(start).chain(std::iter::once(end))
            })
            .flatten()
            .collect();
        break_points.sort_unstable();

        if break_points.is_empty() {
            return;
        }

        for idx in 0..break_points.len() - 1 {
            if break_points[idx] == break_points[idx + 1] {
                continue;
            }
            let part_left = break_points[idx];
            let part_right = break_points[idx + 1];

            // Find all handles that is active in range [part_left, part_right)
            let active_handles: Vec<_> = (0..handles.len())
                .filter(|&x| {
                    let (l, r) = handles[x].get_range();
                    l <= part_left && part_right <= r
                })
                .collect();

            if active_handles.is_empty() {
                continue;
            }

            if per_base {
                decoder.decode_block(
                    part_left as usize,
                    (part_right - part_left) as usize,
                    |pos, res| {
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
                        for &id in active_handles.iter() {
                            handles[id].feed_row(pos as u32, &mut std::iter::once(value));
                        }
                    },
                );
            } else {
                let iter = self.secondary.seek_iter(part_left);
                for (mut left, mut right, value) in iter {
                    left = left.max(part_left);
                    right = right.min(part_right).max(left);
                    for &id in active_handles.iter() {
                        handles[id].feed_rows(left, right, &mut std::iter::once(value));
                    }
                    if right == part_right {
                        break;
                    }
                }
            }
        }
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
impl<P: PTableReader, S: STableReader> MultiTrackReader for D4TrackReader<P, S> {
    type PartitionType = D4FilePartition<P, S>;
    fn split(&mut self, size_limit: Option<usize>) -> Result<Vec<Self::PartitionType>> {
        Ok(self
            .split(size_limit)?
            .into_iter()
            .map(|(primary, secondary)| D4FilePartition { primary, secondary })
            .collect())
    }
}

pub struct D4MatrixReader<S: STableReader> {
    tracks: Vec<D4TrackReader<BitArrayReader, S>>,
}

pub struct D4MatrixReaderPartition<S: STableReader> {
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
impl <S:STableReader> D4MatrixReaderPartition<S> {
    fn decode_block_impl<H: FnMut(u32, &mut MatrixRow)>(&mut self, mut handle: H, ncols: usize, part_left: u32, part_right: u32, decoder: &MatrixDecoder) {
        let mut decode_buf = MatrixRow::default();
        decode_buf.resize(ncols, 0);
        decoder.decode_block(part_left, part_right, |pos, data| {
            for i in  0..ncols {
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
            handle(pos, &mut decode_buf);
            true
        });
    }
}
impl<S: STableReader> MultiTrackPartitionReader for D4MatrixReaderPartition<S> {
    type RowType = MatrixRow;

    fn scan_partition<DS: DataScanner<Self::RowType>>(&mut self, handles: &mut [DS]) {
        let decoder = MatrixDecoder::new(self.primary.as_mut_slice());
        let per_base = decoder.is_zero_sized();

        // First, we need to determine all the break points defined by each scanner
        let mut break_points: Vec<_> = handles
            .iter()
            .map(|x| {
                let (start, end) = x.get_range();
                std::iter::once(start).chain(std::iter::once(end))
            })
            .flatten()
            .collect();
        break_points.sort_unstable();

        if break_points.is_empty() {
            return;
        }

        for idx in 0..break_points.len() - 1 {
            if break_points[idx] == break_points[idx + 1] {
                continue;
            }
            let part_left = break_points[idx];
            let part_right = break_points[idx + 1];

            // Find all handles that is active in range [part_left, part_right)
            let active_handles: Vec<_> = (0..handles.len())
                .filter(|&x| {
                    let (l, r) = handles[x].get_range();
                    l <= part_left && part_right <= r
                })
                .collect();

            if active_handles.is_empty() {
                continue;
            }


            if !per_base {
                match active_handles.len() {
                    1 => {
                        let h0 = &mut handles[active_handles[0]];
                        self.decode_block_impl(|pos, buf| {
                            buf.read_idx = 0;
                            h0.feed_row(pos, buf);
                        }, self.secondary.len(), part_left, part_right, &decoder);
                    },
                    2 => {
                        let (head, tail) = handles.split_at_mut(1);
                        let h0 = &mut head[0];
                        let h1 = &mut tail[0];
                        self.decode_block_impl(|pos, buf| {
                            buf.read_idx = 0;
                            h0.feed_row(pos, buf);
                            buf.read_idx = 0;
                            h1.feed_row(pos, buf);
                        }, self.secondary.len(), part_left, part_right, &decoder);
                    },
                    _=> {
                        self.decode_block_impl(|pos, buf| {
                            for &handle_id in active_handles.iter() {
                                buf.read_idx = 0;
                                handles[handle_id].feed_row(pos, buf);
                            }
                        }, self.secondary.len(), part_left, part_right, &decoder);
                    }
                }
            } else {
                let default_values: Vec<_> = self
                    .primary
                    .iter()
                    .map(|p| {
                        let dict = p.dict();
                        dict.first_value()
                    })
                    .collect();
                let mut iters: Vec<_> = self
                    .secondary
                    .iter()
                    .map(|s| s.seek_iter(part_left))
                    .collect();
                // Event(idx, start, value)
                let mut event_heap: BinaryHeap<(u32, i32, usize)> = iters
                    .iter_mut()
                    .enumerate()
                    .map(|(id, r)| {
                        if let Some(data) = r.next() {
                            vec![(data.0, data.2, id), (data.1, default_values[id], id)]
                        } else {
                            vec![]
                        }
                        .into_iter()
                    })
                    .flatten()
                    .collect();

                let mut data_buf = MatrixRow::default();
                data_buf.extend(&default_values);
                let mut cur_pos = part_left;
                let mut last_event_pos = part_left;

                while let Some(event) = event_heap.pop() {
                    if event.1 == default_values[event.2] {
                        if let Some(next) = iters[event.2].next() {
                            event_heap.push((next.0, next.2, event.2));
                            event_heap.push((next.1, default_values[event.2], event.2));
                        }
                    }
                    if last_event_pos != event.0 {
                        if cur_pos < last_event_pos {
                            for &hid in active_handles.iter() {
                                data_buf.read_idx = 0;
                                handles[hid].feed_rows(cur_pos, last_event_pos, &mut data_buf);
                            }
                        }
                        cur_pos = last_event_pos;
                    }

                    data_buf[event.2] = event.1;
                    last_event_pos = event.0;
                }

                if cur_pos < last_event_pos {
                    for &hid in active_handles.iter() {
                        data_buf.read_idx = 0;
                        handles[hid].feed_rows(cur_pos, last_event_pos, &mut data_buf);
                    }
                }
            }
        }
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

impl<S: STableReader> D4MatrixReader<S> {
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

impl<S: STableReader> MultiTrackReader for D4MatrixReader<S> {
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
