use super::D4TrackReader;
use crate::{
    ptab::{DecodeResult, Decoder, PTablePartitionReader, PTableReader},
    stab::{STablePartitionReader, STableReader},
};

use std::{io::Result, iter::Once};

/// Tagged data
pub struct TrackValue {
    pub tag_id: usize,
    pub value: i32,
}

/// Code that used to scan a multi-track D4 file
pub trait DataScanner<RowType: Iterator<Item = TrackValue> + ExactSizeIterator> {
    /// Get the range this data scanner want to scan. Please note all the data scanner doesn't across the chromosome boundary
    /// so we don't specify the chromosome, as it's implied by "current chromosome", which is defined by the MultiTrackPartitionReader
    fn get_range(&self) -> (u32, u32);
    fn feed_row(&mut self, pos: u32, row: RowType) -> bool;
    fn feed_rows(&mut self, begin: u32, end: u32, row: RowType) -> bool;
}

/// A reader that scans one partition within a chromosome
pub trait MultiTrackPartitionReader {
    /// The type for each row
    type RowType: Iterator<Item = TrackValue> + ExactSizeIterator;
    /// Scan the partition with a group of scanners
    fn scan_partition<S: DataScanner<Self::RowType>>(&mut self, handles: &mut [S]);
    fn chrom(&self) -> &str;
    fn begin(&self) ->  u32;
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
    type RowType = Once<TrackValue>;

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
                            handles[id].feed_row(pos as u32, std::iter::once(TrackValue {value, tag_id: 0}));
                        }
                    },
                );
            } else {
                let iter = self.secondary.seek_iter(part_left);
                for (mut left, mut right, value) in iter {
                    left = left.max(part_left);
                    right = right.min(part_right).max(left);
                    for &id in active_handles.iter() {
                        handles[id].feed_rows(left, right, std::iter::once(TrackValue { value, tag_id: 0 }));
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

    fn begin(&self) ->  u32 {
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
