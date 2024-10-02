#[cfg(not(feature = "seq-task"))]
use rayon::prelude::*;
use std::{collections::HashMap, io::Result};

use super::{Task, TaskOutputVec, TaskPartition};
use crate::d4file::{DataScanner, MultiTrackPartitionReader, MultiTrackReader};

struct TaskScanner<P> {
    task_id: usize,
    range: (u32, u32),
    partition: P,
}

impl<P: TaskPartition<RT>, RT: Iterator<Item = i32> + ExactSizeIterator> DataScanner<RT>
    for TaskScanner<P>
{
    fn init(&mut self) {
        self.partition.init();
    }
    fn get_range(&self) -> (u32, u32) {
        self.range
    }

    fn feed_row(&mut self, pos: u32, row: &mut RT) -> bool {
        self.partition.feed(pos, row)
    }

    fn feed_rows(&mut self, begin: u32, end: u32, row: &mut RT) -> bool {
        self.partition.feed_range(begin, end, row)
    }
}

struct PartitionContext<R: MultiTrackPartitionReader, T: Task<R::RowType>> {
    reader: R,
    tasks: Vec<TaskScanner<T::Partition>>,
}

impl<R: MultiTrackPartitionReader, T: Task<R::RowType>> PartitionContext<R, T> {
    fn execute(&mut self) -> Vec<TaskScanner<T::Partition>> {
        self.reader.scan_partition(self.tasks.as_mut_slice());
        std::mem::take(&mut self.tasks)
    }
}

/// The context for a parallel task
pub struct TaskContext<R: MultiTrackReader, T>
where
    T: Task<<R::PartitionType as MultiTrackPartitionReader>::RowType>,
{
    regions: Vec<T>,
    partitions: Vec<PartitionContext<R::PartitionType, T>>,
}

impl<R: MultiTrackReader, T: Task<<R::PartitionType as MultiTrackPartitionReader>::RowType>>
    TaskContext<R, T>
where
    T::Partition: Send,
    R::PartitionType: Send,
{
    /// Create a new task that processing the given file
    pub fn new(reader: &mut R, mut tasks: Vec<T>) -> Result<Self> {
        let mut file_partition = MultiTrackReader::split(reader, Some(10_000_000))?;

        file_partition.sort_unstable_by(|a, b| {
            (a.chrom(), a.begin(), a.end()).cmp(&(b.chrom(), b.begin(), b.end()))
        });

        tasks.sort_unstable_by(|a, b| a.region().cmp(&b.region()));

        let mut task_assignment: Vec<Vec<_>> = (0..file_partition.len())
            .map(|_| Default::default())
            .collect();

        // Now assign the d4 file partition to each task assignments
        let mut idx = 0;
        for (fpid, part) in file_partition.iter().enumerate() {
            let chr = part.chrom();
            let fpl = part.begin();
            let fpr = part.end();

            // first, skip all the regions that *before* this partition
            while idx < tasks.len() {
                let (task_chr, _, task_right) = tasks[idx].region();
                if task_chr < chr || (task_chr == chr && task_right < fpl) {
                    idx += 1;
                } else {
                    break;
                }
            }

            let mut overlapping_idx = idx;

            while overlapping_idx < tasks.len() {
                let this = &tasks[overlapping_idx];
                let (c, l, r) = this.region();
                if c != chr || fpr < l {
                    break;
                }
                // As ranges are sorted on their left position, the idx approach
                // above can let through ranges with lower left positions which still
                // do not reach into the next segment
                // See https://github.com/38/d4-format/pull/91
                if fpl > r {
                    overlapping_idx += 1;
                    continue;
                }
                let actual_left = fpl.max(l);
                let actual_right = fpr.min(r);

                task_assignment[fpid].push(TaskScanner {
                    task_id: overlapping_idx,
                    range: (actual_left, actual_right),
                    partition: <<T as Task<_>>::Partition as TaskPartition<_>>::new(
                        actual_left,
                        actual_right,
                        this,
                    ),
                });

                overlapping_idx += 1;
            }
        }

        Ok(Self {
            regions: tasks,
            partitions: file_partition
                .into_iter()
                .zip(task_assignment)
                .map(|(f_part, scanner)| PartitionContext {
                    reader: f_part,
                    tasks: scanner,
                })
                .collect(),
        })
    }

    /// Run the task in parallel
    pub fn run(self) -> TaskOutputVec<T::Output> {
        #[cfg(not(feature = "seq-task"))]
        let part_iter = self.partitions.into_par_iter();
        #[cfg(feature = "seq-task")]
        let part_iter = self.partitions.into_iter();
        let mut task_result: Vec<_> = part_iter
            .map(|mut partition| partition.execute())
            .flatten()
            .collect();
        task_result.sort_by_key(|part| part.task_id);

        let mut result = TaskOutputVec {
            chrom_list: vec![],
            results: vec![],
        };

        let mut task_result_idx = 0;
        let mut chrom_dict = HashMap::new();
        for (id, region_ctx) in self.regions.into_iter().enumerate() {
            let region = region_ctx.region();
            let chrom_idx = if let Some(&idx) = chrom_dict.get(region.0) {
                idx
            } else {
                chrom_dict.insert(region.0.to_string(), chrom_dict.len());
                result.chrom_list.push(region.0.to_string());
                chrom_dict.len() - 1
            };
            let mut region_partition_results = vec![];
            while task_result_idx < task_result.len() && task_result[task_result_idx].task_id <= id
            {
                if task_result[task_result_idx].task_id == id {
                    region_partition_results.push(task_result[task_result_idx].partition.result());
                }
                task_result_idx += 1;
            }
            let final_result = region_ctx.combine(&region_partition_results);
            result
                .results
                .push((chrom_idx, region.1, region.2, final_result));
        }
        result
    }
}
