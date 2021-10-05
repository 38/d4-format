use rayon::prelude::*;
use std::io::Result;

use super::{Task, TaskPartition};
use crate::d4file::{DataScanner, MultiTrackPartitionReader, MultiTrackReader};

struct PartitionExecutionResult<RT: Iterator<Item = i32> + ExactSizeIterator, T: Task<RT>> {
    task_id: usize,
    result: <T::Partition as TaskPartition<RT>>::ResultType,
}

struct TaskScanner<P> {
    task_id: usize,
    partition: P,
}

impl<P: TaskPartition<RT>, RT: Iterator<Item = i32> + ExactSizeIterator> DataScanner<RT>
    for TaskScanner<P>
{
    fn get_range(&self) -> (u32, u32) {
        self.partition.scope()
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
    fn execute(&mut self) -> Vec<PartitionExecutionResult<R::RowType, T>> {
        self.reader.scan_partition(self.tasks.as_mut_slice());

        std::mem::take(&mut self.tasks)
            .into_iter()
            .map(|scanner| PartitionExecutionResult {
                task_id: scanner.task_id,
                result: scanner.partition.into_result(),
            })
            .collect()
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
        file_partition.sort_by_key(|p| (p.chrom().to_string(), p.begin(), p.end()));

        tasks.sort_unstable_by_key(|t| {
            let (chr, begin, end) = t.region();
            (chr.to_string(), begin, end)
        });

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
                let actual_left = fpl.max(l);
                let actual_right = fpr.min(r);

                task_assignment[fpid].push(TaskScanner {
                    task_id: overlapping_idx,
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
                .zip(task_assignment.into_iter())
                .map(|(f_part, scanner)| PartitionContext {
                    reader: f_part,
                    tasks: scanner,
                })
                .collect(),
        })
    }

    /// Run the task in parallel
    pub fn run(self) -> Vec<(String, u32, u32, T::Output)> {
        let mut task_result: Vec<_> = self
            .partitions
            .into_par_iter()
            .map(|mut partition| partition.execute())
            .flatten()
            .collect();
        task_result.sort_by_key(|part| part.task_id);

        let mut result = vec![];

        let mut task_result_idx = 0;
        for (id, region_ctx) in self.regions.into_iter().enumerate() {
            let region = region_ctx.region();
            let mut region_partition_results = vec![];
            while task_result_idx < task_result.len() && task_result[task_result_idx].task_id <= id
            {
                if task_result[task_result_idx].task_id == id {
                    region_partition_results.push(task_result[task_result_idx].result.clone());
                }
                task_result_idx += 1;
            }
            let final_result = region_ctx.combine(&region_partition_results);
            result.push((region.0.to_string(), region.1, region.2, final_result));
        }
        result
    }
}
