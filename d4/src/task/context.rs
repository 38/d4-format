use rayon::prelude::*;
use std::io::Result;
use std::iter::Once;

use super::{Task, TaskPartition};
use crate::d4file::{D4FilePartition, D4TrackReader, DataScanner, MultiTrackPartitionReader, MultiTrackReader, TrackValue};
use crate::{ptab::PTableReader, stab::STableReader};

struct PartitionExecutionResult<T : Task> {
    task_id: usize,
    result: <T::Partition as TaskPartition>::ResultType,
}

struct TaskScanner<P : TaskPartition> {
    task_id: usize,
    partition: P,
}

impl <P: TaskPartition> DataScanner<Once<TrackValue>> for TaskScanner<P> {
    fn get_range(&self) -> (u32, u32) {
        self.partition.scope()
    }

    fn feed_row(&mut self, pos:u32, mut row: Once<TrackValue>) -> bool {
        let TrackValue { value, ..}= row.next().unwrap();
        self.partition.feed(pos, value)
    }

    fn feed_rows(&mut self, begin: u32, end: u32, mut row: Once<TrackValue>) -> bool {
        let TrackValue { value, ..}= row.next().unwrap();
        self.partition.feed_range(begin, end, value)
    }
}

struct PartitionContext<R: MultiTrackPartitionReader, T: Task> {
    reader: R,
    tasks: Vec<TaskScanner<T::Partition>>,
}

impl<R: MultiTrackPartitionReader<RowType = Once<TrackValue>>, T: Task> PartitionContext<R, T> {
    #[allow(clippy::type_complexity)]
    fn execute(
        &mut self,
    ) -> Vec<PartitionExecutionResult<T>> {

        self.reader.scan_partition(self.tasks.as_mut_slice());
        
        std::mem::take(&mut self.tasks)
            .into_iter()
            .map(|scanner| {
                PartitionExecutionResult { task_id: scanner.task_id, result: scanner.partition.into_result() }
            })
            .collect()
    }
}

/// The context for a parallel task
pub struct TaskContext<P: PTableReader, S: STableReader, T: Task> {
    regions: Vec<T>,
    partitions: Vec<PartitionContext<D4FilePartition<P, S>, T>>,
}

impl<P: PTableReader, S: STableReader, T: Task> TaskContext<P, S, T>
where
    P::Partition: Send,
    S::Partition: Send,
    T::Partition: Send,
{
    /// Create a new task that processing the given file
    pub fn new<Name: AsRef<str>>(
        reader: &mut D4TrackReader<P, S>,
        regions: &[(Name, u32, u32)],
        partition_param: <<T as Task>::Partition as TaskPartition>::PartitionParam,
    ) -> Result<TaskContext<P, S, T>> {
        let mut file_partition = MultiTrackReader::split(reader, Some(10_000_000))?;
        file_partition.sort_by_key(|p| (p.chrom().to_string(), p.begin(), p.end()));
        let mut regions: Vec<_> = regions
            .iter()
            .map(|(c, b, e)| (c.as_ref().to_string(), *b, *e))
            .collect();
        regions.sort();

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
            while idx < regions.len()
                && (regions[idx].0.as_str() < chr
                    || (regions[idx].0.as_str() == chr && regions[idx].2 < fpl))
            {
                idx += 1;
            }

            let mut overlapping_idx = idx;

            while overlapping_idx < regions.len() {
                let this = &regions[overlapping_idx];
                let (c, l, r) = (&this.0, this.1, this.2);
                if c != chr || fpr < l {
                    break;
                }
                let actual_left = fpl.max(l);
                let actual_right = fpr.min(r);

                task_assignment[fpid].push(TaskScanner{
                    task_id: overlapping_idx,
                    partition: <<T as Task>::Partition as TaskPartition>::new(
                        actual_left,
                        actual_right,
                        partition_param.clone(),
                    ),
                });

                overlapping_idx += 1;
            }
        }

        Ok(Self {
            regions: regions
                .into_iter()
                .map(|r| T::new(&r.0, r.1, r.2))
                .collect(),
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
    pub fn run(mut self) -> Vec<(String, u32, u32, T::Output)> {
        let mut tasks = vec![];
        std::mem::swap(&mut tasks, &mut self.partitions);

        let mut task_result: Vec<_> = tasks
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
            while task_result_idx < task_result.len() && task_result[task_result_idx].task_id <= id {
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
