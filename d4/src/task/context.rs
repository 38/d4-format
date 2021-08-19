use rayon::prelude::*;
use std::io::Result;

use super::{Task, TaskPartition};
use crate::d4file::D4FileReader;
use crate::ptab::{DecodeResult, Decoder, PTablePartitionReader, PTableReader};
use crate::stab::{STablePartitionReader, STableReader};

struct PartitionContext<P: PTableReader, S: STableReader, T: Task> {
    primary: P::Partition,
    secondary: S::Partition,
    tasks: Vec<(usize, T::Partition)>,
}

impl<P: PTableReader, S: STableReader, T: Task> PartitionContext<P, S, T> {
    fn execute(
        &mut self,
    ) -> Result<
        Vec<(
            usize,
            String,
            u32,
            u32,
            <T::Partition as TaskPartition>::ResultType,
        )>,
    > {
        let chr = self.primary.region().0.to_string();
        let per_base = self.primary.bit_width() > 0;
        let mut decoder = self.primary.as_decoder();
        let mut break_points: Vec<_> = self
            .tasks
            .iter()
            .map(|(_, x)| vec![x.scope().0, x.scope().1].into_iter())
            .flatten()
            .collect();
        break_points.sort();

        if break_points.is_empty() {
            return Ok(vec![]);
        }

        for idx in 0..break_points.len() - 1 {
            if break_points[idx] == break_points[idx + 1] {
                continue;
            }
            let part_left = break_points[idx];
            let part_right = break_points[idx + 1];

            let active_tasks: Vec<_> = (0..self.tasks.len())
                .filter(|&x| {
                    let (l, r) = self.tasks[x].1.scope();
                    l <= part_left && part_right <= r
                })
                .collect();

            if active_tasks.is_empty() {
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
                        for &task_id in active_tasks.iter() {
                            self.tasks[task_id].1.feed(pos as u32, value);
                        }
                    },
                );
            } else {
                let iter = self.secondary.seek_iter(part_left);
                for (mut left, mut right, value) in iter {
                    left = left.max(part_left);
                    right = right.min(part_right).max(left);
                    for &task_id in active_tasks.iter() {
                        self.tasks[task_id].1.feed_range(left, right, value);
                    }
                    if right == part_right {
                        break;
                    }
                }
            }
        }
        let result = std::mem::take(&mut self.tasks)
            .into_iter()
            .map(|task| {
                let (left, right) = task.1.scope();
                let chr = chr.clone();
                (task.0, chr, left, right, task.1.into_result())
            })
            .collect();
        Ok(result)
    }
}

/// The context for a parallel task
pub struct TaskContext<P: PTableReader, S: STableReader, T: Task> {
    regions: Vec<T>,
    partitions: Vec<PartitionContext<P, S, T>>,
}

impl<P: PTableReader, S: STableReader, T: Task> TaskContext<P, S, T>
where
    P::Partition: Send,
    S::Partition: Send,
    T::Partition: Send,
{
    /// Create a new task that processing the given file
    pub fn new<Name: AsRef<str>>(
        reader: &mut D4FileReader<P, S>,
        regions: &[(Name, u32, u32)],
        partition_param: <<T as Task>::Partition as TaskPartition>::PartitionParam,
    ) -> Result<TaskContext<P, S, T>> {
        let mut file_partition = reader.split(Some(10_000_000))?;
        file_partition.sort_by_key(|(p, _)| (p.region().0.to_string(), p.region().1));
        let mut regions: Vec<_> = regions
            .iter()
            .map(|(c, b, e)| (c.as_ref().to_string(), *b, *e))
            .collect();
        regions.sort();

        let mut task_assignment: Vec<Vec<(usize, T::Partition)>> =
            (0..file_partition.len()).map(|_| vec![]).collect();

        // Now assign the d4 file partition to each task assignments
        let mut idx = 0;
        for (part, fpid) in file_partition.iter().zip(0..) {
            let (chr, fpl, fpr) = part.0.region();
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

                task_assignment[fpid].push((
                    overlapping_idx,
                    <<T as Task>::Partition as TaskPartition>::new(
                        actual_left,
                        actual_right,
                        partition_param.clone(),
                    ),
                ));

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
                .map(|((p, s), task)| PartitionContext {
                    primary: p,
                    secondary: s,
                    tasks: task,
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
            .map(|mut partition| partition.execute().unwrap())
            .flatten()
            .collect();
        task_result.sort_by_key(|&(region_id, ..)| region_id);

        let mut result = vec![];

        let mut task_result_idx = 0;
        for (id, region_ctx) in self.regions.into_iter().enumerate() {
            let region = region_ctx.region();
            let mut region_partition_results = vec![];
            while task_result_idx < task_result.len() && task_result[task_result_idx].0 <= id {
                if task_result[task_result_idx].0 == id {
                    region_partition_results.push(task_result[task_result_idx].4.clone());
                }
                task_result_idx += 1;
            }
            let final_result = region_ctx.combine(&region_partition_results);
            result.push((region.0.to_string(), region.1, region.2, final_result));
        }
        result
    }
}
