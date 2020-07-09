use rayon::prelude::*;
use std::io::Result;

use super::{Task, TaskPartition};
use crate::d4file::D4FileReader;
use crate::ptab::{DecodeResult, Decoder, PTablePartitionReader, PTableReader};
use crate::stab::{STablePartitionReader, STableReader};

struct PartitionContext<P: PTableReader, S: STableReader, T: Task> {
    primary: P::Partition,
    secondary: S::Partition,
    tasks: Vec<T::Partition>,
}

impl<P: PTableReader, S: STableReader, T: Task> PartitionContext<P, S, T> {
    fn execute(
        &mut self,
    ) -> Result<
        Vec<(
            String,
            u32,
            u32,
            <T::Partition as TaskPartition>::ResultType,
        )>,
    > {
        let chr = self.primary.region().0.to_string();
        let per_base = self.primary.bit_width() > 0;
        let mut decoder = self.primary.as_decoder();
        let mut result = vec![];
        for mut stat_part in std::mem::replace(&mut self.tasks, vec![]) {
            let (part_left, part_right) = stat_part.scope();
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
                        stat_part.feed(pos as u32, value);
                    },
                );
            } else {
                let iter = self.secondary.seek_iter(part_left);
                for (mut left, mut right, value) in iter {
                    left = left.max(part_left);
                    right = right.min(part_right);
                    stat_part.feed_range(left, right, value);
                    if right == part_right {
                        break;
                    }
                }
            }
            result.push((chr.clone(), part_left, part_right, stat_part.into_result()));
        }
        Ok(result)
    }
}

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

        let mut task_assignment: Vec<Vec<T::Partition>> =
            (0..file_partition.len()).map(|_| vec![]).collect();

        // Now assign the d4 file partition to each task assignments
        let mut idx = 0;
        for region in regions.iter() {
            // for each task region, try to assign it to one or more file partitions
            // Before actually perform the assignment, since we have sorted both task_partition and
            // regions with the same ordering function. So we just loop over the file regions that doesn't
            // have any task on it
            while idx < file_partition.len()
                && (file_partition[idx].0.region().0 < region.0.as_ref()
                    || file_partition[idx].0.region().2 < region.1)
            {
                idx += 1;
            }
            if file_partition[idx].0.region().0 > region.0.as_ref() {
                continue;
            }
            if idx >= file_partition.len() {
                break;
            }
            while idx < file_partition.len() {
                let file_part = &file_partition[idx];
                let file_region = file_part.0.region();
                let actual_left = region.1.max(file_region.1);
                let actual_right = region.2.min(file_region.2);
                // If the size of the assignment on this part is zero, this indicates
                // the task the completely assigned. Thus we are good to go.
                if file_region.0 != region.0 || actual_right - actual_left == 0 {
                    break;
                }
                task_assignment[idx].push(<<T as Task>::Partition as TaskPartition>::new(
                    actual_left,
                    actual_right,
                    partition_param.clone(),
                ));
                if actual_right == file_region.2 {
                    idx += 1;
                }
                if actual_right == region.2 {
                    break;
                }
            }
        }

        Ok(Self {
            regions: regions
                .into_iter()
                .map(|r| T::new(r.0.as_ref(), r.1, r.2))
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

    pub fn run(mut self) -> Vec<(String, u32, u32, T::Output)> {
        let mut tasks = vec![];
        std::mem::swap(&mut tasks, &mut self.partitions);

        let mut task_result: Vec<_> = tasks
            .into_par_iter()
            .map(|mut partition| partition.execute().unwrap())
            .flatten()
            .collect();
        task_result.sort_by_key(|k| (k.0.clone(), k.1, k.2));

        let mut result = vec![];

        let mut idx = 0;
        for region_ctx in self.regions {
            let region = region_ctx.region();
            let mut partitions = vec![];
            while idx < task_result.len()
                && task_result[idx].0.as_str() <= region.0
                && task_result[idx].2 <= region.2
            {
                let next_result = &task_result[idx];
                let (chr, left, right, _) = next_result;
                if &region.0 == chr && region.1 <= *left && *right <= region.2 {
                    partitions.push(task_result[idx].3.clone());
                }
                idx += 1;
            }
            let final_result = region_ctx.combine(&partitions);
            result.push((region.0.to_string(), region.1, region.2, final_result));
        }
        result
    }
}
