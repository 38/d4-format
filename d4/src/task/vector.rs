use std::iter::Once;

use super::{SimpleTask, Task, TaskPartition};

pub struct VectorStat<T: Task<Once<i32>>> {
    scalar_tasks: Vec<T>,
}

//TODO: think about how to create the vector stat efficiently
impl<T: Task<Once<i32>>> VectorStat<T> {
    pub fn create_vector_task_for_region(size: usize, chr: &str, begin: u32, end: u32) -> Self
    where
        T: SimpleTask,
    {
        Self {
            scalar_tasks: (0..size).map(|_| T::new(chr, begin, end)).collect(),
        }
    }
    pub fn create_vector_task(size: usize, task: T) -> Self
    where
        T: Clone,
    {
        Self {
            scalar_tasks: (0..size).map(move |_| task.clone()).collect(),
        }
    }
}

pub struct VectorStatPartition<T: Task<Once<i32>>> {
    scalar_parts: Vec<T::Partition>,
}

impl<R: Iterator<Item = i32> + ExactSizeIterator, T: Task<Once<i32>>> TaskPartition<R>
    for VectorStatPartition<T>
{
    type ParentType = VectorStat<T>;
    type ResultType = Vec<<T::Partition as TaskPartition<Once<i32>>>::ResultType>;

    fn new(left: u32, right: u32, parent: &Self::ParentType) -> Self {
        Self {
            scalar_parts: parent
                .scalar_tasks
                .iter()
                .map(|task| <T::Partition as TaskPartition<Once<i32>>>::new(left, right, task))
                .collect(),
        }
    }

    fn feed(&mut self, pos: u32, value: &mut R) -> bool {
        for (task, value) in self.scalar_parts.iter_mut().zip(value) {
            task.feed(pos, &mut std::iter::once(value));
        }
        true
    }

    fn feed_range(&mut self, left: u32, right: u32, value: &mut R) -> bool {
        for (task, value) in self.scalar_parts.iter_mut().zip(value) {
            task.feed_range(left, right, &mut std::iter::once(value));
        }
        true
    }

    fn result(&mut self) -> Self::ResultType {
        std::mem::take(&mut self.scalar_parts)
            .into_iter()
            .map(|mut task| task.result())
            .collect()
    }
}

impl<R: Iterator<Item = i32> + ExactSizeIterator, T: Task<Once<i32>>> Task<R> for VectorStat<T>
where
    <T::Partition as TaskPartition<Once<i32>>>::ResultType: Send,
{
    type Partition = VectorStatPartition<T>;

    type Output = Vec<T::Output>;

    fn region(&self) -> (&str, u32, u32) {
        self.scalar_tasks[0].region()
    }

    fn combine(
        &self,
        parts: &[<Self::Partition as super::TaskPartition<R>>::ResultType],
    ) -> Self::Output {
        if parts.is_empty() {
            return vec![];
        }

        let mut transposed: Vec<Vec<_>> = parts[0].iter().map(|x| vec![x.clone()]).collect();

        for part in parts.iter().skip(1) {
            for (idx, result) in part.iter().enumerate() {
                transposed[idx].push(result.clone());
            }
        }

        self.scalar_tasks
            .iter()
            .zip(transposed.iter())
            .map(|(scalar_task, scalar_result)| scalar_task.combine(scalar_result.as_slice()))
            .collect()
    }
}
