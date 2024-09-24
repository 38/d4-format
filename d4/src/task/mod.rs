/*! The high-level parallel task interface */

mod context;
mod histogram;
mod mean;
mod sum;
mod value_range;
mod vector;
mod perc_cov;

use std::io::Result;

pub use context::TaskContext;
pub use histogram::Histogram;
pub use mean::Mean;
pub use sum::Sum;
pub use value_range::ValueRange;
pub use vector::VectorStat;
pub use perc_cov::PercentCov;

use crate::d4file::{MultiTrackPartitionReader, MultiTrackReader};

pub trait SimpleTask {
    fn new(chr: &str, start: u32, end: u32) -> Self;
}

pub trait IntoTaskVec<RowType: Iterator<Item = i32> + ExactSizeIterator, T: Task<RowType>> {
    fn into_task_vec(self) -> Vec<T>;
}

impl<T: SimpleTask + Task<R>, R: Iterator<Item = i32> + ExactSizeIterator, N: AsRef<str>>
    IntoTaskVec<R, T> for &'_ [(N, u32, u32)]
{
    fn into_task_vec(self) -> Vec<T> {
        self.iter()
            .map(|(chr, beg, end)| T::new(chr.as_ref(), *beg, *end))
            .collect()
    }
}

impl<T: SimpleTask + Task<R>, R: Iterator<Item = i32> + ExactSizeIterator, N: AsRef<str>>
    IntoTaskVec<R, T> for &'_ Vec<(N, u32, u32)>
{
    fn into_task_vec(self) -> Vec<T> {
        self.iter()
            .map(|(chr, beg, end)| T::new(chr.as_ref(), *beg, *end))
            .collect()
    }
}

impl<T: Task<R>, R: Iterator<Item = i32> + ExactSizeIterator> IntoTaskVec<R, T> for Vec<T> {
    fn into_task_vec(self) -> Vec<T> {
        self
    }
}

pub struct TaskOutput<'a, T> {
    pub chrom: &'a str,
    pub chrom_id: usize,
    pub begin: u32,
    pub end: u32,
    pub output: &'a T,
}

pub struct TaskOutputVec<T> {
    chrom_list: Vec<String>,
    results: Vec<(usize, u32, u32, T)>,
}

pub struct TaskOutputIter<'a, T> {
    idx: usize,
    data: &'a TaskOutputVec<T>,
}

impl<T> TaskOutputVec<T> {
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.results.len()
    }
}

impl<'a, T> IntoIterator for &'a TaskOutputVec<T> {
    type IntoIter = TaskOutputIter<'a, T>;
    type Item = TaskOutput<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        TaskOutputIter { idx: 0, data: self }
    }
}

impl<'a, T> Iterator for TaskOutputIter<'a, T> {
    type Item = TaskOutput<'a, T>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.idx < self.data.results.len() {
            let idx = self.idx;
            self.idx += 1;
            return Some(TaskOutput {
                chrom: &self.data.chrom_list[self.data.results[idx].0],
                chrom_id: self.data.results[idx].0,
                begin: self.data.results[idx].1,
                end: self.data.results[idx].2,
                output: &self.data.results[idx].3,
            });
        }
        None
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.data.results.len() - self.idx;
        (size, Some(size))
    }
}

impl<'a, T> ExactSizeIterator for TaskOutputIter<'a, T> {}

/// An abstracted task
pub trait Task<RowType: Iterator<Item = i32> + ExactSizeIterator> {
    /// The type for each partition
    type Partition: TaskPartition<RowType, ParentType = Self> + Send;
    /// The output type for the entire task
    type Output;
    /// Get the effective range of this task
    fn region(&self) -> (&str, u32, u32);
    /// Combine all the partitions and finalize the computation
    fn combine(
        &self,
        parts: &[<Self::Partition as TaskPartition<RowType>>::ResultType],
    ) -> Self::Output;

    fn create_task<R, P, RS: IntoTaskVec<RowType, Self>>(
        reader: &mut R,
        regions: RS,
    ) -> Result<TaskContext<R, Self>>
    where
        Self: Sized,
        R: MultiTrackReader<PartitionType = P>,
        P: MultiTrackPartitionReader<RowType = RowType>,
        R::PartitionType: Send,
    {
        let tasks: Vec<Self> = regions.into_task_vec();
        TaskContext::new(reader, tasks)
    }
}

/// A partition of a task
pub trait TaskPartition<RowType: Iterator<Item = i32> + ExactSizeIterator>: Send {
    type ParentType: Task<RowType>;
    /// The result type for this task partition
    type ResultType: Send + Clone;
    /// The type for a single row
    fn new(left: u32, right: u32, parent: &Self::ParentType) -> Self;
    /// Initlize the task
    #[inline(always)]
    fn init(&mut self) {}
    /// Feed one value to the task
    #[inline(always)]
    fn feed(&mut self, pos: u32, value: &mut RowType) -> bool {
        self.feed_range(pos, pos + 1, value)
    }
    /// Feed a range of position that has the same value
    fn feed_range(&mut self, left: u32, right: u32, value: &mut RowType) -> bool;
    /// Convert the task into the result
    fn result(&mut self) -> Self::ResultType;
}
