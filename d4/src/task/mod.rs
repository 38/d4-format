/*! The high-level parallel task interface */

mod context;
mod histogram;
mod mean;
mod value_range;
mod vector;

use std::io::Result;

pub use context::TaskContext;
pub use histogram::Histogram;
pub use mean::Mean;
pub use value_range::ValueRange;
pub use vector::VectorStat;

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

impl<T: SimpleTask + Task<R>, R: Iterator<Item = i32> + ExactSizeIterator> IntoTaskVec<R, T>
    for Vec<T>
{
    fn into_task_vec(self) -> Vec<T> {
        self
    }
}

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
    /// Query the scope for current task partition
    fn scope(&self) -> (u32, u32);
    /// Feed one value to the task
    fn feed(&mut self, pos: u32, value: RowType) -> bool;
    /// Feed a range of position that has the same value
    fn feed_range(&mut self, left: u32, right: u32, value: RowType) -> bool;
    /// Convert the task into the result
    fn into_result(self) -> Self::ResultType;
}
