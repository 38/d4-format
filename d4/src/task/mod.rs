/*! The high-level parallel task interface */

mod context;
mod histogram;
mod mean;
mod value_range;

use std::io::Result;

pub use context::TaskContext;
pub use histogram::Histogram;
pub use mean::Mean;
pub use value_range::ValueRange;

use crate::{ptab::PTableReader, stab::STableReader, D4TrackReader};

/// An abstracted task
pub trait Task {
    /// The type for each partition
    type Partition: TaskPartition;
    /// The output type for the entire task
    type Output;
    /// Create a new task for the given range
    fn new(chr: &str, start: u32, end: u32) -> Self;
    /// Get the effective range of this task
    fn region(&self) -> (&str, u32, u32);
    /// Combine all the partitions and finalize the computation
    fn combine(&self, parts: &[<Self::Partition as TaskPartition>::ResultType]) -> Self::Output;

    fn create_task<P: PTableReader, S: STableReader, C: AsRef<str>>(
        reader: &mut D4TrackReader<P, S>,
        regions: &[(C, u32, u32)],
        partition_param: <Self::Partition as TaskPartition>::PartitionParam,
    ) -> Result<TaskContext<P, S, Self>>
    where
        Self: Sized,
        P::Partition: Send,
        S::Partition: Send,
        Self::Partition: Send,
    {
        TaskContext::new(reader, regions, partition_param)
    }
}

/// A partition of a task
pub trait TaskPartition: Send {
    /// An additional parameter that provided to construct a task partition
    type PartitionParam: Clone;
    /// The result type for this task partition
    type ResultType: Send + Clone;
    /// Create a new task partition in the given effective range
    fn new(left: u32, right: u32, param: Self::PartitionParam) -> Self;
    /// Query the scope for current task partition
    fn scope(&self) -> (u32, u32);
    /// Feed one value to the task
    fn feed(&mut self, pos: u32, value: i32) -> bool;
    /// Feed a range of position that has the same value
    fn feed_range(&mut self, left: u32, right: u32, value: i32) -> bool {
        for pos in left..right {
            self.feed(pos, value);
        }
        true
    }
    /// Convert the task into the result
    fn into_result(self) -> Self::ResultType;
}
