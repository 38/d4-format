mod context;
mod mean;
mod histogram;

pub use context::TaskContext;
pub use mean::Mean;
pub use histogram::Histogram;

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
    /// Convert the task into the result
    fn into_result(self) -> Self::ResultType;
}
