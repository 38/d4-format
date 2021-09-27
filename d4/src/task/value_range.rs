use super::{Task, TaskPartition};

pub struct ValueRange(String, u32, u32);

pub struct ValueRangePartition {
    task_range: (u32, u32),
    min_value: i32,
    max_value: i32,
}

impl TaskPartition for ValueRangePartition {
    type PartitionParam = ();
    type ResultType = (i32, i32);
    fn new(left: u32, right: u32, _: Self::PartitionParam) -> Self {
        Self {
            task_range: (left, right),
            min_value: i32::MAX,
            max_value: i32::MIN,
        }
    }
    fn scope(&self) -> (u32, u32) {
        self.task_range
    }
    #[inline(always)]
    fn feed(&mut self, _: u32, value: i32) -> bool {
        self.min_value = self.min_value.min(value);
        self.max_value = self.max_value.max(value);
        true
    }
    #[inline(always)]
    fn feed_range(&mut self, _: u32, _: u32, value: i32) -> bool {
        self.feed(0, value)
    }
    fn into_result(self) -> Self::ResultType {
        (self.min_value, self.max_value)
    }
}

impl Task for ValueRange {
    type Partition = ValueRangePartition;
    type Output = (i32, i32);

    fn new(chr: &str, start: u32, end: u32) -> Self {
        Self(chr.to_string(), start, end)
    }

    fn region(&self) -> (&str, u32, u32) {
        (self.0.as_ref(), self.1, self.2)
    }

    fn combine(&self, parts: &[(i32, i32)]) -> Self::Output {
        let mut ret = (i32::MAX, i32::MIN);
        for &(min, max) in parts {
            ret.0 = ret.0.min(min);
            ret.1 = ret.1.max(max);
        }
        ret
    }
}
