use std::iter::Once;

use super::{SimpleTask, Task, TaskPartition};

#[derive(Clone)]
pub struct Sum {
    chr: String,
    start: u32,
    end: u32,
}

impl Sum {
    #[allow(clippy::self_named_constructors)]
    pub fn sum(chr: &str, start: u32, end: u32) -> Self {
        Self {
            chr: chr.to_string(),
            start,
            end,
        }
    }
}

impl SimpleTask for Sum {
    fn new(chr: &str, start: u32, end: u32) -> Self {
        Self {
            chr: chr.to_string(),
            start,
            end,
        }
    }
}

pub struct SumPartition {
    sum: i64,
}

impl TaskPartition<Once<i32>> for SumPartition {
    type ParentType = Sum;
    type ResultType = i64;
    fn new(_: u32, _: u32, _: &Self::ParentType) -> Self {
        Self { sum: 0 }
    }
    #[inline(always)]
    fn feed(&mut self, _: u32, value: &mut Once<i32>) -> bool {
        let value = value.next().unwrap();
        self.sum += value as i64;
        true
    }
    #[inline(always)]
    fn feed_range(&mut self, left: u32, right: u32, value: &mut Once<i32>) -> bool {
        let value = value.next().unwrap();
        self.sum += value as i64 * (right - left) as i64;
        true
    }

    fn result(&mut self) -> Self::ResultType {
        self.sum
    }
}

impl Task<std::iter::Once<i32>> for Sum {
    type Partition = SumPartition;
    type Output = i64;

    fn region(&self) -> (&str, u32, u32) {
        (self.chr.as_ref(), self.start, self.end)
    }

    fn combine(&self, parts: &[i64]) -> i64 {
        let mut values = 0;
        for v in parts {
            values += v;
        }
        values
    }
}
