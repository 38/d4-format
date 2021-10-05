use std::iter::Once;

use super::{SimpleTask, Task, TaskPartition};

#[derive(Clone)]
pub struct Mean(String, u32, u32);

impl SimpleTask for Mean {
    fn new(chr: &str, start: u32, end: u32) -> Self {
        Self(chr.to_string(), start, end)
    }
}

pub struct MeanPartition {
    range: (u32, u32),
    sum: i64,
}

impl TaskPartition<Once<i32>> for MeanPartition {
    type ParentType = Mean;
    type ResultType = (i64, usize);
    fn new(left: u32, right: u32, _: &Self::ParentType) -> Self {
        Self {
            range: (left, right),
            sum: 0,
        }
    }
    fn scope(&self) -> (u32, u32) {
        self.range
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

    fn into_result(self) -> (i64, usize) {
        (self.sum, (self.range.1 - self.range.0) as usize)
    }
}

impl Task<std::iter::Once<i32>> for Mean {
    type Partition = MeanPartition;
    type Output = f64;

    fn region(&self) -> (&str, u32, u32) {
        (self.0.as_ref(), self.1, self.2)
    }

    fn combine(&self, parts: &[(i64, usize)]) -> f64 {
        let mut values = 0;
        let mut counts = 0;
        for (v, c) in parts {
            values += v;
            counts += c;
        }
        if counts == 0 {
            return 0.0;
        }
        values as f64 / counts as f64
    }
}
