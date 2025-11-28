use std::iter::Once;

use super::{SimpleTask, Task, TaskPartition};

#[derive(Clone)]
pub struct Mean{
    chr: String, 
    start: u32, 
    end: u32,
}

impl Mean {
    pub fn sum(chr: &str, start: u32, end: u32) -> Self {
        Self{
            chr: chr.to_string(), start, end,
        }
    }
}

impl SimpleTask for Mean {
    fn new(chr: &str, start: u32, end: u32) -> Self {
        Self{
            chr: chr.to_string(), start, end,
        }
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
        (self.sum, (self.range.1 - self.range.0) as usize)
    }
}

impl Task<std::iter::Once<i32>> for Mean {
    type Partition = MeanPartition;
    type Output = f64;

    fn region(&self) -> (&str, u32, u32) {
        (self.chr.as_ref(), self.start, self.end)
    }

    fn combine(&self, parts: &[(i64, usize)]) -> f64 {
        let (values, counts) = parts
            .iter()
            .fold((0i64, 0usize), |(acc_v, acc_c), (v, c)| (acc_v + v, acc_c + c));

        if counts == 0 {
            return 0.0;
        }
        values as f64 / counts as f64
    }
}
