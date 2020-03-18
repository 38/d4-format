use super::{Task, TaskPartition};
use std::ops::Range;

pub struct Histogram(String, u32, u32);

pub struct Partition {
    range: (u32, u32),
    base: i32,
    histogram: Vec<u32>,
    below: u32,
    above: u32,
}

impl TaskPartition for Partition {
    type PartitionParam = Range<i32>;
    type ResultType = (u32, Vec<u32>, u32);
    fn new(left: u32, right: u32, param:Range<i32>) -> Self {
        let base = param.start;
        let size = (param.end - param.start).max(0) as usize;
        Self {
            base,
            range: (left, right),
            histogram: vec![0; size],
            below: 0,
            above: 0,
        }
    }
    fn scope(&self) -> (u32, u32) {
        self.range
    }
    #[inline(always)]
    fn feed(&mut self, _: u32, value: i32) -> bool {
        let offset = value - self.base;
        if offset < 0 {
            self.below += 1;
            return true;
        }
        if offset >= self.histogram.len() as i32 {
            self.above += 1;
            return true;
        }
        self.histogram[offset as usize] += 1;
        true
    }
    fn into_result(self) -> (u32, Vec<u32>, u32) {
        (self.below, self.histogram, self.above)
    }
}

impl Task for Histogram {
    type Partition = Partition;
    type Output = (u32, Vec<u32>, u32);
    fn new(chr: &str, left: u32, right: u32) -> Self {
        Histogram(chr.to_string(), left, right)
    }
    fn region(&self) -> (&str, u32, u32) {
        (self.0.as_ref(), self.1, self.2)
    }
    fn combine(&self, parts: &[(u32, Vec<u32>, u32)]) -> (u32, Vec<u32>, u32) {
        if parts.is_empty() {
            return (0, vec![], 0);
        }

        let mut histogram = vec![0; parts[0].1.len()];
        let mut below = 0;
        let mut above = 0;
        for (b, v, a) in parts {
            for (idx, value) in v.into_iter().enumerate() {
                histogram[idx] += value;
            }
            below += b;
            above += a;
        }
        (below, histogram, above)
    }
}
