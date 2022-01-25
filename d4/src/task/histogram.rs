use super::{SimpleTask, Task, TaskPartition};
use std::{iter::Once, ops::Range};

#[derive(Clone)]
pub struct Histogram(String, u32, u32, Range<i32>);

impl Histogram {
    pub fn with_bin_range(chrom: &str, begin: u32, end: u32, bin_range: Range<i32>) -> Self {
        Histogram(chrom.to_string(), begin, end, bin_range)
    }
}

impl SimpleTask for Histogram {
    fn new(chr: &str, start: u32, end: u32) -> Self {
        Self(chr.to_string(), start, end, 0..1000)
    }
}

pub struct Partition {
    base: i32,
    range: usize,
    histogram: Option<Vec<u32>>,
    below: u32,
    above: u32,
}

impl TaskPartition<Once<i32>> for Partition {
    type ParentType = Histogram;
    type ResultType = (u32, Vec<u32>, u32);
    fn new(_: u32, _: u32, parent: &Histogram) -> Self {
        let param = &parent.3;
        let base = param.start;
        let range = (param.end - param.start).max(0) as usize;
        Self {
            base,
            histogram: None,
            range,
            below: 0,
            above: 0,
        }
    }

    #[inline(always)]
    fn init(&mut self) {
        self.histogram = Some(vec![0; self.range]);
    }

    #[inline(always)]
    fn feed_range(&mut self, left: u32, right: u32, value: &mut Once<i32>) -> bool {
        let value = value.next().unwrap();
        let offset = value - self.base;
        let histogram = self.histogram.as_mut().unwrap();
        if offset < 0 {
            self.below += 1;
            return true;
        }
        if offset >= histogram.len() as i32 {
            self.above += right - left;
            return true;
        }
        histogram[offset as usize] += right - left;
        true
    }

    fn result(&mut self) -> Self::ResultType {
        (self.below, self.histogram.take().unwrap(), self.above)
    }
}

impl Task<std::iter::Once<i32>> for Histogram {
    type Partition = Partition;
    type Output = (u32, Vec<u32>, u32);

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
            for (idx, value) in v.iter().enumerate() {
                histogram[idx] += value;
            }
            below += b;
            above += a;
        }
        (below, histogram, above)
    }
}
