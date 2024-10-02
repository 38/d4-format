use super::{Task, TaskPartition};
use std::iter::Once;

#[derive(Clone)]
pub struct PercentCovPart {
    //thresholds should be sorted
    thresholds: Vec<u32>,
    counts: Vec<u32>,
}

impl TaskPartition<Once<i32>> for PercentCovPart {
    type ParentType = PercentCov;
    type ResultType = Vec<u32>;
    fn new(_left: u32, _right: u32, parent: &Self::ParentType) -> Self {
        Self {
            thresholds: parent.3.clone(),
            counts: vec![0; parent.3.len()],
        }
    }
    // #[inline(always)]
    fn feed(&mut self, _: u32, value: &mut Once<i32>) -> bool {
        let value = value.next().unwrap();
        for (i, thresh) in self.thresholds.iter().enumerate() {
            if value as u32 >= *thresh {
                self.counts[i] += 1
            }
        }
        true
    }
    // #[inline(always)]
    fn feed_range(&mut self, left: u32, right: u32, value: &mut Once<i32>) -> bool {
        let value = value.next().unwrap();
        for (i, thresh) in self.thresholds.iter().enumerate() {
            if value as u32 >= *thresh {
                self.counts[i] += right - left
            }
        }
        true
    }

    fn result(&mut self) -> Self::ResultType {
        self.counts.clone()
    }
}

pub struct PercentCov(String, u32, u32, Vec<u32>);

impl PercentCov {
    pub fn new(chrom: &str, begin: u32, end: u32, thresholds: Vec<u32>) -> Self {
        PercentCov(chrom.to_string(), begin, end, thresholds)
    }
}

impl Task<std::iter::Once<i32>> for PercentCov {
    type Partition = PercentCovPart;
    type Output = Vec<f32>;
    fn region(&self) -> (&str, u32, u32) {
        (self.0.as_str(), self.1, self.2)
    }
    fn combine(&self, parts: &[Vec<u32>]) -> Self::Output {
        let divisor = (self.2 - self.1) as f32;

        let mut sums: Vec<u32> = vec![0; self.3.len()];

        // Sum the vectors index-wise

        for part in parts {
            for (i, &value) in part.iter().enumerate() {
                sums[i] += value;
            }
        }

        let result: Vec<f32> = sums.into_iter().map(|x| x as f32 / divisor).collect();

        result
    }
}
