use std::{io::Result, iter::Once, marker::PhantomData};

use crate::{
    task::{Task, TaskContext, TaskOutputVec, TaskPartition},
    D4TrackReader,
};

use super::DataIndexType;

pub trait DataSummary: Sized + Send + Sync + Clone {
    const INDEX_NAME: &'static str;
    const INDEX_TYPE_CODE: DataIndexType;
    fn identity() -> Self;
    fn add_data(&self, pos: u32, val: i32) -> Self;
    fn combine(&self, other: &Self) -> Self;
    fn to_native_byte_order(&self) -> Self;
    fn to_format_byte_order(&self) -> Self;
    fn add_data_range(&self, begin: u32, end: u32, val: i32) -> Self {
        let mut ret = Self::identity();
        for pos in begin..end {
            ret = ret.add_data(pos, val);
        }
        ret
    }
    fn from_data_iter<I: Iterator<Item = (u32, i32)>>(iter: I) -> Self {
        iter.fold(Self::identity(), |sum, (pos, val)| sum.add_data(pos, val))
    }
    fn combine_iter<'a, I: Iterator<Item = &'a Self>>(iter: I) -> Self
    where
        Self: 'a,
    {
        iter.fold(Self::identity(), |sum, value| sum.combine(value))
    }
    fn run_summary_task(reader: &mut D4TrackReader, bin_size: u32) -> Result<TaskOutputVec<Self>> {
        let chrom_list = reader.header().chrom_list().to_owned();
        let task_array: Vec<_> = chrom_list
            .iter()
            .flat_map(|seq| {
                let chrom = seq.name.as_str();
                (0..(seq.size as u32 + bin_size - 1) / bin_size).map(move |idx| {
                    (
                        chrom,
                        idx * bin_size,
                        ((idx + 1) * bin_size).min(seq.size as u32),
                    )
                })
            })
            .map(|(chrom, begin, end)| DataSummaryTask::<Self> {
                chrom,
                begin,
                end,
                _phantom_data: Default::default(),
            })
            .collect();

        Ok(TaskContext::new(reader, task_array)?.run())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Sum(f64);

impl Sum {
    pub fn mean(&self, base_count: u32) -> f64 {
        self.0 / base_count as f64
    }
    pub fn sum(&self) -> f64 {
        self.0
    }
}

impl DataSummary for Sum {
    fn identity() -> Self {
        Sum(0.0)
    }

    fn add_data(&self, _: u32, val: i32) -> Self {
        Sum(self.0 + val as f64)
    }

    fn add_data_range(&self, begin: u32, end: u32, val: i32) -> Self {
        Sum(self.0 + (end - begin) as f64 * val as f64)
    }

    fn combine(&self, other: &Self) -> Self {
        Sum(self.0 + other.0)
    }

    fn to_native_byte_order(&self) -> Self {
        *self
    }

    fn to_format_byte_order(&self) -> Self {
        *self
    }

    const INDEX_NAME: &'static str = "sum_index";

    const INDEX_TYPE_CODE: DataIndexType = DataIndexType::Sum;
}

pub struct DataSummaryTask<'a, T: DataSummary> {
    chrom: &'a str,
    begin: u32,
    end: u32,
    _phantom_data: PhantomData<T>,
}

pub struct DataSummaryTaskPart<'a, T: DataSummary> {
    sum: T,
    _phantom_data: PhantomData<&'a ()>,
}

impl<'a, T: DataSummary> TaskPartition<Once<i32>> for DataSummaryTaskPart<'a, T> {
    type ParentType = DataSummaryTask<'a, T>;

    type ResultType = T;

    fn new(_left: u32, _right: u32, _parent: &Self::ParentType) -> Self {
        Self {
            sum: T::identity(),
            _phantom_data: PhantomData,
        }
    }

    fn feed(&mut self, pos: u32, value: &mut Once<i32>) -> bool {
        self.sum = self.sum.add_data(pos, value.next().unwrap());
        true
    }

    fn feed_range(&mut self, left: u32, right: u32, value: &mut Once<i32>) -> bool {
        self.sum = self.sum.add_data_range(left, right, value.next().unwrap());
        true
    }

    fn result(&mut self) -> Self::ResultType {
        self.sum.clone()
    }
}

impl<'a, T: DataSummary> Task<Once<i32>> for DataSummaryTask<'a, T> {
    type Partition = DataSummaryTaskPart<'a, T>;

    type Output = T;

    fn region(&self) -> (&str, u32, u32) {
        (self.chrom, self.begin, self.end)
    }

    fn combine(&self, parts: &[T]) -> Self::Output {
        T::combine_iter(parts.iter())
    }
}
