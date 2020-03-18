use framefile::mode::{ReadOnly, ReadWrite};
use framefile::Directory;
use std::fs::File;
use std::io::Result;

use crate::header::Header;

mod simple_kv;

pub trait STableWriter: Sized {
    type Partition: STablePartitionWriter;
    fn create(root: &mut Directory<'static, ReadWrite, File>, header: &Header) -> Result<Self>;
    fn split(&mut self, partitions: &[(&str, u32, u32)]) -> Result<Vec<Self::Partition>>;
}

pub trait STablePartitionWriter: Send {
    fn encode(&mut self, pos: u32, value: i32) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
}

pub use simple_kv::SimpleKeyValueWriter;

pub trait STableReader: Sized {
    type Partition: STablePartitionReader;
    fn create(root: &mut Directory<'static, ReadOnly, File>, header: &Header) -> Result<Self>;
    fn split(&mut self, partitions: &[(&str, u32, u32)]) -> Result<Vec<Self::Partition>>;
}

pub trait STablePartitionReader {
    type IteratorState: Sized + Default;
    fn decode(&mut self, pos: u32) -> Option<i32>;
    fn next_record(&self, state: &mut Self::IteratorState) -> Option<(u32, u32, i32)>;
    fn into_iter(&self) -> RecordIterator<Self> {
        RecordIterator(self, Self::IteratorState::default())
    }
}

pub struct RecordIterator<'a, S: STablePartitionReader + ?Sized>(&'a S, S::IteratorState);
impl<'a, S: STablePartitionReader> Iterator for RecordIterator<'a, S> {
    type Item = (u32, u32, i32);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_record(&mut self.1)
    }
}

pub use simple_kv::SimpleKeyValueReader;
pub use simple_kv::{RangeRecord, SingleRecord};
