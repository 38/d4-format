use d4_framefile::mode::{ReadOnly, ReadWrite};
use d4_framefile::Directory;
use std::fs::File;
use std::io::Result;

use crate::header::Header;

mod simple_kv;

pub trait STableWriter: Sized {
    type Partition: STablePartitionWriter;
    fn create(root: &mut Directory<'static, ReadWrite, File>, header: &Header) -> Result<Self>;
    fn split(&mut self, partitions: &[(&str, u32, u32)]) -> Result<Vec<Self::Partition>>;
    fn enable_deflate_encoding(&mut self, level: u32) -> &mut Self;
}

pub trait STablePartitionWriter: Send {
    fn encode(&mut self, pos: u32, value: i32) -> Result<()>;
    fn encode_record(&mut self, left: u32, right: u32, value: i32) -> Result<()>;
    fn flush(&mut self) -> Result<()>;
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

pub use simple_kv::SimpleKeyValueWriter;

pub trait STableReader: Sized {
    type Partition: STablePartitionReader;
    fn create(root: &mut Directory<'static, ReadOnly, File>, header: &Header) -> Result<Self>;
    fn split(&mut self, partitions: &[(&str, u32, u32)]) -> Result<Vec<Self::Partition>>;
}

pub trait STablePartitionReader {
    type IteratorState: Sized;
    fn decode(&mut self, pos: u32) -> Option<i32>;
    fn next_record(&self, state: &mut Self::IteratorState) -> Option<(u32, u32, i32)>;
    fn iter(&self) -> RecordIterator<Self>;
    fn seek_state(&self, pos: u32) -> Self::IteratorState;
    fn seek_iter(&self, pos: u32) -> RecordIterator<Self> {
        RecordIterator(self, self.seek_state(pos))
    }
}

pub struct RecordIterator<'a, S: STablePartitionReader + ?Sized>(&'a S, S::IteratorState);
impl<'a, S: STablePartitionReader> RecordIterator<'a, S> {
    pub fn into_state(self) -> S::IteratorState {
        self.1
    }
    pub fn new(parent: &'a S, state: S::IteratorState) -> Self {
        Self(parent, state)
    }
}
impl<'a, S: STablePartitionReader> Iterator for RecordIterator<'a, S> {
    type Item = (u32, u32, i32);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_record(&mut self.1)
    }
}

pub use simple_kv::RangeRecord;
pub use simple_kv::SimpleKeyValuePartialReader;
pub use simple_kv::SimpleKeyValuePartialWriter;
pub use simple_kv::SimpleKeyValueReader;
