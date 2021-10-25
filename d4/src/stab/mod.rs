/*!
  The secondary table implementation
*/
use d4_framefile::mode::{ReadOnly, ReadWrite};
use d4_framefile::Directory;
use std::fs::File;
use std::io::Result;

use crate::header::Header;

mod simple_kv;

pub use simple_kv::RangeRecord;

pub(crate) use simple_kv::{CompressionMethod, Record, RecordBlockParsingState, SimpleKvMetadata};

/// Any type that is used to write a secondary table for D4 file
pub trait STableWriter: Sized {
    /// The writer type to write a single parallel partition for the secondary table
    type Partition: STablePartitionWriter;
    /// Create the secondary table in the D4 file
    fn create(root: &mut Directory<'static, ReadWrite, File>, header: &Header) -> Result<Self>;
    /// Split the secondary table into parallel partitions
    fn split(&mut self, partitions: &[(&str, u32, u32)]) -> Result<Vec<Self::Partition>>;
    /// Enable the secondary table compression
    fn enable_deflate_encoding(&mut self, level: u32) -> &mut Self;
}

/// A type that is used to write a single parallel partition of a secondary table
pub trait STablePartitionWriter: Send {
    /// Encode a single value
    fn encode(&mut self, pos: u32, value: i32) -> Result<()>;
    /// Encode a range with same value
    fn encode_record(&mut self, left: u32, right: u32, value: i32) -> Result<()>;
    /// Flush current cached data
    fn flush(&mut self) -> Result<()>;
    /// Finish the partition writing
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Type usd as a secondary table reader
pub trait STableReader: Sized {
    /// The type used to read a single parallel partition
    type Partition: STablePartitionReader;
    /// Create a new reader instance
    fn create(root: &mut Directory<'static, ReadOnly, File>, header: &Header) -> Result<Self>;
    /// Split the reader into parts
    fn split(&mut self, partitions: &[(&str, u32, u32)]) -> Result<Vec<Self::Partition>>;
}

/// A type that can be used as a partition reader for a secondary table
pub trait STablePartitionReader: Sized {
    /// The type for additional data used for the iterator interface
    type IteratorState: Sized;
    /// Decode the value at given location
    fn decode(&mut self, pos: u32) -> Option<i32>;
    /// Read the next record
    fn next_record(&self, state: &mut Self::IteratorState) -> Option<(u32, u32, i32)>;
    /// Return a iter that iterate over  all the records
    fn iter(&self) -> RecordIterator<Self>;
    /// Build a iterator state starting from the given location
    fn seek_state(&self, pos: u32) -> Self::IteratorState;
    /// Return a iterator that seeks to the given location
    fn seek_iter(&self, pos: u32) -> RecordIterator<Self> {
        RecordIterator(self, self.seek_state(pos))
    }
}

/// The iterator over all the intervals
pub struct RecordIterator<'a, S: STablePartitionReader>(&'a S, S::IteratorState);
impl<'a, S: STablePartitionReader> RecordIterator<'a, S> {
    /// Cast the record into the iterator state
    pub fn into_state(self) -> S::IteratorState {
        self.1
    }
    /// Create new record iterator
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

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
pub mod mapped {
    use super::simple_kv;
    pub use simple_kv::RangeRecord;
    pub use simple_kv::SimpleKeyValuePartialReader;
    pub use simple_kv::SimpleKeyValuePartialWriter;
    pub use simple_kv::SimpleKeyValueReader;
    pub use simple_kv::SimpleKeyValueWriter;
}

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
pub use mapped::*;
