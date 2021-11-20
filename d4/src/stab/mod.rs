/*!
  The secondary table implementation
*/
use d4_framefile::Directory;
use std::fs::File;
use std::io::Result;

use crate::header::Header;

mod sparse_array;

pub use sparse_array::RangeRecord;

pub(crate) use sparse_array::{
    CompressionMethod, Record, RecordBlockParsingState, SparseArraryMetadata,
};

pub const SECONDARY_TABLE_NAME: &str = ".stab";
pub const SECONDARY_TABLE_METADATA_NAME: &str = ".metadata";

/// Any type that is used to write a secondary table for D4 file
pub trait SecondaryTableWriter: Sized {
    /// The writer type to write a single parallel partition for the secondary table
    type Partition: SecondaryTablePartWriter;
    /// Create the secondary table in the D4 file
    fn create(root: &mut Directory<File>, header: &Header) -> Result<Self>;
    /// Split the secondary table into parallel partitions
    fn split(&mut self, partitions: &[(&str, u32, u32)]) -> Result<Vec<Self::Partition>>;
    /// Enable the secondary table compression
    fn enable_deflate_encoding(&mut self, level: u32) -> &mut Self;
}

/// A type that is used to write a single parallel partition of a secondary table
pub trait SecondaryTablePartWriter: Send {
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
pub trait SecondaryTableReader: Sized {
    /// The type used to read a single parallel partition
    type Partition: SecondaryTablePartReader;
    /// Create a new reader instance
    fn create(root: &mut Directory<File>, header: &Header) -> Result<Self>;
    /// Split the reader into parts
    fn split(&mut self, partitions: &[(&str, u32, u32)]) -> Result<Vec<Self::Partition>>;
}

/// A type that can be used as a partition reader for a secondary table
pub trait SecondaryTablePartReader: Sized {
    /// The type for additional data used for the iterator interface
    type IteratorState: Sized;
    /// Decode the value at given location
    fn decode(&mut self, pos: u32) -> Option<i32>;
    /// Read the next record
    fn next_record(&self, state: &mut Self::IteratorState) -> Option<(u32, u32, i32)>;
    /// Return a iter that iterate over  all the records
    fn iter(&self) -> RecordIterator<'_, Self>;
    /// Build a iterator state starting from the given location
    fn seek_state(&self, pos: u32) -> Self::IteratorState;
    /// Return a iterator that seeks to the given location
    fn seek_iter(&self, pos: u32) -> RecordIterator<'_, Self> {
        RecordIterator(self, self.seek_state(pos))
    }
}

/// The iterator over all the intervals
pub struct RecordIterator<'a, S: SecondaryTablePartReader>(&'a S, S::IteratorState);
impl<'a, S: SecondaryTablePartReader> RecordIterator<'a, S> {
    /// Cast the record into the iterator state
    pub fn into_state(self) -> S::IteratorState {
        self.1
    }
    /// Create new record iterator
    pub fn new(parent: &'a S, state: S::IteratorState) -> Self {
        Self(parent, state)
    }
}
impl<'a, S: SecondaryTablePartReader> Iterator for RecordIterator<'a, S> {
    type Item = (u32, u32, i32);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next_record(&mut self.1)
    }
}

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
pub mod mapped {
    use super::sparse_array;
    pub use sparse_array::RangeRecord;
    pub use sparse_array::SparseArrayPartReader;
    pub use sparse_array::SparseArrayPartWriter;
    pub use sparse_array::SparseArrayReader;
    pub use sparse_array::SparseArrayWriter;
}

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
pub use mapped::*;
