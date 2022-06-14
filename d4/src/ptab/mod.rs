//! The primary table implementaion
//! The default primary table implementation is bit array, which uses K-bits integer represents an entity in the
//! primary table.

use crate::header::Header;
use d4_framefile::Directory;
use std::fs::File;
use std::io::Result;

mod bit_array;

/// The name of the primary table blob under the root container
pub const PRIMARY_TABLE_NAME: &str = ".ptab";

/// The result of decoding a value from a primary table
pub enum DecodeResult {
    /// Value at this location is definitely the value returned
    Definitely(i32),
    /// The value may be the value return, but the query to the secondary table is required
    Maybe(i32),
}

/// The trait that is used to write a primary table
pub trait PrimaryTableWriter: Sized {
    /// The writer type for a parallel chunk
    type Partition: PTablePartitionWriter;
    /// Create the primary table in the file
    fn create(directory: &mut Directory<File>, header: &Header) -> Result<Self>;
    /// Split the primary table into parallel partitions
    fn split(&mut self, header: &Header, size_limit: Option<usize>)
        -> Result<Vec<Self::Partition>>;
}

/// The trait that is used as one of the parallel partition split from the primary table writer
pub trait PTablePartitionWriter: Send {
    /// The type describes how we encode
    type EncoderType: Encoder;
    /// Create encoder for current partition
    fn make_encoder(&mut self) -> Self::EncoderType;
    /// Report the genome range this partition is responsible to
    fn region(&self) -> (&str, u32, u32);
    /// Report if the primary table can encode the value
    fn can_encode(&self, value: i32) -> bool;
    /// Report the bit width for this primary table
    fn bit_width(&self) -> usize;
}

/// Any type used to encode a value in primary table
pub trait Encoder {
    /// Encode a value at given location
    fn encode(&mut self, pos: usize, value: i32) -> bool;
}

/// Type that reads a D4 primary table
pub trait PrimaryTableReader: Sized {
    /// The type for parallel reading one of the partition
    type Partition: PrimaryTablePartReader + Send;
    /// Create the reader instance
    fn create(directory: &mut Directory<File>, header: &Header) -> Result<Self>;
    /// Split the reader to parallel chunks
    fn split(&mut self, header: &Header, size_limit: Option<usize>)
        -> Result<Vec<Self::Partition>>;
}

/// The type that decodes one part of the primary table in parallel
pub trait PrimaryTablePartReader: Send {
    /// The decoder type
    type DecoderType: Decoder;
    /// Create decoder for current chunk
    fn make_decoder(&mut self) -> Self::DecoderType;
    /// Report the responsible region
    fn region(&self) -> (&str, u32, u32);
    /// Report the bit width
    fn bit_width(&self) -> usize;
    /// The default value of the primary table.
    /// Return None if this is not a 0-sized primary table or the only value defined in this table
    fn default_value(&self) -> Option<i32>;
}

/// Logically this is not needed, as it's just FnMut.
/// Unfortunately, today's Rust doesn't allow us to put
/// inline directive for closure. Thus that makes the code
/// outlines the handler for some case.
/// We need to find a place to put inline directive, and this
/// is the reason why we have this function here.
pub trait DecodeBlockHandle {
    /// Handles a decoded value. `pos` is the locus of the value and `result` carries the data
    fn handle(&mut self, pos: usize, result: DecodeResult);
}

impl<F: FnMut(usize, DecodeResult)> DecodeBlockHandle for F {
    fn handle(&mut self, pos: usize, result: DecodeResult) {
        self(pos, result)
    }
}

/// Any type that used for decoding the primary table
pub trait Decoder {
    /// Decode the value at one location
    fn decode(&mut self, pos: usize) -> DecodeResult;
    /// Decode a block of values - this is just a default implementation
    fn decode_block<F: DecodeBlockHandle>(&mut self, pos: usize, count: usize, mut handle: F) {
        for idx in 0..count {
            handle.handle(pos + idx, self.decode(pos + idx));
        }
    }
}

/// The writer for bit-array backed primary table
pub type BitArrayWriter = bit_array::PrimaryTable<bit_array::Writer>;
/// The reader for bit-array backed primary table
pub type BitArrayReader = bit_array::PrimaryTable<bit_array::Reader>;
/// The partition writer for bit-array backed primary table
pub type BitArrayPartWriter = bit_array::PartialPrimaryTable<bit_array::Writer>;
/// The partition reader for bit-array backed primary table
pub type BitArrayPartReader = bit_array::PartialPrimaryTable<bit_array::Reader>;

/// The decoder for bit-array primary table
pub type BitArrayDecoder = bit_array::PrimaryTableCodec<bit_array::Reader>;
/// The encoder for bit-array primary table
pub type BitArrayEncoder = bit_array::PrimaryTableCodec<bit_array::Writer>;

pub use bit_array::MatrixDecoder;
