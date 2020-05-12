use crate::header::Header;
use framefile::mode::{ReadOnly, ReadWrite};
use framefile::Directory;
use std::fs::File;
use std::io::Result;

mod uncompressed;

pub enum DecodeResult {
    Definitely(i32),
    Maybe(i32),
}

pub trait PTableWriter: Sized {
    type Partition: PTablePartitionWriter;
    fn create(directory: &mut Directory<'static, ReadWrite, File>, header: &Header)
        -> Result<Self>;
    fn split(&mut self, header: &Header, size_limit: Option<usize>)
        -> Result<Vec<Self::Partition>>;
}

pub trait PTablePartitionWriter: Send {
    type EncoderType: Encoder;
    fn as_encoder(&mut self) -> Self::EncoderType;
    fn region(&self) -> (&str, u32, u32);
}

pub trait Encoder {
    fn encode(&mut self, pos: usize, value: i32) -> bool;
}

pub trait PTableReader: Sized {
    type Partition: PTablePartitionReader;
    fn create(directory: &mut Directory<'static, ReadOnly, File>, header: &Header) -> Result<Self>;
    fn split(&mut self, header: &Header, size_limit: Option<usize>)
        -> Result<Vec<Self::Partition>>;
}

pub trait PTablePartitionReader: Send {
    type DecoderType: Decoder;
    fn as_decoder(&mut self) -> Self::DecoderType;
    fn region(&self) -> (&str, u32, u32);
    fn bit_width(&self) -> usize;
}

pub trait Decoder {
    fn decode(&mut self, pos: usize) -> DecodeResult;
    fn decode_block<F: FnMut(usize, DecodeResult)>(
        &mut self,
        pos: usize,
        count: usize,
        mut handle: F,
    ) {
        for idx in 0..count {
            handle(pos + idx, self.decode(pos + idx));
        }
    }
}

pub type UncompressedWriter = uncompressed::PrimaryTable<uncompressed::Writer>;
pub type UncompressedReader = uncompressed::PrimaryTable<uncompressed::Reader>;
pub type UncompressedPartWriter = uncompressed::PartialPrimaryTable<uncompressed::Writer>;
pub type UncompressedPartReader = uncompressed::PartialPrimaryTable<uncompressed::Reader>;

pub type UncompressedDecoder = uncompressed::PrimaryTableCodec<uncompressed::Reader>;
pub type UncompressedEncoder = uncompressed::PrimaryTableCodec<uncompressed::Writer>;
