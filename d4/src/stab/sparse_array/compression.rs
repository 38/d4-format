use super::record::Record;
use d4_framefile::Stream;
use flate2::{write::DeflateEncoder, Compression};
use serde_derive::{Deserialize, Serialize};

use std::fs::File;
use std::io::{Result, Write};
use std::marker::PhantomData;

/// The flag that indicates what compression method we are using
#[derive(Serialize, Deserialize, Copy, Clone)]
pub enum CompressionMethod {
    NoCompression,
    Deflate(u32),
}

/// A context that is used to compress the record blocks
pub(super) enum CompressionContext<R: Record> {
    NoCompression,
    Deflate {
        first_block: bool,
        buffer: Vec<u8>,
        compressor: DeflateEncoder<Vec<u8>>,
        unused_buffer: Option<Vec<u8>>,
        first_pos: Option<u32>,
        last_pos: Option<u32>,
        count: u32,
        _phantom: PhantomData<R>,
    },
}

impl Default for CompressionMethod {
    fn default() -> Self {
        Self::NoCompression
    }
}

impl CompressionMethod {
    /// Create a new context from the compression method
    pub(super) fn context<R: Record>(&self) -> CompressionContext<R> {
        match self {
            Self::NoCompression => CompressionContext::NoCompression,
            Self::Deflate(level) => {
                let compressor = DeflateEncoder::new(Vec::new(), Compression::new(*level));
                CompressionContext::Deflate {
                    _phantom: PhantomData,
                    first_block: true,
                    buffer: vec![],
                    compressor,
                    unused_buffer: Some(vec![]),
                    last_pos: None,
                    first_pos: None,
                    count: 0,
                }
            }
        }
    }
}

impl<R: Record> CompressionContext<R> {
    /// Append a new record to the compression context
    pub(super) fn append_record(
        &mut self,
        record: Option<&R>,
        stream: &mut Stream<File>,
    ) -> Result<()> {
        match self {
            Self::NoCompression => {
                if let Some(record) = record {
                    let buffer = record.as_bytes();
                    stream.write_with_alloc_callback(buffer, |s| {
                        s.disable_pre_alloc();
                        s.double_frame_size(2 * 1024 * 1024);
                    })?;
                }
            }
            Self::Deflate {
                first_block,
                buffer,
                compressor,
                unused_buffer,
                last_pos,
                count,
                first_pos,
                ..
            } => {
                let size_limit = if *first_block {
                    stream.get_frame_capacity() - 1
                } else {
                    65536
                };
                if buffer.len() + R::SIZE >= size_limit || record.is_none() {
                    if *first_block {
                        // By default we put a leading 0 to the first block, indicating this block is compressed
                        compressor.get_mut().push(0);
                    }
                    compressor
                        .get_mut()
                        .write_all(&first_pos.unwrap_or(0).to_le_bytes())
                        .unwrap();
                    compressor
                        .get_mut()
                        .write_all(&last_pos.unwrap_or(0).to_le_bytes())
                        .unwrap();
                    compressor
                        .get_mut()
                        .write_all(&count.to_le_bytes())
                        .unwrap();
                    compressor.write_all(buffer).unwrap();
                    let next_buffer = unused_buffer.take().unwrap();
                    let mut result = compressor.reset(next_buffer).unwrap();
                    // We handle the first block differently, since for each stream the first block should be
                    // pre-allocated, which means we can't use variant-length block at this point.
                    // Even though the size of the compressed data usually smaller than the raw data, but this
                    // isn't always true. Thus, we should check if this is the case, if the compressed data is lager than
                    // the raw data, we store the raw data instead of compressed one. By doing so we can guarantee that
                    // the first block always have a valid size which is no more than the preallocated space in the file.
                    if *first_block && result.len() > size_limit {
                        result.resize(1 + size_limit, 0);
                        result[1..].copy_from_slice(buffer);
                        // At this point, we just modify the flag byte from 0 to 1, which indicates the data isn't compressed
                        result[0] = 1;
                    }
                    if *first_block {
                        // If this is the first block, it's guaranteed size is smaller than the first frame
                        stream.write(result.as_ref())?;
                    } else {
                        // Otherwise, we force to write an entire frame
                        stream.write_frame(result.as_ref())?;
                    }

                    *first_block = false;
                    result.clear();
                    *unused_buffer = Some(result);
                    buffer.clear();
                    *count = 0;
                    *last_pos = None;
                    *first_pos = None;
                }
                if let Some(record) = record {
                    buffer.write_all(record.as_bytes())?;
                    *first_pos = Some(first_pos.unwrap_or(record.effective_range().0));
                    *last_pos = Some(record.effective_range().1);
                    *count += 1;
                }
            }
        }
        Ok(())
    }
}
