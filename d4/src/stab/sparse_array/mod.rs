mod compression;
mod reader;
mod record;
mod record_block;

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
mod writer;

use serde_derive::{Deserialize, Serialize};

/// The metadata of the Key Value table
#[derive(Serialize, Deserialize)]
pub(crate) struct SparseArraryMetadata {
    /// The format identifier
    format: String,
    /// The record format identifier
    record_format: String,
    /// The list of partitions represented by (chromId, start, end)
    partitions: Vec<(String, u32, u32)>,
    /// The method that used for compression
    #[serde(default)]
    compression: CompressionMethod,
}

pub(crate) struct StreamInfo {
    pub(crate) id: String,
    pub(crate) chr: String,
    #[allow(dead_code)]
    pub(crate) range: (u32, u32),
}

impl SparseArraryMetadata {
    pub(crate) fn compression(&self) -> CompressionMethod {
        self.compression
    }
    pub(crate) fn streams(&self) -> impl Iterator<Item = StreamInfo> {
        self.partitions
            .clone()
            .into_iter()
            .enumerate()
            .map(|(idx, (chr, begin, end))| StreamInfo {
                id: format!("{}", idx),
                chr,
                range: (begin, end),
            })
    }
}

pub use compression::CompressionMethod;
#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
pub use reader::{SparseArrayPartReader, SparseArrayReader};
#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
pub use writer::{SparseArrayPartWriter, SparseArrayWriter};

pub(crate) use reader::RecordBlockParsingState;
pub use record::{RangeRecord, Record};
