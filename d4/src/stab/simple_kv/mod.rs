mod compression;
mod reader;
mod record;
mod record_block;
mod writer;

use serde_derive::{Deserialize, Serialize};

/// The metadata of the Key Value table
#[derive(Serialize, Deserialize)]
struct SimpleKVMetadata {
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

struct StreamInfo {
    id: String,
    chr: String,
    #[allow(dead_code)]
    range: (u32, u32),
}

impl SimpleKVMetadata {
    fn streams(&self) -> impl Iterator<Item = StreamInfo> {
        self.partitions
            .clone()
            .into_iter()
            .enumerate()
            .map(|(idx, (chr, begin, end))| StreamInfo {
                id: format!("{}", idx),
                chr: chr.clone(),
                range: (begin, end),
            })
    }
}

pub use compression::CompressionMethod;
pub use reader::{SimpleKeyValuePartialReader, SimpleKeyValueReader};
pub use record::{RangeRecord, Record};
pub use writer::{SimpleKeyValuePartialWriter, SimpleKeyValueWriter};
