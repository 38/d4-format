use d4_framefile::mode::ReadWrite;
use d4_framefile::{Directory, Stream};

use crate::stab::{STablePartitionWriter, STableWriter};
use crate::Header;

use super::compression::{CompressionContext, CompressionMethod};
use super::record::Record;
use super::SimpleKVMetadata;

use std::fs::File;
use std::io::Result;
use std::marker::PhantomData;

/// The writer type for the simple sparse array based secondary table
pub struct SimpleKeyValueWriter<R: Record>(
    Directory<'static, ReadWrite, File>,
    CompressionMethod,
    PhantomData<R>,
);

/// The partial writer type for the simple sparse array based secondary table
pub struct SimpleKeyValuePartialWriter<R: Record> {
    stream: Stream<'static, ReadWrite, File>,
    pending_record: Option<R>,
    compression: CompressionContext<R>,
}

impl<R: Record> STableWriter for SimpleKeyValueWriter<R> {
    type Partition = SimpleKeyValuePartialWriter<R>;
    fn enable_deflate_encoding(&mut self, level: u32) -> &mut Self {
        self.1 = CompressionMethod::Deflate(level);
        self
    }
    fn create(root: &mut Directory<'static, ReadWrite, File>, _header: &Header) -> Result<Self> {
        Ok(SimpleKeyValueWriter(
            root.new_stream_cluster(".stab")?,
            Default::default(),
            PhantomData,
        ))
    }
    fn split(
        &mut self,
        partitions: &[(&str, u32, u32)],
    ) -> Result<Vec<SimpleKeyValuePartialWriter<R>>> {
        let metadata = SimpleKVMetadata {
            format: "SimpleKV".to_string(),
            record_format: R::FORMAT_NAME.to_string(),
            partitions: {
                partitions
                    .iter()
                    .map(|(chr, start, end)| (chr.to_string(), *start, *end))
                    .collect()
            },
            compression: self.1,
        };
        let mut metadata_stream = self.0.new_variant_length_stream(".metadata", 512)?;
        metadata_stream.write(serde_json::to_string(&metadata).unwrap().as_bytes())?;
        let compression = self.1;
        Ok(partitions
            .iter()
            .enumerate()
            .map(|(idx, _)| SimpleKeyValuePartialWriter {
                stream: self
                    .0
                    .new_variant_length_stream(format!("{}", idx).as_ref(), 512)
                    .unwrap(),
                pending_record: None,
                compression: compression.context(),
            })
            .collect())
    }
}

impl<R: Record> STablePartitionWriter for SimpleKeyValuePartialWriter<R> {
    #[inline(always)]
    fn flush(&mut self) -> Result<()> {
        if let Some(record) = self.pending_record {
            self.compression
                .append_record(Some(&record), &mut self.stream)?;
            self.pending_record = None;
        }
        Ok(())
    }

    fn encode_record(&mut self, left: u32, right: u32, value: i32) -> Result<()> {
        self.flush()?;
        R::encode_range(left, right, value, |record| {
            self.compression
                .append_record(Some(&record), &mut self.stream)
        })
    }

    fn encode(&mut self, pos: u32, value: i32) -> Result<()> {
        if let Some(new_pending) = R::encode(self.pending_record.as_mut(), pos, value) {
            self.flush()?;
            self.pending_record = Some(new_pending);
        }
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.compression.append_record(None, &mut self.stream)
    }
}
