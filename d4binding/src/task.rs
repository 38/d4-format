use d4::ptab::{
    BitArrayDecoder, BitArrayPartReader, BitArrayReader, DecodeResult, PrimaryTablePartReader,
};
use d4::stab::{RangeRecord, SecondaryTablePartReader, SparseArrayPartReader, SparseArrayReader};
use d4::D4TrackReader;

use std::io::Result;

type FileReader = D4TrackReader<BitArrayReader, SparseArrayReader<RangeRecord>>;
type ReaderTaskContext = (BitArrayPartReader, SparseArrayPartReader<RangeRecord>);

pub enum TaskHandle {
    Read {
        context: ReaderTaskContext,
        encoder: Option<BitArrayDecoder>,
    },
}

impl TaskHandle {
    pub fn from_reader(reader: &mut FileReader, size_limit: u32) -> Result<Vec<Self>> {
        let size_limit = if size_limit == 0 {
            None
        } else {
            Some(size_limit as usize)
        };
        let parts = reader.split(size_limit)?;
        Ok(parts
            .into_iter()
            .map(|(p, s)| TaskHandle::Read {
                context: (p, s),
                encoder: None,
            })
            .collect())
    }

    #[inline(always)]
    pub fn read(&mut self, pos: u32) -> Option<i32> {
        let (stab, dec) = match self {
            TaskHandle::Read { context, encoder } => {
                if encoder.is_none() {
                    *encoder = Some(context.0.make_decoder());
                }
                (&mut context.1, encoder.as_mut().unwrap())
            }
        };
        let value = match dec.decode(pos as usize) {
            DecodeResult::Definitely(value) => value,
            DecodeResult::Maybe(value) => stab.decode(pos).unwrap_or(value),
        };
        Some(value)
    }

    pub fn range(&self) -> (&str, u32, u32) {
        match self {
            TaskHandle::Read {
                context: (p, _), ..
            } => p.region(),
        }
    }
}
