use std::{
    collections::VecDeque,
    io::{Read, Result, Seek},
};

use crate::{
    stab::{RangeRecord, Record, RecordBlockParsingState},
    Dictionary,
};

use d4_framefile::{Blob, Stream};

use super::table::SecondaryTableRef;

pub struct D4TrackView<R: Read + Seek> {
    pub(super) chrom: String,
    pub(super) end: u32,
    pub(super) cursor: u32,
    pub(super) primary_table: Blob<R>,
    pub(super) primary_table_buffer: Option<(u32, Vec<u8>)>,
    pub(super) secondary_tables: VecDeque<SecondaryTableRef<R>>,
    pub(super) stream: Option<Stream<R>>,
    pub(super) rbp_state: RecordBlockParsingState<RangeRecord>,
    pub(super) frame_decode_result: VecDeque<RangeRecord>,
    pub(super) current_record: Option<RangeRecord>,
    pub(super) dictionary: Dictionary,
}

impl<'a, R: Read + Seek + 'a> Iterator for D4TrackView<R> {
    type Item = Result<(u32, i32)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor >= self.end {
            None
        } else {
            Some(self.read_next_value())
        }
    }
}

impl<R: Read + Seek> D4TrackView<R> {
    pub fn chrom_name(&self) -> &str {
        self.chrom.as_ref()
    }

    fn ensure_primary_table_buffer(&mut self) -> Result<()> {
        if self
            .primary_table_buffer
            .as_ref()
            .map_or(true, |(start, buf)| {
                let offset = self.cursor - start;
                let byte_offset = (offset as usize * self.dictionary.bit_width()) / 8;
                byte_offset >= buf.len()
            })
        {
            let start_pos = self.cursor - self.cursor % 8;
            let start_byte = start_pos as usize * self.dictionary.bit_width() / 8;
            let end_pos = start_pos as usize
                + (4096 * 8 / self.dictionary.bit_width()).min((self.end - start_pos) as usize);
            let end_byte = (end_pos * self.dictionary.bit_width() + 7) / 8;
            let size = end_byte - start_byte;
            let mut buf = vec![0; size];
            self.primary_table
                .read_block(start_byte as u64, &mut buf[..])?;
            self.primary_table_buffer = Some((start_pos, buf));
        }
        Ok(())
    }

    fn load_next_secondary_record(&mut self) -> Result<Option<&RangeRecord>> {
        if let Some(rec) = self.frame_decode_result.pop_front() {
            self.current_record = Some(rec);
            return Ok(self.current_record.as_ref());
        }

        if let Some(stream) = self.stream.as_mut() {
            if let Some(frame_data) = stream.read_current_frame() {
                let mut blocks = vec![];
                self.rbp_state.parse_frame(frame_data, &mut blocks);
                for block in blocks {
                    for &rec in block.as_ref() {
                        self.frame_decode_result.push_back(rec);
                    }
                }
                return self.load_next_secondary_record();
            }
        }

        if let Some(stream) = self.secondary_tables.pop_front() {
            self.stream = Some(stream.open_stream()?);
            self.rbp_state = stream.get_frame_parsing_state();
            return self.load_next_secondary_record();
        }
        Ok(None)
    }
    pub fn read_next_value(&mut self) -> Result<(u32, i32)> {
        let pos = self.cursor;
        self.ensure_primary_table_buffer()?;
        let (start_pos, buf) = self.primary_table_buffer.as_ref().unwrap();
        let bit_idx = (self.cursor - *start_pos) as usize * self.dictionary.bit_width();
        let idx = bit_idx / 8;
        let shift = bit_idx % 8;
        let data: &u32 = unsafe { std::mem::transmute(&buf[idx]) };
        let data = (*data >> shift) & ((1 << self.dictionary.bit_width()) - 1);
        self.cursor += 1;

        if data != (1 << self.dictionary.bit_width()) - 1 {
            return Ok((pos, self.dictionary.decode_value(data).unwrap_or(0)));
        } else {
            let fallback_value = self.dictionary.decode_value(data).unwrap_or(0);
            if self.current_record.is_none() {
                self.load_next_secondary_record()?;
            }
            while let Some(record) = self.current_record.as_ref() {
                if !record.is_valid() {
                    continue;
                }
                let (begin, end) = record.effective_range();
                if end > pos || begin >= pos {
                    break;
                }
                if self.load_next_secondary_record()?.is_none() {
                    break;
                }
            }
            if let Some(rec) = self.current_record {
                let (begin, end) = rec.effective_range();
                if begin <= pos && pos < end {
                    return Ok((pos, rec.value()));
                }
            }
            Ok((pos, fallback_value))
        }
    }
}
