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
    pub(super) fetch_size: usize,
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
        if self.dictionary.bit_width() == 0 {
            return Ok(());
        }
        #[allow(clippy::blocks_in_conditions)]
        if self
            .primary_table_buffer
            .as_ref()
            .map_or(true, |(start, buf)| {
                let offset = self.cursor - start;
                let byte_offset = (offset as usize * self.dictionary.bit_width()) / 8;
                byte_offset + 4 >= buf.len() - 4
            })
        {
            let start_pos = self.cursor - self.cursor % 8;
            let start_byte = start_pos as usize * self.dictionary.bit_width() / 8;
            let end_pos = start_pos as usize
                + (self.fetch_size * 8 / self.dictionary.bit_width())
                    .min((self.end - start_pos) as usize);
            self.fetch_size = (self.fetch_size * 2)
                .min(self.primary_table.size())
                .min(1024 * 1024);
            let end_byte = (end_pos * self.dictionary.bit_width() + 7) / 8;
            let size = end_byte - start_byte;
            let mut buf = vec![0; size + 4];
            let mut buf_cursor = 0;
            if let Some((prev_start, prev_buf)) = self.primary_table_buffer.as_ref() {
                let prev_start =
                    (prev_start - prev_start % 8) as usize * self.dictionary.bit_width() / 8;
                let prev_end = prev_start + prev_buf.len() - 4;
                let overlap_start = prev_start.max(start_byte);
                let overlap_end = prev_end.min(end_byte);
                if overlap_start == start_byte && overlap_start < overlap_end {
                    buf[..overlap_end - overlap_start].copy_from_slice(
                        &prev_buf[overlap_start - prev_start..overlap_end - prev_start],
                    );
                    buf_cursor = overlap_end - overlap_start;
                }
            }
            self.primary_table
                .read_block((start_byte + buf_cursor) as u64, &mut buf[buf_cursor..size])?;
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
                        if !rec.is_valid() {
                            break;
                        }
                        self.frame_decode_result.push_back(rec);
                    }
                }
                stream.load_next_frame()?;
                return self.load_next_secondary_record();
            }
        }

        if let Some(stream) = self.secondary_tables.pop_front() {
            self.stream = Some(stream.open_stream()?);
            self.rbp_state = stream.get_frame_parsing_state();
            return self.load_next_secondary_record();
        }
        self.current_record = None;
        Ok(None)
    }
    pub fn tell(&self) -> Option<u32> {
        if self.cursor >= self.end {
            None
        } else {
            Some(self.cursor)
        }
    }

    pub fn read_next_interval(&mut self) -> Result<(u32, u32, i32)> {
        if self.dictionary.bit_width() > 0 {
            let (pos, val) = self.read_next_value()?;
            return Ok((pos, pos + 1, val));
        }

        let begin_pos = self.cursor;

        let fallback_value = self.dictionary.decode_value(0).unwrap_or(0);

        self.update_current_secrec()?;

        if let Some(current_rec) = self.current_record {
            let (cur_rec_beg, cur_rec_end) = current_rec.effective_range();
            if begin_pos < cur_rec_beg {
                self.cursor = begin_pos;
                return Ok((begin_pos, cur_rec_beg, current_rec.value()));
            } else if begin_pos < cur_rec_end {
                self.cursor = cur_rec_end;
                return Ok((begin_pos, cur_rec_end, current_rec.value()));
            } else {
                unreachable!("Buggy update_current_secrec implementation!")
            }
        }

        Ok((begin_pos, begin_pos + 1, fallback_value))
    }

    fn update_current_secrec(&mut self) -> Result<()> {
        let pos = self.cursor;

        if self.current_record.is_none() {
            self.load_next_secondary_record()?;
        }
        while let Some(record) = self.current_record.as_ref() {
            let (begin, end) = record.effective_range();
            if pos < end || pos <= begin {
                break;
            }
            if self.load_next_secondary_record()?.is_none() {
                break;
            }
        }

        Ok(())
    }

    pub fn read_next_value(&mut self) -> Result<(u32, i32)> {
        let pos = self.cursor;
        self.ensure_primary_table_buffer()?;
        let data = if let Some((start_pos, buf)) = self.primary_table_buffer.as_ref() {
            let bit_idx = (self.cursor - *start_pos) as usize * self.dictionary.bit_width();
            let idx = bit_idx / 8;
            let shift = bit_idx % 8;
            let data: &u32 = unsafe { std::mem::transmute(&buf[idx]) };
            (*data >> shift) & ((1 << self.dictionary.bit_width()) - 1)
        } else {
            0
        };

        if data != (1 << self.dictionary.bit_width()) - 1 {
            self.cursor += 1;
            Ok((pos, self.dictionary.decode_value(data).unwrap_or(0)))
        } else {
            self.update_current_secrec()?;

            self.cursor += 1;

            let fallback_value = self.dictionary.decode_value(data).unwrap_or(0);

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
