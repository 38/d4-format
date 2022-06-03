use d4::ptab::DecodeResult;
use d4::ptab::PrimaryTablePartReader;
use d4::ptab::{
    BitArrayDecoder, BitArrayEncoder, BitArrayPartReader, BitArrayPartWriter, PTablePartitionWriter,
};

use d4::ssio::http::HttpReader;
use d4::stab::{
    RangeRecord, RecordIterator, SecondaryTablePartReader, SecondaryTablePartWriter,
    SparseArrayPartReader, SparseArrayPartWriter,
};
use d4::{D4FileWriter, D4TrackReader};

use d4_framefile::Directory;

type RemoteD4Reader = d4::ssio::D4TrackReader<HttpReader>;
type RemoteD4ReaderView = d4::ssio::D4TrackView<HttpReader>;

use std::fs::File;
use std::io::Result;
use std::ops::Range;

type D4Reader = D4TrackReader;
type D4Writer = D4FileWriter;
type D4ReaderParts = (BitArrayPartReader, SparseArrayPartReader<RangeRecord>);

type D4WriterParts = (BitArrayPartWriter, SparseArrayPartWriter<RangeRecord>);

pub enum RootContainer<'a> {
    Local(&'a Directory<File>),
    Remote(&'a Directory<HttpReader>),
}

pub trait StreamReader {
    fn tell(&self) -> Option<(&str, u32)>;
    fn seek(&mut self, name: &str, pos: u32) -> bool;
    fn next_interval(&mut self, same_chrom: bool) -> Option<(Range<u32>, i32)>;
    fn next(&mut self, this_chrom: bool) -> Option<i32>;
    fn root_container(&self) -> RootContainer;
}

pub struct RemoteStreamReader {
    reader: RemoteD4Reader,
    current_view: Option<RemoteD4ReaderView>,
}

impl RemoteStreamReader {
    pub fn new(url: &str) -> Result<Self> {
        let reader = RemoteD4Reader::from_url(url)?;
        Ok(Self {
            reader,
            current_view: None,
        })
    }

    fn find_next_read_pos(&self) -> Option<(usize, u32)> {
        let chr_list = self.reader.chrom_list();
        let begin_idx = if let Some(view) = self.current_view.as_ref() {
            let cur_name = view.chrom_name();
            let mut cur_idx = chr_list
                .iter()
                .enumerate()
                .find(|(_, x)| x.name == cur_name)
                .map(|(idx, _)| idx)
                .unwrap_or(chr_list.len());

            if chr_list.get(cur_idx).map_or(false, |c| {
                view.tell().unwrap_or(c.size as u32) < c.size as u32
            }) {
                cur_idx += 1;
            } else {
                return Some((cur_idx, view.tell().unwrap()));
            }

            cur_idx
        } else {
            0
        };
        chr_list[begin_idx..]
            .iter()
            .enumerate()
            .find(|(_, x)| x.size > 0)
            .map(|(id, _)| (id, 0))
    }
    fn get_current_view(&mut self, load_next: bool) -> Result<Option<&mut RemoteD4ReaderView>> {
        if let Some((chr, pos)) = self.find_next_read_pos() {
            let chr = self.reader.chrom_list()[chr].clone();
            if pos == 0 && chr.name != self.current_view.as_ref().map_or("", |x| x.chrom_name()) {
                if load_next {
                    self.current_view =
                        Some(self.reader.get_view(&chr.name, 0, chr.size as u32)?);
                } else {
                    return Ok(None);
                }
            }
            return Ok(self.current_view.as_mut());
        }
        Ok(None)
    }
}

impl StreamReader for RemoteStreamReader {
    fn tell(&self) -> Option<(&str, u32)> {
        let chr_list = self.reader.chrom_list();
        self.find_next_read_pos()
            .map(|(id, pos)| (chr_list[id].name.as_str(), pos))
    }

    fn seek(&mut self, name: &str, pos: u32) -> bool {
        let mut chr_idx = 0;
        let chr_list = self.reader.chrom_list();
        while chr_idx < chr_list.len() {
            let chr = &chr_list[chr_idx];
            if chr.name == name {
                if pos >= chr.size as u32 {
                    chr_idx += 1;
                }
                break;
            }
        }
        if let Some(chr) = chr_list.get(chr_idx).cloned() {
            if let Ok(view) = self.reader.get_view(&chr.name, pos, chr.size as u32) {
                self.current_view = Some(view);
                return true;
            }
        }
        false
    }

    fn next_interval(&mut self, same_chrom: bool) -> Option<(Range<u32>, i32)> {
        if let Ok(Some(view)) = self.get_current_view(!same_chrom) {
            if let Ok((begin, end, value)) = view.read_next_interval() {
                return Some((begin..end, value));
            }
        }
        None
    }

    fn next(&mut self, this_chrom: bool) -> Option<i32> {
        if let Ok(Some(view)) = self.get_current_view(!this_chrom) {
            if let Some(Ok(next)) = view.next() {
                return Some(next.1);
            }
        }
        None
    }

    fn root_container(&self) -> RootContainer {
        RootContainer::Remote(self.reader.as_root())
    }
}

pub struct LocalStreamReader {
    _inner: D4Reader,
    parts: Vec<D4ReaderParts>,
    current_primary_decoder: Option<BitArrayDecoder>,
    current_part_id: usize,
    current_chr: String,
    current_pos: u32,
    current_stab_iter_state: Option<(usize, usize)>,
}

impl LocalStreamReader {
    pub fn new(mut inner: D4Reader) -> Result<Self> {
        let parts = inner.split(None)?;
        Ok(Self {
            _inner: inner,
            parts,
            current_primary_decoder: None,
            current_chr: "".to_string(),
            current_part_id: 0,
            current_pos: 0,
            current_stab_iter_state: None,
        })
    }
}

impl StreamReader for LocalStreamReader {
    fn tell(&self) -> Option<(&str, u32)> {
        if self.current_part_id >= self.parts.len() {
            return None;
        }
        let current_part = &self.parts[self.current_part_id];

        let (chr, _, end) = current_part.0.region();

        if chr != self.current_chr {
            return Some((chr, 0));
        }

        if end <= self.current_pos {
            if self.current_part_id + 1 >= self.parts.len() {
                return None;
            }
            let (chr, _, _) = self.parts[self.current_part_id + 1].0.region();
            return Some((chr, 0));
        }

        Some((self.current_chr.as_ref(), self.current_pos))
    }

    fn seek(&mut self, name: &str, pos: u32) -> bool {
        if let Some((idx, _)) = self.parts.iter().enumerate().find(|(_, (p, _))| {
            let (chr, _, end) = p.region();
            chr == name && pos < end
        }) {
            self.current_part_id = idx;
            self.current_chr = name.to_string();
            self.current_pos = pos;
            self.current_stab_iter_state = None;
            self.current_primary_decoder = None;
            return true;
        }
        false
    }

    fn next_interval(&mut self, same_chrom: bool) -> Option<(Range<u32>, i32)> {
        if self.current_part_id >= self.parts.len() {
            return None;
        }

        let current_part = &mut self.parts[self.current_part_id];
        let (chr, _, end) = current_part.0.region();

        if chr != self.current_chr {
            if same_chrom {
                return None;
            }
            self.current_pos = 0;
            self.current_chr = chr.to_string();
        }

        if end <= self.current_pos {
            self.current_part_id += 1;
            self.current_primary_decoder = None;
            self.next_interval(same_chrom)
        } else {
            let mut iter = if let Some(state) = self.current_stab_iter_state {
                RecordIterator::new(&current_part.1, state)
            } else {
                current_part.1.seek_iter(self.current_pos)
            };

            let next_interval = iter.next();

            if let Some((l, r, v)) = next_interval {
                if l <= self.current_pos {
                    self.current_pos = r;
                    return Some((l..r, v));
                }
            }

            if self.current_primary_decoder.is_none() {
                self.current_primary_decoder = Some(current_part.0.make_decoder());
            }

            let empty_ptab = current_part.0.bit_width() == 0;
            let mut ret_value = None;
            let mut last_pos = self.current_pos;
            for pos in self.current_pos
                ..end.min(next_interval.as_ref().map_or(end, |&(start, _, _)| start))
            {
                let cur_value = match self
                    .current_primary_decoder
                    .as_mut()
                    .unwrap()
                    .decode(pos as usize)
                {
                    DecodeResult::Definitely(value) => value,
                    DecodeResult::Maybe(value) => value,
                };
                if ret_value.map_or(false, |old| old != cur_value) {
                    break;
                }
                ret_value = Some(cur_value);
                if !empty_ptab {
                    last_pos = pos + 1;
                } else {
                    last_pos = next_interval.as_ref().map_or(end, |&(_, r, _)| r);
                    break;
                }
            }
            if let Some(value) = ret_value {
                let first_pos = self.current_pos;
                self.current_pos = last_pos;
                return Some((first_pos..last_pos, value));
            }
            self.current_pos += 1;
            None
        }
    }

    fn next(&mut self, this_chrom: bool) -> Option<i32> {
        if self.current_part_id >= self.parts.len() {
            return None;
        }
        self.current_stab_iter_state = None;

        let current_part = &mut self.parts[self.current_part_id];

        let (chr, _, end) = current_part.0.region();
        if chr != self.current_chr {
            if this_chrom {
                return None;
            }
            self.current_pos = 0;
            self.current_chr = chr.to_string();
        }
        if end <= self.current_pos {
            self.current_part_id += 1;
            self.current_primary_decoder = None;
            return self.next(this_chrom);
        } else {
            if self.current_primary_decoder.is_none() {
                self.current_primary_decoder = Some(current_part.0.make_decoder());
            }
            let ret = match self
                .current_primary_decoder
                .as_mut()
                .unwrap()
                .decode(self.current_pos as usize)
            {
                DecodeResult::Definitely(value) => Some(value),
                DecodeResult::Maybe(value) => {
                    if let Some(value) = current_part.1.decode(self.current_pos) {
                        Some(value)
                    } else {
                        Some(value)
                    }
                }
            };
            self.current_pos += 1;
            return ret;
        }
    }

    fn root_container(&self) -> RootContainer {
        RootContainer::Local(self._inner.as_root_container())
    }
}

pub struct StreamWriter {
    _inner: D4Writer,
    parts: Vec<D4WriterParts>,
    last_part_pos: Vec<u32>,
    current_primary_encoder: Option<BitArrayEncoder>,
    current_part_id: usize,
    current_chr: String,
    current_pos: u32,
}

impl StreamWriter {
    pub fn new(mut inner: D4Writer) -> Result<Self> {
        let parts = inner.parallel_parts(None)?;
        let np = parts.len();
        Ok(Self {
            _inner: inner,
            parts,
            last_part_pos: vec![0; np],
            current_primary_encoder: None,
            current_part_id: 0,
            current_chr: "".to_string(),
            current_pos: 0,
        })
    }

    pub fn tell(&self) -> Option<(&str, u32)> {
        if self.current_part_id >= self.parts.len() {
            return None;
        }
        let current_part = &self.parts[self.current_part_id];

        let (chr, _, end) = current_part.0.region();

        if chr != self.current_chr {
            return Some((chr, 0));
        }

        if end <= self.current_pos {
            if self.current_part_id + 1 >= self.parts.len() {
                return None;
            }
            let (chr, _, _) = self.parts[self.current_part_id + 1].0.region();
            return Some((chr, 0));
        }

        Some((self.current_chr.as_ref(), self.current_pos))
    }

    pub fn write_value(&mut self, value: i32, same_chrom: bool) -> Result<bool> {
        if self.current_part_id >= self.parts.len() {
            return Ok(false);
        }

        let current_part = &mut self.parts[self.current_part_id];
        let (chr, _, end) = current_part.0.region();

        if chr != self.current_chr {
            if same_chrom {
                return Ok(false);
            }
            self.current_pos = 0;
            self.current_chr = chr.to_string();
        }

        if end <= self.current_pos {
            self.current_part_id += 1;
            self.current_primary_encoder = None;
            self.write_value(value, same_chrom)
        } else {
            if self.current_primary_encoder.is_none() {
                self.current_primary_encoder = Some(current_part.0.to_codec());
            }
            if !self
                .current_primary_encoder
                .as_mut()
                .unwrap()
                .encode(self.current_pos as usize, value)
            {
                let stab = &mut current_part.1;
                stab.encode(self.current_pos, value)?;
                self.current_pos += 1;
                Ok(true)
            } else {
                self.current_pos += 1;
                Ok(true)
            }
        }
    }

    pub fn write_interval(&mut self, left: u32, right: u32, value: i32) -> Result<bool> {
        let (current_part, end) = loop {
            if self.current_part_id >= self.parts.len() {
                return Ok(false);
            }

            let current_part = &mut self.parts[self.current_part_id];
            let (chr, begin, end) = current_part.0.region();

            if chr != self.current_chr {
                return Ok(false);
            }

            if chr == self.current_chr && begin <= left && left < end {
                break (current_part, end);
            }

            self.current_part_id += 1;
            self.current_primary_encoder = None;
        };

        let actual_right = end.min(right);

        let should_iterate = current_part.0.bit_width() != 0;

        if self.current_primary_encoder.is_none() {
            self.current_primary_encoder = Some(current_part.0.to_codec());
        }
        let stab = &mut current_part.1;

        if should_iterate {
            for pos in left..actual_right {
                if !self
                    .current_primary_encoder
                    .as_mut()
                    .unwrap()
                    .encode(pos as usize, value)
                {
                    stab.encode(pos, value)?;
                }
            }
            self.current_pos = actual_right;
        } else {
            stab.encode_record(left, actual_right, value)?;
        }

        if actual_right < right {
            self.write_interval(actual_right, right, value)?;
        }
        Ok(true)
    }

    pub fn flush(&mut self) {
        if self.current_part_id < self.parts.len() {
            let current_part = &mut self.parts[self.current_part_id];
            current_part.1.flush().ok();
        }
    }

    pub fn seek(&mut self, name: &str, pos: u32) -> bool {
        if let Some((idx, _)) = self.parts.iter().enumerate().find(|(_, (p, _))| {
            let (chr, _, end) = p.region();
            chr == name && pos < end
        }) {
            if &self.current_chr == name && self.current_pos > pos {
                return false;
            }
            if self.last_part_pos[idx] > pos {
                return false;
            }
            if self.current_part_id < self.parts.len() {
                self.last_part_pos[self.current_part_id] = self.current_pos;
            }
            self.current_part_id = idx;
            self.current_chr = name.to_string();
            self.current_primary_encoder = None;
            self.current_pos = pos;
            self.last_part_pos[idx] = pos;
            return true;
        }
        false
    }
}
