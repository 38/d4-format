/// # The Seek-able Stream Input and Output
/// Currently normal local D4 file reading is heavily relies on mapped IO
/// This gives D4 amazing performance, however, when we want to deal with
/// some other types of data sources rather than the local file system,
/// we need a reader that is purely written with random IO APIs with seek and
/// read.
/// This module is used in this purpose
use std::{
    collections::VecDeque,
    io::{Error, ErrorKind, Read, Result, Seek},
};

use crate::{
    d4file::validate_header,
    ptab::PRIMARY_TABLE_NAME,
    stab::{
        CompressionMethod, RangeRecord, Record, RecordBlockParsingState, SparseArraryMetadata,
        SECONDARY_TABLE_METADATA_NAME, SECONDARY_TABLE_NAME,
    },
    Chrom, Dictionary, Header,
};
use d4_framefile::{Blob, Directory, OpenResult, Stream};

#[cfg(feature = "http_reader")]
pub mod http;

struct DataStreamEntry<R: Read + Seek>(Directory<R>, String);

impl<R: Read + Seek> Clone for DataStreamEntry<R> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1.clone())
    }
}

impl<R: Read + Seek> DataStreamEntry<R> {
    fn get_stream(&mut self) -> Result<Stream<R>> {
        let DataStreamEntry(dir, name) = self;
        Ok(dir.open_stream(name)?)
    }
}

struct SecondaryTableStream<R: Read + Seek> {
    chrom_id: usize,
    begin: u32,
    end: u32,
    data_stream: DataStreamEntry<R>,
}

impl<R: Read + Seek> Clone for SecondaryTableStream<R> {
    fn clone(&self) -> Self {
        Self {
            data_stream: self.data_stream.clone(),
            ..*self
        }
    }
}

pub struct D4TrackReader<R: Read + Seek> {
    header: Header,
    primary_table: Blob<R>,
    compression: CompressionMethod,
    secondary_table: Vec<SecondaryTableStream<R>>,
}

pub struct D4TrackView<R: Read + Seek> {
    chrom: String,
    end: u32,
    cursor: u32,
    primary_table: Blob<R>,
    primary_table_buffer: Option<(u32, Vec<u8>)>,
    secondary_tables: VecDeque<SecondaryTableStream<R>>,
    stream: Option<Stream<R>>,
    rbp_state: RecordBlockParsingState<RangeRecord>,
    frame_decode_result: VecDeque<RangeRecord>,
    current_record: Option<RangeRecord>,
    dictionary: Dictionary,
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

    fn load_next_secondary_record(&mut self) -> Option<&RangeRecord> {
        if let Some(rec) = self.frame_decode_result.pop_front() {
            self.current_record = Some(rec);
            return self.current_record.as_ref();
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

        if let Some(mut stream) = self.secondary_tables.pop_front() {
            self.stream = Some(stream.data_stream.get_stream().unwrap());
            self.rbp_state.reset();
            return self.load_next_secondary_record();
        }
        None
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
                self.load_next_secondary_record();
            }
            while let Some(record) = self.current_record.as_ref() {
                let (begin, end) = record.effective_range();
                if end > pos || begin >= pos {
                    break;
                }
                if self.load_next_secondary_record().is_none() {
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

impl<R: Read + Seek> D4TrackReader<R> {
    pub fn chrom_list(&self) -> &[Chrom] {
        self.header.chrom_list()
    }
    pub fn get_view(&mut self, chrom: &str, begin: u32, end: u32) -> Result<D4TrackView<R>> {
        let primary_offset = self.header.primary_table_offset_of_chrom(chrom);
        let primary_size = self.header.primary_table_size_of_chrom(chrom);
        if primary_size == 0 {
            return Err(Error::new(ErrorKind::Other, "chrom name not found"));
        }

        let primary_view = self
            .primary_table
            .get_view(primary_offset as u64, primary_size);
        let mut secondary_view = Vec::new();
        for ss_info in self.secondary_table.iter() {
            if self.header.chrom_list()[ss_info.chrom_id].name == chrom
                && ss_info.begin.max(begin) < ss_info.end.min(end)
            {
                secondary_view.push(ss_info.clone());
            }
        }
        Ok(D4TrackView {
            primary_table: primary_view,
            secondary_tables: secondary_view.into(),
            chrom: chrom.to_string(),
            end,
            cursor: begin,
            primary_table_buffer: None,
            dictionary: self.header.dictionary().clone(),
            stream: None,
            current_record: None,
            rbp_state: RecordBlockParsingState::new(self.compression),
            frame_decode_result: Default::default(),
        })
    }
    pub fn from_reader(mut reader: R, track_name: Option<&str>) -> Result<Self> {
        validate_header(&mut reader)?;
        let file_root = Directory::open_root(reader, 8)?;
        let track_root = if let Some(track_name) = track_name {
            match file_root.open(track_name)? {
                OpenResult::SubDir(root) => root,
                _ => {
                    return Err(Error::new(
                        std::io::ErrorKind::Other,
                        "track root not found",
                    ));
                }
            }
        } else {
            file_root
        };

        let header_stream = track_root.open_stream(Header::HEADER_STREAM_NAME)?;
        let header = Header::read(header_stream)?;
        let primary_table = track_root.open_blob(PRIMARY_TABLE_NAME)?;

        let chrom_list = header.chrom_list();

        let (secondary_table, compression) = {
            let root = track_root.open_directory(SECONDARY_TABLE_NAME)?;
            let metadata: SparseArraryMetadata = {
                let mut stream = root.open_stream(SECONDARY_TABLE_METADATA_NAME)?;
                let mut buf = vec![];
                loop {
                    let mut buffer = [0; 4096];
                    let mut sz = stream.read(&mut buffer)?;
                    if sz == 0 {
                        break;
                    }
                    while sz > 0 && buffer[sz - 1] == 0 {
                        sz -= 1;
                    }
                    buf.extend_from_slice(&buffer[..sz]);
                }
                serde_json::from_reader(&buf[..])?
            };
            let mut buf = vec![];
            for stream in metadata.streams() {
                if let Some((chrom_id, _)) = chrom_list
                    .iter()
                    .enumerate()
                    .find(|(_, c)| c.name == stream.chr)
                {
                    let stream_name = stream.id.as_str();
                    let stream = SecondaryTableStream {
                        chrom_id,
                        begin: stream.range.0,
                        end: stream.range.1,
                        data_stream: DataStreamEntry(root.clone(), stream_name.to_string()),
                    };
                    buf.push(stream);
                }
            }
            (buf, metadata.compression())
        };

        Ok(D4TrackReader {
            header,
            primary_table,
            compression,
            secondary_table,
        })
    }
}

#[test]
fn test_open_hg002() {
    //use std::fs::File;
    env_logger::init();
    let hg002_file =
        http::HttpReader::new("https://home.chpc.utah.edu/~u0875014/hg002.d4").unwrap();
    let mut reader = D4TrackReader::from_reader(hg002_file, None).unwrap();
    let view = reader.get_view("1", 0, 20000).unwrap();
    for read_result in view {
        let (_pos, _value) = read_result.unwrap();
    }
}
