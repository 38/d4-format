use std::{
    io::{Error, Read, Result, Seek},
    sync::Arc,
};

use crate::{
    d4file::validate_header,
    stab::{CompressionMethod, SimpleKvMetadata},
    Dictionary, Header,
};
use d4_framefile::{mode::ReadOnly, Blob, Directory, OpenResult, Stream};

#[cfg(feature = "http_reader")]
pub mod http;

#[derive(Clone)]
struct DataStreamEntry<'a, R: Read + Seek + 'a>(Directory<'a, ReadOnly, R>, String);

impl<'a, R: Read + Seek + 'a> DataStreamEntry<'a, R> {
    fn get_stream(&mut self) -> Result<Stream<'a, ReadOnly, R>> {
        let DataStreamEntry(dir, name) = self;
        Ok(dir.open_stream(name)?)
    }
}

#[derive(Clone)]
struct SecondaryTableStream<'a, R: Read + Seek + 'a> {
    chrom_id: usize,
    begin: u32,
    end: u32,
    data_stream: DataStreamEntry<'a, R>,
}

pub struct D4TrackReader<'a, R: Read + Seek + 'a> {
    header: Header,
    primary_table: Blob<'a, ReadOnly, R>,
    compressed_stab: bool,
    secondary_table: Vec<SecondaryTableStream<'a, R>>,
}

pub struct D4TrackView<'a, R: Read + Seek + 'a> {
    chrom: &'a str,
    begin: u32,
    end: u32,
    cursor: u32,
    primary_table: Blob<'a, ReadOnly, R>,
    primary_table_buffer: Option<(u32, Vec<u8>)>,
    secondary_tables: Vec<Stream<'a, ReadOnly, R>>,
    dictionary: Dictionary,
}

impl<'a, R: Read + Seek + 'a> D4TrackView<'a, R> {
    fn ensure_primary_table_buffer(&mut self) -> Result<()> {
        if self
            .primary_table_buffer
            .as_ref()
            .map_or(true, |(start, buf)| {
                let offset = self.cursor - start;
                let byte_offset = (offset as usize * self.dictionary.bit_width()) / 8;
                byte_offset < buf.len()
            })
        {
            let start_pos = self.cursor - self.cursor % 8;
            let start_byte = start_pos as usize * self.dictionary.bit_width() / 8;
            let end_pos =
                (4096 * 8 / self.dictionary.bit_width()).min((self.end - self.cursor) as usize);
            let end_byte = (end_pos * self.dictionary.bit_width() + 7) / 8;
            let size = end_byte - start_byte;
            let mut buf = vec![0; size];
            self.primary_table
                .read_block(start_byte as u64, &mut buf[..])?;
            self.primary_table_buffer = Some((start_pos, buf));
        }
        Ok(())
    }
    pub fn read_next_value(&mut self) -> Result<(u32, i32)> {
        self.ensure_primary_table_buffer()?;
        todo!()
    }
}

impl<'a, R: Read + Seek + 'a> D4TrackReader<'a, R> {
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

        let header_stream = track_root.open_stream(".metadata")?;
        let header = Header::read(header_stream)?;
        let primary_table = track_root.open_blob(".ptab")?;

        let chrom_list = header.chrom_list();

        let (secondary_table, compressed_stab) = {
            let root = track_root.open_directory(".stab")?;
            let metadata: SimpleKvMetadata = {
                let mut stream = root.open_stream(".metadata")?;
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
            (
                buf,
                matches!(metadata.compression(), CompressionMethod::Deflate(..)),
            )
        };

        Ok(D4TrackReader {
            header,
            primary_table,
            compressed_stab,
            secondary_table,
        })
    }
}

#[test]
fn test_open_hg002() {
    use std::fs::File;
    env_logger::init();
    let hg002_file =
        http::HttpReader::new("https://home.chpc.utah.edu/~u0875014/hg002.d4").unwrap();
    let reader = D4TrackReader::from_reader(hg002_file, None).unwrap();
}
