use d4_framefile::mode::ReadOnly;
use d4_framefile::{Directory, OpenResult};

use std::fs::File;
use std::io::{Read, Result};
use std::path::Path;

use crate::header::Header;
use crate::ptab::{PTablePartitionReader, PTableReader, UncompressedReader};
use crate::stab::{RangeRecord, STableReader, SimpleKeyValueReader};

/// The reader that reads a D4 file
pub struct D4FileReader<
    P: PTableReader = UncompressedReader,
    S: STableReader = SimpleKeyValueReader<RangeRecord>,
> {
    _root: Directory<'static, ReadOnly, File>,
    header: Header,
    p_table: P,
    s_table: S,
}

impl<P: PTableReader, S: STableReader> D4FileReader<P, S> {
    /// Split the input D4 file into small chunks
    pub fn split(
        &mut self,
        size_limit: Option<usize>,
    ) -> Result<Vec<(P::Partition, S::Partition)>> {
        let p_parts = self.p_table.split(&self.header, size_limit)?;
        let partition: Vec<_> = p_parts.iter().map(|p| p.region()).collect();
        let s_parts = self.s_table.split(partition.as_ref())?;
        Ok(p_parts.into_iter().zip(s_parts.into_iter()).collect())
    }
    /// Get the header of the input D4 file
    pub fn header(&self) -> &Header {
        &self.header
    }
    /// Open a D4 file for read
    pub fn open<PathType: AsRef<Path>>(path: PathType) -> Result<Self> {
        let mut fp = File::open(path.as_ref())?;
        let mut signature = [0u8; 8];
        fp.read_exact(&mut signature[..])?;
        if signature[..4] != super::FILE_MAGIC_NUM[..] {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Invalid D4 File magic number",
            ));
        }

        let mut root = {
            let file_root = Directory::open_root(fp, 8)?;
            if let Some(mut track_metadata_path) = file_root.find_first_object(".metadata") {
                track_metadata_path.pop();
                let track_root_path = track_metadata_path;
                match file_root.open(&track_root_path)? {
                    OpenResult::SubDir(root) => root,
                    _ => unreachable!(),
                }
            } else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Empty container",
                ));
            }
        };

        let header_data = {
            let mut stream = root.open_stream(".metadata")?;
            let mut ret = vec![];
            loop {
                let mut buf = [0u8; 4096];
                let sz = stream.read(&mut buf)?;
                let mut actual_size = sz;
                while actual_size > 0 && buf[actual_size - 1] == 0 {
                    actual_size -= 1;
                }
                ret.extend_from_slice(&buf[..actual_size]);
                if actual_size != sz {
                    break ret;
                }
            }
        };
        let header = serde_json::from_str(String::from_utf8_lossy(&header_data).as_ref())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Invalid Metadata"))?;
        let p_table = PTableReader::create(&mut root, &header)?;
        let s_table = STableReader::create(&mut root, &header)?;
        Ok(Self {
            _root: root,
            header,
            p_table,
            s_table,
        })
    }
}
