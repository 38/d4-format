use d4_framefile::{Directory, OpenResult};

use std::fs::File;
use std::io::{Error, Result};
use std::path::Path;

use crate::find_tracks_in_file;
use crate::header::Header;
use crate::ptab::{BitArrayReader, PrimaryTablePartReader, PrimaryTableReader};
use crate::stab::{RangeRecord, SecondaryTableReader, SparseArrayReader};

/// The reader that reads a D4 file
pub struct D4TrackReader<
    P: PrimaryTableReader = BitArrayReader,
    S: SecondaryTableReader = SparseArrayReader<RangeRecord>,
> {
    _root: Directory<File>,
    header: Header,
    p_table: P,
    s_table: S,
}

impl<P: PrimaryTableReader, S: SecondaryTableReader> D4TrackReader<P, S> {
    pub fn as_root_container(&self) -> &Directory<File> {
        &self._root
    }
    /// Split the input D4 file into small chunks
    pub fn split(
        &mut self,
        size_limit: Option<usize>,
    ) -> Result<Vec<(P::Partition, S::Partition)>> {
        let p_parts = self.p_table.split(&self.header, size_limit)?;
        let partition: Vec<_> = p_parts.iter().map(|p| p.region()).collect();
        let s_parts = self.s_table.split(partition.as_ref())?;
        Ok(p_parts.into_iter().zip(s_parts).collect())
    }
    pub fn chrom_regions(&self) -> Vec<(&str, u32, u32)> {
        self.header
            .chrom_list()
            .iter()
            .map(|x| (x.name.as_str(), 0, x.size as u32))
            .collect()
    }
    /// Get the header of the input D4 file
    pub fn header(&self) -> &Header {
        &self.header
    }

    pub fn create_reader_for_root(mut root: Directory<File>) -> Result<Self> {
        let stream = root.open_stream(".metadata")?;
        let header = Header::read(stream)?;
        let p_table = PrimaryTableReader::create(&mut root, &header)?;
        let s_table = SecondaryTableReader::create(&mut root, &header)?;
        Ok(Self {
            _root: root,
            header,
            p_table,
            s_table,
        })
    }

    pub fn open(track_spec: &str) -> Result<Self> {
        if let Some(split_pos) = track_spec.find(':') {
            let (path, track) = (&track_spec[..split_pos], &track_spec[split_pos + 1..]);
            Self::open_track_with_path(path, track)
        } else {
            Self::open_first_track(track_spec)
        }
    }

    pub fn open_tracks<PathType: AsRef<Path>, Predict: FnMut(Option<&Path>) -> bool>(
        path: PathType,
        mut track_pattern: Predict,
    ) -> Result<Vec<Self>> {
        let mut buf = Vec::new();
        find_tracks_in_file(path.as_ref(), |_| true, &mut buf)?;
        let mut ret = Vec::new();
        let file = super::open_file_and_validate_header(path.as_ref())?;
        let file_root = Directory::open_root(file, 8)?;
        for track_path in buf.into_iter().filter(|p| track_pattern(Some(p.as_path()))) {
            let track_root = match file_root.open(track_path)? {
                OpenResult::SubDir(root) => root,
                _ => continue,
            };
            ret.push(Self::create_reader_for_root(track_root)?);
        }
        Ok(ret)
    }

    pub fn open_track_with_path<PathType: AsRef<Path>, TrackType: AsRef<Path>>(
        path: PathType,
        track: TrackType,
    ) -> Result<Self> {
        let fp: File = super::open_file_and_validate_header(path)?;
        let file_root = Directory::open_root(fp, 8)?;
        let track_root = match file_root.open(track)? {
            OpenResult::SubDir(dir) => dir,
            _ => {
                return Err(Error::new(
                    std::io::ErrorKind::Other,
                    "Cannot open D4 data track",
                ))
            }
        };
        Self::create_reader_for_root(track_root)
    }

    /// Open a D4 file for read, load the first available data track
    pub fn open_first_track<PathType: AsRef<Path>>(path: PathType) -> Result<Self> {
        let fp = super::open_file_and_validate_header(path)?;
        let root = {
            let file_root = Directory::open_root(fp, 8)?;
            if let Some(mut track_metadata_path) = file_root.find_first_object(".metadata") {
                track_metadata_path.pop();
                let track_root_path = track_metadata_path;
                match file_root.open(track_root_path)? {
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
        Self::create_reader_for_root(root)
    }
}
