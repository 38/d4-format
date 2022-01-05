use std::{
    fs::{File, OpenOptions},
    io::{Cursor, Read, Result, Seek},
    path::Path,
};

use d4_framefile::Directory;

use crate::{d4file::validate_header, Header};

pub const INDEX_ROOT_NAME: &str = ".index";
pub const SECONDARY_FRAME_INDEX_NAME: &str = "s_frame_index";

mod data_index;
mod sfi;

pub use data_index::{DataIndexQueryResult, DataIndexRef, DataSummary, Sum};
pub use sfi::{RecordFrameAddress, SecondaryFrameIndex};

use self::data_index::DataIndex;

#[allow(dead_code)]
pub struct D4IndexCollection<T> {
    track_root: Directory<T>,
    index_root: Directory<T>,
}

impl<T: Read + Seek> D4IndexCollection<T> {
    pub fn from_root_container(file_root: &Directory<T>) -> Result<Self> {
        let track_root = file_root.clone();
        let index_root = file_root.open_directory(INDEX_ROOT_NAME)?;
        Ok(Self {
            track_root,
            index_root,
        })
    }
    pub fn from_reader(mut reader: T) -> Result<Self> {
        validate_header(&mut reader)?;
        let file_root = Directory::open_root(reader, 8)?;
        Self::from_root_container(&file_root)
    }
    pub fn load_seconary_frame_index(&self) -> Result<SecondaryFrameIndex> {
        let header = Header::read(self.track_root.open_stream(Header::HEADER_STREAM_NAME)?)?;
        let mut blob = self
            .index_root
            .open_blob(SecondaryFrameIndex::STREAM_NAME)?;

        SecondaryFrameIndex::from_reader(blob.get_reader(), header)
    }
    pub fn load_data_index<S: DataSummary>(&self) -> Result<DataIndexRef<S>> {
        let header = Header::read(self.track_root.open_stream(Header::HEADER_STREAM_NAME)?)?;
        let mut data_index_blob = self.index_root.open_blob(S::INDEX_NAME)?;
        DataIndex::<S>::from_blob(&mut data_index_blob, header.chrom_list())
    }
}

impl D4IndexCollection<File> {
    pub fn open_for_write<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut fp = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .truncate(false)
            .open(path.as_ref())?;
        // TODO: Currently we assume that only single track file can be indexed. This means we need to index the file before merge otherwise we can not add any index to the file, but later this might be changed
        validate_header(&mut fp)?;
        let mut file_root = Directory::open_root_for_update(fp, 8)?;

        // TODO: For mutliple track support, we need find the track root first
        let track_root = file_root.clone();

        let index_root = file_root.open_or_create_directory(INDEX_ROOT_NAME)?;

        Ok(Self {
            track_root,
            index_root,
        })
    }
    pub fn create_secondary_frame_index(&mut self) -> Result<()> {
        let sfi_index = sfi::SecondaryFrameIndex::from_data_track(&self.track_root)?;
        let blob_size = sfi_index.get_blob_size();
        let mut blob = self
            .index_root
            .create_blob(SecondaryFrameIndex::STREAM_NAME, blob_size)?;
        let mut mapped_blob = blob.mmap_mut()?;
        let writer = Cursor::new(mapped_blob.as_mut());
        sfi_index.write(writer)?;
        Ok(())
    }
    pub fn create_sum_index(&mut self) -> Result<()> {
        DataIndex::<Sum>::build(&mut self.track_root, &mut self.index_root, 65536)?;
        Ok(())
    }
}
