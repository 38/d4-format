use std::{
    fs::{File, OpenOptions},
    io::{Read, Result, Seek},
    path::Path,
};

use d4_framefile::Directory;

use crate::{d4file::validate_header, Header};

pub const INDEX_ROOT_NAME: &'static str = ".index";
pub const SECONDARY_FRAME_INDEX_NAME: &'static str = "s_frame_index";

mod sfi;

pub use sfi::{RecordFrameAddress, SecondaryFrameIndex};

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

        SecondaryFrameIndex::from_reader(
            self.index_root
                .open_stream(SecondaryFrameIndex::STREAM_NAME)?,
            header,
        )
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
        sfi_index.write(
            self.index_root
                .create_stream(SecondaryFrameIndex::STREAM_NAME, 511)?,
        )
    }
}

#[test]
fn test_read() {
    let idx = D4IndexCollection::from_reader(File::open("/tmp/hg002.d4").unwrap()).unwrap();
    let sfi = idx.load_seconary_frame_index().unwrap();
    //let stab = idx.track_root.open_directory(SECONDARY_TABLE_NAME).unwrap();

    let addr = sfi.find_partial_seconary_table("1", 0).unwrap().unwrap();
    assert_eq!(addr.first_frame, true);

    let addr = sfi
        .find_partial_seconary_table("1", 123456)
        .unwrap()
        .unwrap();
    assert_eq!(addr.first_frame, false);

    sfi.print_secondary_table_index(std::io::stdout()).unwrap();
}
