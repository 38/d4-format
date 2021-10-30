use std::{
    fs::{File, OpenOptions},
    io::Result,
    path::Path,
};

use d4_framefile::Directory;

use crate::d4file::validate_header;

pub const INDEX_ROOT_NAME: &'static str = ".index";
pub const SECONDARY_FRAME_INDEX_NAME: &'static str = "s_frame_index";

#[allow(dead_code)]
pub struct D4IndexCollection {
    track_root: Directory<File>,
    index_root: Directory<File>,
}

impl D4IndexCollection {
    pub fn open_for_write<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut fp = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .truncate(false)
            .open(path.as_ref())?;
        // TODO: Currently we assume that only single track file can be indexed. This means we need to index the file before merge otherwise we can not add any index to the file, but later this might be changed
        validate_header(&mut fp)?;
        let mut file_root = Directory::open_root_rw(fp, 8)?;

        // TODO: For mutliple track support, we need find the track root first
        let track_root = file_root.clone();

        let index_root = file_root.open_or_create_directory(INDEX_ROOT_NAME)?;

        Ok(Self {
            track_root,
            index_root,
        })
    }
    pub fn create_secondary_frame_index(&mut self) -> ! {
        todo!()
    }
}

#[test]
fn test_main() {
    D4IndexCollection::open_for_write("/tmp/hg002.d4").unwrap();
}