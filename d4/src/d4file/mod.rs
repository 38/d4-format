mod reader;
mod writer;

use std::{
    io::{Result, Seek, SeekFrom},
    path::{Path, PathBuf},
};

use d4_framefile::{Directory, EntryKind};
pub use reader::D4FileReader;
pub use writer::{D4FileBuilder, D4FileWriter};

/// The D4 magic number
pub const FILE_MAGIC_NUM: &[u8] = b"d4\xdd\xdd";

pub fn find_tracks_in_file<Pat: Fn(Option<&Path>) -> bool, PathType: AsRef<Path>>(
    path: PathType,
    pattern: Pat,
    buf: &mut Vec<PathBuf>,
) -> Result<()> {
    let fp = reader::open_file_and_validate_header(path)?;
    let file_root = Directory::open_root(fp, 8)?;
    file_root.recurse(|path, kind| {
        if path.file_name().unwrap_or_default() == ".metadata"
            && kind == EntryKind::Stream
            && pattern(path.parent())
        {
            buf.push(path.parent().map(ToOwned::to_owned).unwrap_or_default());
            return false;
        }
        true
    });

    Ok(())
}

pub struct D4FileMerger {
    dest: PathBuf,
    sources: Vec<(String, PathBuf)>,
}

impl D4FileMerger {
    pub fn new<P: AsRef<Path>>(target: P) -> Self {
        Self {
            dest: target.as_ref().to_owned(),
            sources: Vec::new(),
        }
    }

    pub fn add_input<P: AsRef<Path>>(mut self, dest: P) -> Self {
        let tag = dest
            .as_ref()
            .file_stem()
            .map(|x| x.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("{}", self.sources.len()));
        self.sources.push((tag, dest.as_ref().to_owned()));
        self
    }

    pub fn merge(self) -> Result<()> {
        let mut root_dir = D4FileBuilder::write_d4_header(self.dest.as_path())?;
        for (name, path) in self.sources {
            let mut input = reader::open_file_and_validate_header(path)?;
            let size = input.seek(SeekFrom::End(0))?;
            root_dir.copy_directory_from_file(&name, input, 8, size as usize - 8)?;
        }
        Ok(())
    }
}
