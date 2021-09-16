mod reader;
mod writer;

use std::{
    fs::File,
    io::{Result, Seek, SeekFrom},
    path::{Path, PathBuf},
};

pub use reader::D4FileReader;
pub use writer::{D4FileBuilder, D4FileWriter};

/// The D4 magic number
pub const FILE_MAGIC_NUM: &'static [u8] = b"d4\xdd\xdd";

#[allow(dead_code)]
pub struct D4FileMerger {
    dest: PathBuf,
    sources: Vec<(String, PathBuf)>,
}

impl D4FileMerger {
    #[allow(dead_code)]
    pub fn new<P: AsRef<Path>>(target: P) -> Self {
        Self {
            dest: target.as_ref().to_owned(),
            sources: Vec::new(),
        }
    }
    #[allow(dead_code)]
    pub fn add_input<P: AsRef<Path>>(mut self, dest: P) -> Self {
        let tag = dest
            .as_ref()
            .file_stem()
            .map(|x| x.to_string_lossy().to_string())
            .unwrap_or_else(|| format!("{}", self.sources.len()));
        self.sources.push((tag, dest.as_ref().to_owned()));
        self
    }
    #[allow(dead_code)]
    pub fn merge(self) -> Result<()> {
        let mut root_dir = D4FileBuilder::write_d4_header(self.dest.as_path())?;
        for (name, path) in self.sources {
            let mut input = File::open(path)?;
            // TODO: validate the input file is valid D4
            let size = input.seek(SeekFrom::End(0))?;
            root_dir.copy_directory_from_file(&name, input, 8, size as usize - 8)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test_merge() {
        super::D4FileMerger::new("/tmp/merged.d4")
            .add_input("/home/haohou/base2/data/hg002.d4")
            .merge()
            .unwrap();
    }
}
