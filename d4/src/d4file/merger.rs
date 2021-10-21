use super::D4FileBuilder;
use std::{
    io::{Result, Seek, SeekFrom},
    path::{Path, PathBuf},
};

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

    pub fn add_input<P: AsRef<Path>>(self, dest: P) -> Self {
        if let Some(tag) = dest.as_ref().file_stem().map(|x| x.to_string_lossy()) {
            self.add_input_with_tag(&dest, tag.as_ref())
        } else {
            self
        }
    }

    pub fn add_input_with_tag<P: AsRef<Path>>(mut self, dest: P, tag: &str) -> Self {
        self.sources
            .push((tag.to_owned(), dest.as_ref().to_owned()));
        self
    }

    pub fn merge(self) -> Result<()> {
        let mut root_dir = D4FileBuilder::write_d4_header(self.dest.as_path())?;
        for (name, path) in self.sources {
            let mut input = super::open_file_and_validate_header(path)?;
            let size = input.seek(SeekFrom::End(0))?;
            root_dir.copy_directory_from_file(&name, input, 8, size as usize - 8)?;
        }
        Ok(())
    }
}
