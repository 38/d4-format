use crate::mode::{AccessMode, CanRead, CanWrite};
use crate::randfile::RandFile;
use std::fs::File;
use std::io::Result;

pub struct Chunk<'a, M: AccessMode, T: 'a> {
    file: RandFile<'a, M, T>,
    size: usize,
    offset: u64,
}

impl<'a, M: AccessMode, T> Chunk<'a, M, T> {
    pub(crate) fn new(file: RandFile<'a, M, T>, offset: u64, size: usize) -> Self {
        Self { file, size, offset }
    }
}

impl<M: CanRead<File>> Chunk<'_, M, File> {
    pub fn mmap(&self) -> Result<impl AsRef<[u8]>> {
        self.file.mmap(self.offset, self.size)
    }
}

impl<M: CanRead<File> + CanWrite<File>> Chunk<'_, M, File> {
    pub fn mmap_mut(&mut self) -> Result<impl AsMut<[u8]>> {
        self.file.mmap_mut(self.offset, self.size)
    }
}

// TODO: We need support the read/write API as well
