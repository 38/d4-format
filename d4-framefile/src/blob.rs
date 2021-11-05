use crate::randfile::RandFile;
use std::io::{Read, Result, Seek};

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
use std::fs::File;

pub struct Blob<T> {
    file: RandFile<T>,
    size: usize,
    offset: u64,
}

impl<T: Read + Seek> Blob<T> {
    pub fn size(&self) -> usize {
        self.size
    }
    pub fn get_view(&self, offset: u64, size: usize) -> Self {
        let offset = self.offset + offset.min(self.size as u64);
        let size = (self.size - offset as usize).min(size);

        Self {
            file: self.file.clone(),
            size,
            offset,
        }
    }
    pub fn read_block(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if self.size < offset as usize {
            return Ok(0);
        }

        let bytes_to_read = buf.len().min(self.size - offset as usize);

        self.file.read_block(self.offset + offset, buf)?;

        Ok(bytes_to_read)
    }
}

impl<T> Blob<T> {
    pub(crate) fn new(file: RandFile<T>, offset: u64, size: usize) -> Self {
        Self { file, size, offset }
    }
}

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
impl Blob<File> {
    pub fn mmap(&self) -> Result<impl AsRef<[u8]>> {
        self.file.mmap(self.offset, self.size)
    }
    pub fn mmap_mut(&mut self) -> Result<impl AsMut<[u8]>> {
        self.file.mmap_mut(self.offset, self.size)
    }
}
