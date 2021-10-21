use crate::mode::AccessMode;

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
use crate::mode::CanWrite;

use crate::mode::CanRead;

use crate::randfile::RandFile;
use std::io::{Read, Result, Seek};

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
use std::fs::File;

pub struct Blob<'a, M: AccessMode, T: 'a> {
    file: RandFile<'a, M, T>,
    size: usize,
    offset: u64,
}

impl<'a, T: Read + Seek + 'a, M: CanRead<T>> Blob<'a, M, T> {
    pub fn get_view(&self, offset: u64, size: usize) -> Self {
        let offset = offset.min(self.size as u64);
        let size = (self.size - offset as usize).min(size);

        Self {
            file: self.file.clone(),
            size,
            offset,
        }
    }
    pub fn read_block(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if self.size < self.offset as usize {
            return Ok(0);
        }

        let bytes_to_read = buf.len().min(self.size - offset as usize);

        self.file.read_block(self.offset + offset, buf)?;

        Ok(bytes_to_read)
    }
}

impl<'a, M: AccessMode, T> Blob<'a, M, T> {
    pub(crate) fn new(file: RandFile<'a, M, T>, offset: u64, size: usize) -> Self {
        Self { file, size, offset }
    }
}

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
impl<M: CanRead<File>> Blob<'_, M, File> {
    pub fn mmap(&self) -> Result<impl AsRef<[u8]>> {
        self.file.mmap(self.offset, self.size)
    }
}

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
impl<M: CanRead<File> + CanWrite<File>> Blob<'_, M, File> {
    pub fn mmap_mut(&mut self) -> Result<impl AsMut<[u8]>> {
        self.file.mmap_mut(self.offset, self.size)
    }
}
