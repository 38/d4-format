use crate::mode::AccessMode;

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
use crate::mode::{CanRead, CanWrite};

use crate::randfile::RandFile;
#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
use std::{fs::File, io::Result};

#[allow(dead_code)]
pub struct Blob<'a, M: AccessMode, T: 'a> {
    file: RandFile<'a, M, T>,
    size: usize,
    offset: u64,
}

// TODO: Make sure we can read a blob that is not memmapped

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

// TODO: We need support the read/write API as well
