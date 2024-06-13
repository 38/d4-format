#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
use std::fs::File;
use std::io::{Read, Result, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::sync::{Arc, Mutex};

/// The file object that supports random access. Since in D4 file,
/// we actually use a random access file mode, which means all the read
/// and write needs to provide the address in file.
/// And this is the object that provides the low level random access interface.
///
/// At the same time, this RandFile object is synchronized, which means we guarantee
/// the thread safety that each block of data is written to file correctly (without overlaps).
///
/// The rand file provides a offset-based file access API and data can be read and write from the
/// specified address in blocks. But rand file itself doesn't tracking the block size and it's the
/// upper layer's responsibility to determine the correct block beginning.
pub struct RandFile<T> {
    inner: Arc<Mutex<IoWrapper<T>>>,
    token: u32,
}

impl<T> Drop for RandFile<T> {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        if inner.token_stack[self.token as usize].ref_count > 0 {
            inner.token_stack[self.token as usize].ref_count -= 1;
        }
        let mut update_callbacks = vec![];
        while inner.current_token > 0
            && inner.token_stack[inner.current_token as usize].ref_count == 0
        {
            inner.current_token -= 1;
            if let Some(TokenStackItem {
                on_release: update, ..
            }) = inner.token_stack.pop()
            {
                update_callbacks.push(update);
            }
        }
        drop(inner);
        update_callbacks.into_iter().for_each(|f| f());
    }
}

struct TokenStackItem {
    ref_count: u32,
    on_release: Box<dyn FnOnce() + Send>,
}

/// This is the internal wrapper of an IO object that used by D4 randfile.
/// It's used with mutex and enforces the lock policy D4 randfile is using.
/// This wrapper is shared between different higher level IO objects, for instance: a directory in
/// framefile.
struct IoWrapper<T> {
    inner: T,
    current_token: u32,
    token_stack: Vec<TokenStackItem>,
}

impl<T> IoWrapper<T> {
    fn try_borrow_mut(&mut self, token: u32) -> Result<&mut T> {
        if token == self.current_token {
            Ok(&mut self.inner)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Rand file locked",
            ))
        }
    }
    fn seek(&mut self, addr: u64) -> Result<()>
    where
        T: Seek,
    {
        self.inner.seek(SeekFrom::Start(addr))?;
        Ok(())
    }
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>
    where
        T: Read,
    {
        self.inner.read(buf)
    }
}

impl<T> Deref for IoWrapper<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.inner
    }
}

impl<T> Clone for RandFile<T> {
    fn clone(&self) -> Self {
        self.inner.lock().unwrap().token_stack[self.token as usize].ref_count += 1;
        Self {
            inner: self.inner.clone(),
            token: self.token,
        }
    }
}

impl<T> RandFile<T> {
    pub fn clone_inner(&self) -> Result<T>
    where
        T: Clone,
    {
        let inner = self
            .inner
            .lock()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Lock Error"))?;
        Ok(inner.inner.clone())
    }
    /// Create a new random access file wrapper
    ///
    /// - `inner`: The underlying implementation for the backend
    /// - `returns`: The newly created random file object
    pub(crate) fn new(inner: T) -> Self {
        RandFile {
            inner: Arc::new(Mutex::new(IoWrapper {
                current_token: 0,
                token_stack: vec![TokenStackItem {
                    ref_count: 1,
                    on_release: Box::new(|| ()),
                }],
                inner,
            })),
            token: 0,
        }
    }

    /// Lock the current IO object and derive a fresh token
    /// This will prevent any object that holds earlier token from locking this file again.
    /// However, the freshly returned token can be cloned.
    pub fn lock(&mut self, update_fn: Box<dyn FnOnce() + Send>) -> Result<Self> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Lock Error"))?;
        inner.current_token += 1;
        inner.token_stack.push(TokenStackItem {
            ref_count: 1,
            on_release: update_fn,
        });
        let token = inner.current_token;
        drop(inner);
        Ok(RandFile {
            inner: self.inner.clone(),
            token,
        })
    }
}

impl<T: Read + Write + Seek> RandFile<T> {
    /// The convenient helper function to create a read-write random file
    ///
    /// - `inner`: The underlying implementation for this backend
    pub fn for_read_write(inner: T) -> Self {
        Self::new(inner)
    }
}

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
impl RandFile<File> {
    pub fn mmap(&self, offset: u64, size: usize) -> Result<mapping::MappingHandle> {
        mapping::MappingHandle::new(self, offset, size)
    }

    pub fn mmap_mut(&mut self, offset: u64, size: usize) -> Result<mapping::MappingHandleMut> {
        mapping::MappingHandleMut::new(self, offset, size)
    }
}

impl<T: Write + Seek> RandFile<T> {
    /// Append a block to the random accessing file
    /// the return value is the relative address compare to the last
    /// accessed block.
    ///
    /// - `buf`: The data buffer that needs to be write
    /// - `returns`: The absolute address of the block that has been written to the file.
    pub fn append_block(&mut self, buf: &[u8]) -> Result<u64> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "LockError"))?;
        let ret = inner.try_borrow_mut(self.token)?.seek(SeekFrom::End(0))?;
        inner.try_borrow_mut(self.token)?.write_all(buf)?;
        Ok(ret)
    }

    /// Update a data block with the given data buffer.
    ///
    /// - `offset`: The offset of the data block
    /// - `buf`: The data buffer to write
    pub fn update_block(&mut self, offset: u64, buf: &[u8]) -> Result<()> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "LockError"))?;
        inner
            .try_borrow_mut(self.token)?
            .seek(SeekFrom::Start(offset))?;
        inner.try_borrow_mut(self.token)?.write_all(buf)?;
        Ok(())
    }

    /// Reserve some space in the rand file. This is useful when we want to reserve a data block
    /// for future use. This is very useful for some volatile data (for example the directory block), etc.
    /// And later, we are able to use `update_block` function to keep the reserved block up-to-dated
    pub fn reserve_block(&mut self, size: usize) -> Result<u64> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "LockError"))?;
        let ret = inner.try_borrow_mut(self.token)?.seek(SeekFrom::End(0))?;
        inner
            .try_borrow_mut(self.token)?
            .seek(SeekFrom::Current(size as i64 - 1))?;
        inner.try_borrow_mut(self.token)?.write_all(b"\0")?;
        Ok(ret)
    }
}

impl<T: Read + Seek> RandFile<T> {
    pub fn size(&mut self) -> Result<u64> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "LockError"))?;
        inner.try_borrow_mut(self.token)?.seek(SeekFrom::End(0))
    }
    /// Read a block from the random accessing file
    /// the size of the buffer slice is equal to the number of bytes that is requesting
    /// But there might not be enough bytes available for read, thus we always return
    /// the actual number of bytes is loaded
    pub fn read_block(&mut self, addr: u64, buf: &mut [u8]) -> Result<usize> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "LockError"))?;
        inner.seek(addr)?;
        let mut ret = 0;
        loop {
            let bytes_read = inner.read(&mut buf[ret..])?;
            if bytes_read == 0 {
                break Ok(ret);
            }
            ret += bytes_read;
        }
    }
}

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
pub mod mapping {
    use super::*;

    use memmap::{Mmap, MmapMut, MmapOptions};
    use std::fs::File;
    use std::io::{Error, ErrorKind};
    use std::sync::Arc;

    struct SyncGuard(MmapMut);

    impl Drop for SyncGuard {
        fn drop(&mut self) {
            self.0.flush().expect("Sync Error");
        }
    }

    #[derive(Clone)]
    pub struct MappingHandle(Arc<Option<Mmap>>);

    impl AsRef<[u8]> for MappingHandle {
        fn as_ref(&self) -> &[u8] {
            if let Some(ref mmap) = *self.0 {
                mmap.as_ref()
            } else {
                &[]
            }
        }
    }

    impl MappingHandle {
        pub(super) fn new(file: &RandFile<File>, offset: u64, size: usize) -> Result<Self> {
            let inner = file
                .inner
                .lock()
                .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
            let mapped = if size > 0 {
                Some(unsafe { MmapOptions::new().offset(offset).len(size).map(&inner)? })
            } else {
                None
            };
            drop(inner);
            Ok(MappingHandle(Arc::new(mapped)))
        }
    }

    #[derive(Clone)]
    pub struct MappingHandleMut {
        handle: Arc<SyncGuard>,
        base_addr: *mut u8,
        size: usize,
    }

    unsafe impl Send for MappingHandleMut {}

    impl AsRef<[u8]> for MappingHandleMut {
        fn as_ref(&self) -> &[u8] {
            self.handle.as_ref().0.as_ref()
        }
    }

    impl AsMut<[u8]> for MappingHandleMut {
        fn as_mut(&mut self) -> &mut [u8] {
            unsafe { std::slice::from_raw_parts_mut(self.base_addr, self.size) }
        }
    }

    impl MappingHandleMut {
        pub(super) fn new(file: &RandFile<File>, offset: u64, size: usize) -> Result<Self> {
            let inner = file
                .inner
                .lock()
                .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
            let mut mapped = unsafe {
                MmapOptions::new()
                    .offset(offset)
                    .len(size)
                    .map_mut(&inner)?
            };
            drop(inner);
            let base_addr = mapped.as_mut().as_mut_ptr();
            Ok(MappingHandleMut {
                handle: Arc::new(SyncGuard(mapped)),
                base_addr,
                size,
            })
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Cursor;
    #[test]
    fn test_from_inner() {
        let backend = Cursor::new(vec![0; 1024]);
        let _rand_file = RandFile::new(backend);

        let backend = Cursor::new(vec![0; 1024]);
        let _rand_file = RandFile::new(backend);
    }

    #[test]
    fn test_read_write_blocks() {
        let backend = Cursor::new(vec![0; 0]);
        let mut rand_file = RandFile::new(backend);
        assert_eq!(0, rand_file.append_block(b"This is a test block").unwrap());
        assert_eq!(20, rand_file.append_block(b"This is a test block").unwrap());

        let mut buf = [0u8; 20];
        assert_eq!(20, rand_file.read_block(0, &mut buf).unwrap());
        assert_eq!(b"This is a test block", &buf);
    }

    #[test]
    fn test_lock() {
        let backend = Cursor::new(vec![0; 0]);
        let mut rand_file = RandFile::new(backend);
        let flag = Arc::new(std::sync::Mutex::new(false));
        {
            let flag = flag.clone();
            let mut locked = rand_file
                .lock(Box::new(move || {
                    *flag.lock().unwrap() = true;
                }))
                .unwrap();
            let mut locked_clone = locked.clone();

            locked.append_block(b"a").unwrap();
            locked_clone.append_block(b"a").unwrap();

            rand_file.append_block(b"c").expect_err("Should be error!");
        }
        rand_file.append_block(b"c").unwrap();
        assert!(*flag.lock().unwrap());
    }
}
