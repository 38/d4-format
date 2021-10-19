#[cfg(feature = "mapped_io")]
use crate::mapped::MappedDirectory;
use crate::mode::{AccessMode, CanWrite, ReadOnly, ReadWrite};
use crate::randfile::RandFile;
use crate::stream::Stream;
use crate::Blob;
use std::fs::File;
use std::io::{Error, ErrorKind, Read, Result, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, RwLock};

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum EntryKind {
    /// A stream with arbitrary size
    Stream = 0,
    /// A sub-directory
    SubDir = 1,
    /// A fix sized blob
    Blob = 2,
}

/// Describes an entry in a directory
#[derive(Clone, Debug)]
pub struct Entry {
    /// The type of the entry
    pub kind: EntryKind,
    /// The absolute offset of the beginning of the data object
    /// - for streams, this is the address of the primary frame of the stream
    /// - for blobs, this is the address of the actual blob
    /// - for directory, this is the address of the primary frame of the metadata stream for this directory
    /// Note in the file, this is actually the relative offset from the beginning of the parent directory
    pub primary_offset: u64,
    /// The size of the primary frame of the data object.
    /// - For blobs and directories,  it describes the **total size** of the object
    /// - For streams, this only describes the primary frame of the data object
    pub primary_size: usize,
    /// The name of the stream
    pub name: String,
}

struct DirectoryImpl<'a, M: AccessMode, T> {
    offset: u64,
    entries: Vec<Entry>,
    stream: Stream<'a, M, T>,
}

pub struct Directory<'a, M: AccessMode, T>(Arc<RwLock<DirectoryImpl<'a, M, T>>>);

impl<M: AccessMode, T> Clone for Directory<'_, M, T> {
    fn clone(&self) -> Self {
        Directory(self.0.clone())
    }
}

impl<M: AccessMode, T> Directory<'_, M, T> {
    pub const INIT_BLOCK_SIZE: usize = 512;
    /// Get the type of the child object
    pub fn entry_kind(&self, name: &str) -> Option<EntryKind> {
        self.0.read().unwrap().entries.iter().find_map(|e| {
            if e.name == name {
                return Some(e.kind);
            }
            None
        })
    }
}

impl<'a, T: Read + Seek + 'a> Directory<'a, ReadOnly, T> {
    /// Get a list of children under this directory
    pub fn entries(&self) -> Vec<Entry> {
        self.0.read().unwrap().entries.clone()
    }
    /// Open an root directory from a seek-able backend stream and an **absolute** offset
    pub fn open_root(back: T, offset: u64) -> Result<Directory<'a, ReadOnly, T>> {
        let randfile = RandFile::for_read_only(back);
        Self::open_directory_impl(randfile, offset)
    }
    pub(crate) fn read_next_entry<R: Read>(base: u64, input: &mut R) -> Result<Option<Entry>> {
        let mut has_next = [0u8];
        if input.read(&mut has_next)? != 1 || has_next[0] == 0 {
            return Ok(None);
        }
        let mut kind_buffer = [0];
        let mut offset_buffer = [0; 8];
        let mut size_buffer = [0; 8];
        input.read_exact(&mut kind_buffer)?;
        input.read_exact(&mut offset_buffer)?;
        input.read_exact(&mut size_buffer)?;
        let offset = u64::from_le_bytes(offset_buffer) + base;
        let size = usize::from_le_bytes(size_buffer);
        let mut name = vec![];
        let mut current_byte = [0];
        while input.read(&mut current_byte)? > 0 {
            if current_byte[0] == 0 {
                break;
            }
            name.push(current_byte[0]);
        }
        let name = String::from_utf8_lossy(&name[..]).to_string();
        let kind = match kind_buffer[0] {
            0 => EntryKind::Stream,
            1 => EntryKind::SubDir,
            2 => EntryKind::Blob,
            _ => return Err(Error::new(ErrorKind::Other, "Invalid directory type code")),
        };
        Ok(Some(Entry {
            kind,
            name,
            primary_offset: offset,
            primary_size: size,
        }))
    }
    fn open_directory_impl(
        randfile: RandFile<'a, ReadOnly, T>,
        offset: u64,
    ) -> Result<Directory<'a, ReadOnly, T>> {
        let mut stream = Stream::open_ro(randfile, (offset, Self::INIT_BLOCK_SIZE))?;
        let mut entries = vec![];
        while let Some(entry) = Self::read_next_entry(offset, &mut stream)? {
            entries.push(entry);
        }
        Ok(Directory(Arc::new(RwLock::new(DirectoryImpl {
            offset,
            entries,
            stream,
        }))))
    }
}
impl<'a, T: Read + Write + Seek + 'a> Directory<'a, ReadWrite, T> {
    pub fn make_root(back: T) -> Result<Directory<'a, ReadWrite, T>> {
        let randfile = RandFile::for_read_write(back);
        let stream = Stream::create_rw(randfile, 512)?;
        let entries = vec![];
        Ok(Directory(Arc::new(RwLock::new(DirectoryImpl {
            offset: stream.get_frame_offset().unwrap(),
            entries,
            stream,
        }))))
    }
}

impl<'a, M: CanWrite<T>, T: Write + Seek + 'a> DirectoryImpl<'a, M, T> {
    fn append_directory(&mut self, new_entry: Entry) -> Result<()> {
        self.stream.write(&[1, new_entry.kind as u8])?;
        self.stream
            .write(&(new_entry.primary_offset - self.offset).to_le_bytes())?;
        self.stream.write(&new_entry.primary_size.to_le_bytes())?;
        self.stream.write(
            new_entry
                .name
                .bytes()
                .chain(std::iter::once(0))
                .collect::<Vec<_>>()
                .as_ref(),
        )?;
        self.entries.push(new_entry);
        Ok(())
    }
}

impl<'a, T: Read + Write + Seek + 'a> Directory<'a, ReadWrite, T> {
    pub fn create_blob<'b>(
        &'b mut self,
        name: &str,
        size: usize,
    ) -> Result<Blob<'a, ReadWrite, T>> {
        let mut inner = self
            .0
            .write()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        let mut file = inner.stream.clone_underlying_file();
        let offset = file.reserve_block(size)?;
        inner.append_directory(Entry {
            kind: EntryKind::Blob,
            primary_offset: offset,
            primary_size: size,
            name: name.to_string(),
        })?;
        Ok(Blob::new(file, offset, size))
    }

    pub fn create_directory<'b>(&'b mut self, name: &str) -> Result<Directory<'a, ReadWrite, T>>
    where
        T: Send,
    {
        let file = {
            let mut parent_file = self
                .0
                .read()
                .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?
                .stream
                .clone_underlying_file();
            let dir_addr = parent_file.size()?;
            let parent_directory = self.clone();
            let name = name.to_string();
            parent_file.clone().lock(Box::new(move || {
                let kind = EntryKind::SubDir;
                let primary_offset = dir_addr;
                let primary_size = (parent_file.size().unwrap() - dir_addr) as usize;
                let mut inner = parent_directory.0.write().unwrap();
                let entry = Entry {
                    kind,
                    primary_offset,
                    primary_size,
                    name,
                };
                inner.append_directory(entry).unwrap();
            }))?
        };
        let stream = Stream::create_rw(file, Self::INIT_BLOCK_SIZE)?;
        let entries = vec![];
        Ok(Directory(Arc::new(RwLock::new(DirectoryImpl {
            offset: stream.get_frame_offset().unwrap(),
            entries,
            stream,
        }))))
    }
    pub fn create_stream<'b>(
        &'b mut self,
        name: &str,
        frame_size: usize,
    ) -> Result<Stream<'a, ReadWrite, T>> {
        let mut inner = self
            .0
            .write()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        let file = inner.stream.clone_underlying_file();
        let stream = Stream::create_rw(file, frame_size)?;
        inner.append_directory(Entry {
            kind: EntryKind::Stream,
            primary_offset: stream.get_frame_offset().unwrap(),
            primary_size: stream.get_frame_size().unwrap(),
            name: name.to_string(),
        })?;
        Ok(stream)
    }
}
impl<'a> Directory<'a, ReadWrite, File> {
    #[cfg(feature = "mapped_io")]
    pub fn copy_directory_from_file<T: Read + Seek>(
        &mut self,
        name: &str,
        mut source: T,
        offset: u64,
        size: usize,
    ) -> Result<()> {
        let mut inner = self
            .0
            .write()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        let mut file = inner.stream.clone_underlying_file();
        let dest_offset = file.reserve_block(size)?;
        inner.append_directory(Entry {
            kind: EntryKind::SubDir,
            primary_offset: dest_offset,
            primary_size: size,
            name: name.to_string(),
        })?;
        let mut object = Blob::new(file, dest_offset, size);
        let mut object_data = object.mmap_mut()?;
        source.seek(SeekFrom::Start(offset))?;
        source.read_exact(object_data.as_mut())
    }
}

impl<'a> Directory<'a, ReadOnly, File> {
    #[cfg(feature = "mapped_io")]
    pub fn map_directory(&self, name: &str) -> Result<MappedDirectory> {
        let inner = self
            .0
            .read()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        if let Some(entry) = inner
            .entries
            .iter()
            .find(|e| e.name == name && e.kind == EntryKind::SubDir)
        {
            return MappedDirectory::new(
                inner.stream.clone_underlying_file(),
                entry.primary_offset,
                entry.primary_size,
            );
        }

        Err(Error::new(ErrorKind::Other, "Directory not found"))
    }
}

pub enum OpenResult<'a, T: Read + Seek + 'a> {
    Blob(Blob<'a, ReadOnly, T>),
    Stream(Stream<'a, ReadOnly, T>),
    SubDir(Directory<'a, ReadOnly, T>),
}

impl<'a, T: Read + Seek + 'a> Directory<'a, ReadOnly, T> {
    pub fn open_blob<'b>(&'b self, name: &str) -> Result<Blob<'a, ReadOnly, T>> {
        let inner = self
            .0
            .read()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        if let Some(entry) = inner
            .entries
            .iter()
            .find(|e| e.name == name && e.kind == EntryKind::Blob)
        {
            let file = inner.stream.clone_underlying_file();
            return Ok(Blob::new(file, entry.primary_offset, entry.primary_size));
        }
        Err(Error::new(ErrorKind::Other, "Chunk not found"))
    }
    pub fn open_stream<'b>(&'b self, name: &str) -> Result<Stream<'a, ReadOnly, T>> {
        let inner = self
            .0
            .read()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        if let Some(entry) = inner
            .entries
            .iter()
            .find(|e| e.name == name && e.kind == EntryKind::Stream)
        {
            let file = inner.stream.clone_underlying_file();
            return Stream::open_ro(file, (entry.primary_offset, entry.primary_size));
        }
        Err(Error::new(ErrorKind::Other, "Stream not found"))
    }

    pub fn open_directory<'b>(&'b self, name: &str) -> Result<Directory<'a, ReadOnly, T>> {
        let inner = self
            .0
            .read()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        if let Some(entry) = inner
            .entries
            .iter()
            .find(|e| e.name == name && e.kind == EntryKind::SubDir)
        {
            let file = inner.stream.clone_underlying_file();
            return Directory::open_directory_impl(file, entry.primary_offset);
        }
        Err(Error::new(ErrorKind::Other, "Stream not found"))
    }

    pub fn open<'b, P: AsRef<Path>>(&'b self, path: P) -> Result<OpenResult<'a, T>> {
        let path = path.as_ref();
        let n_comp = path.components().count();
        let mut cur_dir = self.clone();
        if n_comp == 0 {
            return Ok(OpenResult::SubDir(self.clone()));
        }
        for (idx, comp) in path.components().enumerate() {
            let comp = match comp {
                Component::Normal(name) => name.to_string_lossy().to_owned(),
                _ => continue,
            };
            if idx < n_comp - 1 {
                cur_dir = cur_dir.open_directory(&comp)?
            } else {
                match cur_dir.entries().into_iter().find(|e| e.name == comp) {
                    Some(Entry {
                        kind: EntryKind::Blob,
                        ..
                    }) => {
                        return cur_dir.open_blob(&comp).map(OpenResult::Blob);
                    }
                    Some(Entry {
                        kind: EntryKind::Stream,
                        ..
                    }) => {
                        return cur_dir.open_stream(&comp).map(OpenResult::Stream);
                    }
                    Some(Entry {
                        kind: EntryKind::SubDir,
                        ..
                    }) => {
                        return cur_dir.open_directory(&comp).map(OpenResult::SubDir);
                    }
                    None => {
                        return Err(Error::new(ErrorKind::Other, "Object not found"));
                    }
                }
            }
        }
        Err(Error::new(ErrorKind::Other, "Invalid path"))
    }

    fn recurse_impl<Handle: FnMut(&Path, EntryKind) -> bool>(
        &self,
        handle: &mut Handle,
        prefix: &mut PathBuf,
    ) -> bool {
        for Entry { name, kind, .. } in self.entries() {
            prefix.push(&name);
            if !handle(prefix.as_path(), kind) {
                prefix.pop();
                return false;
            }

            if kind == EntryKind::SubDir {
                if let Ok(subdir) = self.open_directory(&name) {
                    if subdir.recurse_impl(handle, prefix) {
                        return true;
                    }
                }
            }

            prefix.pop();
        }
        true
    }

    pub fn recurse<Handle: FnMut(&Path, EntryKind) -> bool>(&self, mut handle: Handle) {
        self.recurse_impl(&mut handle, &mut Default::default());
    }

    fn find_first_object_impl(&self, name: &str, prefix: &mut PathBuf) -> bool {
        let entries = self.entries();
        if entries
            .iter()
            .any(|Entry { name: ent_name, .. }| name == ent_name)
        {
            prefix.push(name);
            return true;
        }
        for Entry {
            name: subdir_name, ..
        } in entries.into_iter().filter(|e| e.kind == EntryKind::SubDir)
        {
            if let Ok(subdir) = self.open_directory(&subdir_name) {
                prefix.push(subdir_name);
                if subdir.find_first_object_impl(name, prefix) {
                    return true;
                }
                prefix.pop();
            }
        }
        false
    }

    pub fn find_first_object(&self, name: &str) -> Option<PathBuf> {
        let mut ret = PathBuf::default();
        if self.find_first_object_impl(name, &mut ret) {
            Some(ret)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::{Cursor, Result};
    #[test]
    fn test_send_traits() {
        fn check_sync<T: Send>() {}
        check_sync::<Entry>();
        check_sync::<DirectoryImpl<'static, ReadWrite, std::fs::File>>();
        check_sync::<RwLock<DirectoryImpl<'static, ReadWrite, std::fs::File>>>();
    }
    #[test]
    fn test_create_stream() -> Result<()> {
        let mut buf = vec![];
        {
            let cursor = Cursor::new(&mut buf);
            let mut dir = Directory::make_root(cursor)?;
            let mut stream1 = dir.create_stream("test_stream_1", 128)?;
            let mut stream2 = dir.create_stream("test_stream_2", 128)?;
            stream1.write(b"This is the data from the first stream")?;
            stream2.write(b"This is the data from the second stream")?;
        }
        {
            let cursor = Cursor::new(&buf);
            let dir = Directory::open_root(cursor, 0)?;
            let mut first = dir.open_stream("test_stream_1")?;
            let mut data = [0; 128];
            let size = first.read(&mut data)?;
            let result = &data[..size.min(38)];
            assert_eq!(result, &b"This is the data from the first stream"[..]);
        }
        Ok(())
    }
    #[test]
    fn test_stream_cluster() -> Result<()> {
        let mut buf = vec![];
        {
            let cursor = Cursor::new(&mut buf);
            let mut dir = Directory::make_root(cursor)?;
            let mut stream1 = dir.create_stream("test_stream_1", 128)?;
            stream1.write(b"This is a testing block")?;
            stream1.flush()?;
            stream1.write(b"This is a testing block")?;
            {
                let mut cluster1 = dir.create_directory("test_cluster")?;
                let mut cs1 = cluster1.create_stream("clustered_stream_1", 128)?;
                let mut cs2 = cluster1.create_stream("clustered_stream_2", 128)?;
                cs1.write(b"cluster test 1234")?;
                cs2.write(b"hahahaha")?;
                stream1.write(b"test").ok();
                stream1.flush().expect_err("Should be error");
            }
            stream1.write(b"test")?;
            stream1.flush()?;
        }
        {
            let cursor = Cursor::new(&buf);
            let dir = Directory::open_root(cursor, 0)?;
            assert_eq!(dir.0.read().unwrap().entries.len(), 2);
            let cluster = dir.open_directory("test_cluster")?;
            let mut test = cluster.open_stream("clustered_stream_1")?;
            let mut buf = [0; 4];
            test.read(&mut buf[..])?;
            assert_eq!(&buf, b"clus");
        }
        Ok(())
    }
}
