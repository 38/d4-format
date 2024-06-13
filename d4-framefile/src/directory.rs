#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
use crate::mapped::MappedDirectory;
use crate::randfile::RandFile;
use crate::stream::Stream;
use crate::Blob;
use std::io::{Error, ErrorKind, Read, Result, Seek, Write};

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
use std::{fs::File, io::SeekFrom};

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
    pub primary_size: u64,
    /// The name of the stream
    pub name: String,
}

struct DirectoryImpl<T> {
    offset: u64,
    entries: Vec<Entry>,
    stream: Stream<T>,
}

pub enum OpenResult<T: Read + Seek> {
    Blob(Blob<T>),
    Stream(Stream<T>),
    SubDir(Directory<T>),
}

impl<T: Read + Write + Seek> DirectoryImpl<T> {
    fn write_stream(&mut self, data: &[u8]) -> Result<usize> {
        self.stream.write_with_alloc_callback(data, |s| {
            s.double_frame_size(65536);
        })
    }
    fn append_directory(&mut self, new_entry: Entry) -> Result<()> {
        if self.entries.iter().any(|x| x.name == new_entry.name) {
            return Err(Error::new(
                ErrorKind::Other,
                "Directory entry already exists",
            ));
        }
        //self.stream.write(&[1, new_entry.kind as u8])?;
        self.stream.update_current_byte(1)?;
        self.write_stream(&[new_entry.kind as u8])?;
        self.stream
            .write(&(new_entry.primary_offset - self.offset).to_le_bytes())?;
        self.write_stream(&new_entry.primary_size.to_le_bytes())?;
        self.write_stream(
            new_entry
                .name
                .bytes()
                .chain(std::iter::once(0))
                .collect::<Vec<_>>()
                .as_ref(),
        )?;
        self.write_stream(&[0])?;
        self.entries.push(new_entry);
        Ok(())
    }
}

pub struct Directory<T>(Arc<RwLock<DirectoryImpl<T>>>);

impl<T> Clone for Directory<T> {
    fn clone(&self) -> Self {
        Directory(self.0.clone())
    }
}

impl<T> Directory<T> {
    // TODO: For internet accessing, this init block size seems too small.
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

impl<T: Clone> Directory<T> {
    pub fn clone_underlying_file(&self) -> Result<RandFile<T>> {
        let inner = self
            .0
            .read()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        Ok(inner.stream.clone_underlying_file())
    }
}

impl<T: Read + Write + Seek> Directory<T> {
    pub fn make_root(back: T) -> Result<Directory<T>> {
        let randfile = RandFile::new(back);
        let stream = Stream::create(randfile, 512)?;
        let entries = vec![];
        Ok(Directory(Arc::new(RwLock::new(DirectoryImpl {
            offset: stream.get_frame_offset().unwrap(),
            entries,
            stream,
        }))))
    }
    pub fn open_root_for_update(back: T, offset: u64) -> Result<Directory<T>> {
        let randfile = RandFile::new(back);
        Self::open_directory_rw_impl(randfile, offset)
    }

    pub fn flush(&mut self) -> Result<()> {
        let mut inner = self
            .0
            .write()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        inner.stream.flush()
    }

    pub fn create_blob(&mut self, name: &str, size: usize) -> Result<Blob<T>> {
        let mut inner = self
            .0
            .write()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        let mut file = inner.stream.clone_underlying_file();
        let offset = file.reserve_block(size)?;
        inner.append_directory(Entry {
            kind: EntryKind::Blob,
            primary_offset: offset,
            primary_size: size as u64,
            name: name.to_string(),
        })?;
        Ok(Blob::new(file, offset, size))
    }

    pub fn open_or_create_directory(&mut self, name: &str) -> Result<Directory<T>>
    where
        T: Send + 'static,
    {
        if let Ok(dir) = self.open_directory_for_update(name) {
            Ok(dir)
        } else {
            self.create_directory(name)
        }
    }

    pub fn create_directory(&mut self, name: &str) -> Result<Directory<T>>
    where
        T: Send + 'static,
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
                let primary_size = parent_file.size().unwrap() - dir_addr;
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
        let stream = Stream::create(file, Self::INIT_BLOCK_SIZE)?;
        let entries = vec![];
        Ok(Directory(Arc::new(RwLock::new(DirectoryImpl {
            offset: stream.get_frame_offset().unwrap(),
            entries,
            stream,
        }))))
    }
    pub fn create_stream(&mut self, name: &str, frame_size: usize) -> Result<Stream<T>> {
        let mut inner = self
            .0
            .write()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        let file = inner.stream.clone_underlying_file();
        let stream = Stream::create(file, frame_size)?;
        inner.append_directory(Entry {
            kind: EntryKind::Stream,
            primary_offset: stream.get_frame_offset().unwrap(),
            primary_size: stream.get_frame_size().unwrap() as u64,
            name: name.to_string(),
        })?;
        Ok(stream)
    }
    pub fn open_directory_for_update(&self, name: &str) -> Result<Directory<T>> {
        self.open_directory_impl(name, Self::open_directory_rw_impl)
    }
}

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
impl Directory<File> {
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
            primary_size: size as u64,
            name: name.to_string(),
        })?;
        let mut object = Blob::new(file, dest_offset, size);
        let mut object_data = object.mmap_mut()?;
        source.seek(SeekFrom::Start(offset))?;
        source.read_exact(object_data.as_mut())
    }

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
                entry.primary_size as usize,
            );
        }

        Err(Error::new(ErrorKind::Other, "Directory not found"))
    }
}

impl<T: Read + Seek> Directory<T> {
    /// Get a list of children under this directory
    pub fn entries(&self) -> Vec<Entry> {
        self.0.read().unwrap().entries.clone()
    }
    /// Open an root directory from a seek-able backend stream and an **absolute** offset
    pub fn open_root(back: T, offset: u64) -> Result<Directory<T>> {
        let randfile = RandFile::new(back);
        Self::open_directory_ro_impl(randfile, offset)
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
        let size = u64::from_le_bytes(size_buffer);
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
    fn open_directory_with_stream(mut stream: Stream<T>, offset: u64) -> Result<Directory<T>> {
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
    fn open_directory_ro_impl(randfile: RandFile<T>, offset: u64) -> Result<Directory<T>> {
        let stream = Stream::open_for_read(randfile, (offset, Self::INIT_BLOCK_SIZE))?;
        Self::open_directory_with_stream(stream, offset)
    }
    fn open_directory_rw_impl(randfile: RandFile<T>, offset: u64) -> Result<Directory<T>>
    where
        T: Write,
    {
        let stream = Stream::open_for_update(randfile, (offset, Self::INIT_BLOCK_SIZE))?;
        Self::open_directory_with_stream(stream, offset)
    }

    pub fn open_blob(&self, name: &str) -> Result<Blob<T>> {
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
            return Ok(Blob::new(
                file,
                entry.primary_offset,
                entry.primary_size as usize,
            ));
        }
        Err(Error::new(ErrorKind::Other, "Chunk not found"))
    }
    pub fn open_stream_by_offset(&self, offset: u64, frame_size: usize) -> Result<Stream<T>> {
        let inner = self
            .0
            .read()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        let file = inner.stream.clone_underlying_file();
        Stream::open_for_read(file, (offset + inner.offset, frame_size))
    }
    pub fn open_stream(&self, name: &str) -> Result<Stream<T>> {
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
            return Stream::open_for_read(
                file,
                (entry.primary_offset, entry.primary_size as usize),
            );
        }
        Err(Error::new(ErrorKind::Other, "Stream not found"))
    }

    fn open_directory_impl<H: FnOnce(RandFile<T>, u64) -> Result<Directory<T>>>(
        &self,
        name: &str,
        handle: H,
    ) -> Result<Directory<T>> {
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
            return handle(file, entry.primary_offset);
        }
        Err(Error::new(ErrorKind::Other, "Stream not found"))
    }

    pub fn open_directory(&self, name: &str) -> Result<Directory<T>> {
        self.open_directory_impl(name, Self::open_directory_ro_impl)
    }

    pub fn open<P: AsRef<Path>>(&self, path: P) -> Result<OpenResult<T>> {
        let path = path.as_ref();
        let n_comp = path.components().count();
        let mut cur_dir = self.clone();
        if n_comp == 0 {
            return Ok(OpenResult::SubDir(self.clone()));
        }
        for (idx, comp) in path.components().enumerate() {
            let comp = match comp {
                Component::Normal(name) => name.to_string_lossy().clone(),
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
                return true;
            }

            if kind == EntryKind::SubDir {
                if let Ok(subdir) = self.open_directory(&name) {
                    if !subdir.recurse_impl(handle, prefix) {
                        prefix.pop();
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
        check_sync::<DirectoryImpl<std::fs::File>>();
        check_sync::<RwLock<DirectoryImpl<std::fs::File>>>();
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
    fn test_directory_update() -> std::result::Result<(), Box<dyn std::error::Error>> {
        let buf = {
            let cursor = Cursor::new(vec![]);
            let mut dir = Directory::make_root(cursor)?;
            for i in 0..10 {
                let stream_name = format!("test_stream.{}", i);
                let mut test_stream = dir.create_stream(stream_name.as_str(), 32)?;
                test_stream.write("this is a test stream".as_bytes())?;
            }
            dir.flush()?;
            dir.clone_underlying_file()?.clone_inner()?.into_inner()
        };
        let buf = {
            let backend = Cursor::new(buf);
            let mut root = Directory::open_root_for_update(backend, 0)?;
            assert_eq!(root.entries().len(), 10);
            {
                let mut another_dir = root.create_directory("additional_dir")?;
                for i in 0..10 {
                    let stream_name = format!("test_stream.{}", i);
                    let mut test_stream = another_dir.create_stream(stream_name.as_str(), 32)?;
                    test_stream.write("this is a test stream".as_bytes())?;
                }
            }

            root.flush()?;
            root.clone_underlying_file()?.clone_inner()?.into_inner()
        };

        let backend = Cursor::new(buf);
        let root = Directory::open_root(backend, 0)?;
        assert_eq!(root.entries().len(), 11);
        let sub_dir = root.open_directory("additional_dir")?;
        assert_eq!(sub_dir.entries().len(), 10);
        Ok(())
    }
    #[test]
    fn test_stream_cluster() -> Result<()> {
        let buf = {
            let cursor = Cursor::new(vec![]);
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
            dir.flush()?;
            stream1.clone_underlying_file().clone_inner()?
        };
        {
            let dir = Directory::open_root(buf, 0)?;
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
