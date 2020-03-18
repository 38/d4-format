use crate::chunk::Chunk;
use crate::mapped::MappedDirectory;
use crate::mode::{AccessMode, CanWrite, ReadOnly, ReadWrite};
use crate::randfile::RandFile;
use crate::stream::Stream;
use std::io::{Error, ErrorKind, Read, Result, Seek, Write};
use std::sync::{Arc, RwLock};

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum EntryKind {
    VariantLengthStream = 0,
    StreamCluster = 1,
    FixedSized = 2,
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub kind: EntryKind,
    pub primary_offset: u64,
    pub primary_size: usize,
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
    pub fn entries(&self) -> Vec<Entry> {
        self.0.read().unwrap().entries.clone()
    }
    pub fn open_directory(back: T, offset: u64) -> Result<Directory<'a, ReadOnly, T>> {
        let randfile = RandFile::for_read_only(back);
        Self::open_directory_impl(randfile, offset)
    }
    fn open_directory_impl(
        randfile: RandFile<'a, ReadOnly, T>,
        offset: u64,
    ) -> Result<Directory<'a, ReadOnly, T>> {
        let mut stream = Stream::open_ro(randfile, (offset, Self::INIT_BLOCK_SIZE))?;
        let mut entries = vec![];
        loop {
            let mut has_next = [0u8];
            if stream.read(&mut has_next)? != 1 || has_next[0] == 0 {
                break;
            }
            let mut kind_buffer = [0];
            let mut offset_buffer = [0; 8];
            let mut size_buffer = [0; 8];
            if stream.read(&mut kind_buffer)? != 1
                || stream.read(&mut offset_buffer)? != 8
                || stream.read(&mut size_buffer)? != 8
            {
                break;
            }
            let offset = u64::from_le_bytes(offset_buffer) + offset;
            let size = usize::from_le_bytes(size_buffer);
            let mut name = vec![];
            let mut current_byte = [0];
            while stream.read(&mut current_byte)? > 0 {
                if current_byte[0] == 0 {
                    break;
                }
                name.push(current_byte[0]);
            }
            let name = String::from_utf8_lossy(&name[..]).to_string();

            entries.push(Entry {
                kind: match kind_buffer[0] {
                    0 => EntryKind::VariantLengthStream,
                    1 => EntryKind::StreamCluster,
                    2 => EntryKind::FixedSized,
                    _ => return Err(Error::new(ErrorKind::Other, "Invalid directory type code")),
                },
                primary_offset: offset,
                primary_size: size,
                name,
            });
        }

        Ok(Directory(Arc::new(RwLock::new(DirectoryImpl {
            offset,
            entries,
            stream,
        }))))
    }
}
impl<'a, T: Read + Write + Seek + 'a> Directory<'a, ReadWrite, T> {
    pub fn create_directory(back: T) -> Result<Directory<'a, ReadWrite, T>> {
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
    pub fn new_fixed_size_chunk<'b>(
        &'b mut self,
        name: &str,
        size: usize,
    ) -> Result<Chunk<'a, ReadWrite, T>> {
        let mut inner = self
            .0
            .write()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        let mut file = inner.stream.clone_underlying_file();
        let offset = file.reserve_block(size)?;
        inner.append_directory(Entry {
            kind: EntryKind::FixedSized,
            primary_offset: offset,
            primary_size: size,
            name: name.to_string(),
        })?;
        Ok(Chunk::new(file, offset, size))
    }

    pub fn new_stream_cluster<'b>(&'b mut self, name: &str) -> Result<Directory<'a, ReadWrite, T>>
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
                let kind = EntryKind::StreamCluster;
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
        let stream = Stream::create_rw(file, 512)?;
        let entries = vec![];
        Ok(Directory(Arc::new(RwLock::new(DirectoryImpl {
            offset: stream.get_frame_offset().unwrap(),
            entries,
            stream,
        }))))
    }
    pub fn new_variant_length_stream<'b>(
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
            kind: EntryKind::VariantLengthStream,
            primary_offset: stream.get_frame_offset().unwrap(),
            primary_size: stream.get_frame_size().unwrap(),
            name: name.to_string(),
        })?;
        Ok(stream)
    }
}
impl<'a> Directory<'a, ReadOnly, std::fs::File> {
    pub fn map_cluster_ro<'b>(&'b self, name: &str) -> Result<MappedDirectory> {
        let inner = self
            .0
            .read()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        if let Some(entry) = inner
            .entries
            .iter()
            .find(|e| e.name == name && e.kind == EntryKind::StreamCluster)
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
impl<'a, T: Read + Seek + 'a> Directory<'a, ReadOnly, T> {
    pub fn open_chunk_ro<'b>(&'b self, name: &str) -> Result<Chunk<'a, ReadOnly, T>> {
        let inner = self
            .0
            .read()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        if let Some(entry) = inner
            .entries
            .iter()
            .find(|e| e.name == name && e.kind == EntryKind::FixedSized)
        {
            let file = inner.stream.clone_underlying_file();
            return Ok(Chunk::new(file, entry.primary_offset, entry.primary_size));
        }
        Err(Error::new(ErrorKind::Other, "Chunk not found"))
    }
    pub fn open_stream_ro<'b>(&'b self, name: &str) -> Result<Stream<'a, ReadOnly, T>> {
        let inner = self
            .0
            .read()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        if let Some(entry) = inner
            .entries
            .iter()
            .find(|e| e.name == name && e.kind == EntryKind::VariantLengthStream)
        {
            let file = inner.stream.clone_underlying_file();
            return Ok(Stream::open_ro(
                file,
                (entry.primary_offset, entry.primary_size),
            )?);
        }
        Err(Error::new(ErrorKind::Other, "Stream not found"))
    }

    pub fn open_cluster_ro<'b>(&'b self, name: &str) -> Result<Directory<'a, ReadOnly, T>> {
        let inner = self
            .0
            .read()
            .map_err(|_| Error::new(ErrorKind::Other, "Lock Error"))?;
        if let Some(entry) = inner
            .entries
            .iter()
            .find(|e| e.name == name && e.kind == EntryKind::StreamCluster)
        {
            let file = inner.stream.clone_underlying_file();
            return Directory::open_directory_impl(file, entry.primary_offset);
        }
        Err(Error::new(ErrorKind::Other, "Stream not found"))
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
            let mut dir = Directory::create_directory(cursor)?;
            let mut stream1 = dir.new_variant_length_stream("test_stream_1", 128)?;
            let mut stream2 = dir.new_variant_length_stream("test_stream_2", 128)?;
            stream1.write(b"This is the data from the first stream")?;
            stream2.write(b"This is the data from the second stream")?;
        }
        {
            let cursor = Cursor::new(&buf);
            let dir = Directory::open_directory(cursor, 0)?;
            let mut first = dir.open_stream_ro("test_stream_1")?;
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
            let mut dir = Directory::create_directory(cursor)?;
            let mut stream1 = dir.new_variant_length_stream("test_stream_1", 128)?;
            stream1.write(b"This is a testing block")?;
            stream1.flush()?;
            stream1.write(b"This is a testing block")?;
            {
                let mut cluster1 = dir.new_stream_cluster("test_cluster")?;
                let mut cs1 = cluster1.new_variant_length_stream("clustered_stream_1", 128)?;
                let mut cs2 = cluster1.new_variant_length_stream("clustered_stream_2", 128)?;
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
            let dir = Directory::open_directory(cursor, 0)?;
            assert_eq!(dir.0.read().unwrap().entries.len(), 2);
            let cluster = dir.open_cluster_ro("test_cluster")?;
            let mut test = cluster.open_stream_ro("clustered_stream_1")?;
            let mut buf = [0; 4];
            test.read(&mut buf[..])?;
            assert_eq!(&buf, b"clus");
        }
        Ok(())
    }
}
