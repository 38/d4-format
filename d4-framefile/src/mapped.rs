use crate::mode::ReadOnly;
use crate::randfile::mapping::MappingHandle;
use crate::randfile::RandFile;
use crate::stream::FrameHeader;
use crate::{Directory, EntryKind};
use std::collections::HashMap;
use std::fs::File;
use std::io::Result;

pub struct MappedDirectory {
    streams: HashMap<String, (usize, usize)>,
    _handle: MappingHandle,
}

pub struct MappedStream<'a>(&'a u8, usize);

#[repr(packed)]
pub struct MappedStreamFrame {
    header: FrameHeader,
    pub data: [u8],
}

impl<'a> MappedStream<'a> {
    pub(crate) fn new(data: &'a u8, primary_size: usize) -> Self {
        MappedStream(data, primary_size)
    }

    pub fn get_primary_frame(&self) -> &MappedStreamFrame {
        unsafe { std::mem::transmute((self.0, self.1 - std::mem::size_of::<FrameHeader>())) }
    }

    pub fn copy_content(&self) -> Vec<u8> {
        let mut ret = vec![];
        let mut current = Some(self.get_primary_frame());
        while let Some(frame) = current {
            ret.extend_from_slice(&frame.data);
            current = frame.next_frame();
        }
        ret
    }
}

impl MappedStreamFrame {
    pub fn next_frame(&self) -> Option<&MappedStreamFrame> {
        self.header.linked_frame.map(|offset| unsafe {
            std::mem::transmute((
                (&self.header as *const FrameHeader as *const u8)
                    .offset(i64::from(offset) as isize),
                self.header.linked_frame_size - std::mem::size_of::<FrameHeader>() as u64,
            ))
        })
    }
}

impl MappedDirectory {
    pub fn get(&self, name: &str) -> Option<MappedStream> {
        if let Some((ptr, size)) = self.streams.get(name) {
            Some(unsafe { MappedStream::new(std::mem::transmute(*ptr), *size) })
        } else {
            None
        }
    }
    pub fn new(file: RandFile<ReadOnly, File>, offset: u64, size: usize) -> Result<Self> {
        let handle = file.mmap(offset, size)?;
        let data = handle.as_ref();
        let root_stream = MappedStream::new(
            &data[0],
            crate::directory::Directory::<ReadOnly, File>::INIT_BLOCK_SIZE,
        );
        let content = root_stream.copy_content();
        // TODO: consider reuse the directory parsing code
        let mut dir_table = HashMap::new();
        let mut cursor = &content[..];
        while let Some(entry) = Directory::<ReadOnly, File>::read_next_entry(0, &mut cursor)? {
            if entry.kind == EntryKind::Stream {
                dir_table.insert(
                    entry.name,
                    (
                        &data[entry.primary_offset as usize] as *const u8 as usize,
                        entry.primary_size,
                    ),
                );
            }
        }
        Ok(MappedDirectory {
            streams: dir_table,
            _handle: handle,
        })
    }
}
