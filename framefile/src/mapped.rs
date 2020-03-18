use crate::mode::ReadOnly;
use crate::randfile::mapping::MappingHandle;
use crate::randfile::RandFile;
use crate::stream::FrameHeader;
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
        if let Some(offset) = self.header.linked_frame {
            Some(unsafe {
                std::mem::transmute((
                    (&self.header as *const FrameHeader as *const u8)
                        .offset(i64::from(offset) as isize),
                    self.header.linked_frame_size - std::mem::size_of::<FrameHeader>() as u64,
                ))
            })
        } else {
            None
        }
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
        while !cursor.is_empty() && cursor[0] > 0 {
            let kind_code = cursor[1];
            let mut offset = [0u8; 8];
            let mut size = [0u8; 8];
            offset[..].copy_from_slice(&cursor[2..10]);
            size[..].copy_from_slice(&cursor[10..18]);
            let offset = u64::from_le_bytes(offset);
            let size: usize = u64::from_le_bytes(size) as usize;
            let name = unsafe { std::ffi::CStr::from_ptr(&cursor[18] as *const u8 as *const i8) };
            cursor = &cursor[19 + name.to_bytes().len()..];
            let name = name.to_string_lossy().to_string();
            if kind_code == crate::directory::EntryKind::VariantLengthStream as u8 {
                dir_table.insert(name, (&data[offset as usize] as *const u8 as usize, size));
            }
        }
        Ok(MappedDirectory {
            streams: dir_table,
            _handle: handle,
        })
    }
}
