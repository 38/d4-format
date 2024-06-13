use crate::randfile::mapping::MappingHandle;
use crate::randfile::RandFile;
use crate::stream::FrameHeader;
use crate::{Directory, EntryKind};
use std::collections::HashMap;
use std::fs::File;
use std::io::Result;

/// A memory mapped data structure for a directory object in frame file
pub struct MappedDirectory {
    streams: HashMap<String, (usize, usize)>,
    handle: MappingHandle,
}

/// A memory mapped stream object
pub struct MappedStream<'a>(&'a u8, usize);

/// A single stream frame in a mapped stream object
#[repr(packed)]
pub struct MappedStreamFrame {
    header: FrameHeader,
    pub data: [u8],
}

impl AsRef<[u8]> for MappedStreamFrame {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl<'a> MappedStream<'a> {
    pub(crate) fn new(data: &'a u8, primary_size: usize) -> Self {
        MappedStream(data, primary_size)
    }

    /// Get the first frame in this stream
    pub fn get_primary_frame(&self) -> &'a MappedStreamFrame {
        unsafe { std::mem::transmute((self.0, self.1 - std::mem::size_of::<FrameHeader>())) }
    }

    /// Copy the full content of the stream
    pub fn copy_content(&self, buf: &mut Vec<u8>) {
        //let mut ret = Vec::<u8>::new();
        let mut current = Some(self.get_primary_frame());
        while let Some(frame) = current {
            buf.extend_from_slice(&frame.data);
            current = frame.next_frame();
        }
    }
}

impl MappedStreamFrame {
    #[allow(clippy::missing_safety_doc)]
    pub unsafe fn offset_from(&self, base: *const u8) -> isize {
        (self as *const _ as *const u8).offset_from(base)
    }
    pub fn next_frame(&self) -> Option<&MappedStreamFrame> {
        self.header.linked_frame.map(|offset| {
            let payload_size =
                self.header.linked_frame_size.to_le() - std::mem::size_of::<FrameHeader>() as u64;
            let next_frame_addr = unsafe {
                (self as *const _ as *const u8).offset(i64::from(offset).to_le() as isize)
            };
            // Actually, this is the only reliable way to construct a custom DST reference from raw memory address
            // It's not obvious and hacky:
            // - First, we use `std::ptr::slice_from_raw_parts` that make a DST fat pointer by introducing size
            //   information to the pointer. At this point, the pointer doesn't actually points to the correct memory region.
            //   Because returned *const [u8] actually describes memory region from frame_addr to frame_addr + payload_size
            //   thus, there are exactly sizeof(Header) bytes of payload data is out of the range. But this doesn't matter.
            // - Then, we cast the pointer to *const MappedStreamFrame, which is a DST pointer as well. But this time, the
            //   pointer will be interpreted correctly, as it points to the correct location, plus it carries correct size info.
            // Thus the purpose of calling slice_from_raw_parts is not yield a well-formed pointer, but a compiler supported
            // way to introduce size information to a thin pointer. After that we then cast the pointer make sure that the
            // pointer can be dereferenced correctly.
            // See https://github.com/rust-lang/unsafe-code-guidelines/issues/288 for details
            let frame_ptr = std::ptr::slice_from_raw_parts(next_frame_addr, payload_size as usize)
                as *const Self;
            unsafe { &*frame_ptr }
        })
    }
}

impl MappedDirectory {
    pub fn get_base_addr(&self) -> *const u8 {
        self.handle.as_ref().as_ptr()
    }
    pub fn open_stream(&self, name: &str) -> Option<MappedStream<'_>> {
        if let Some((ptr, size)) = self.streams.get(name) {
            Some(unsafe { MappedStream::new(std::mem::transmute(*ptr), *size) })
        } else {
            None
        }
    }
    pub fn new(file: RandFile<File>, offset: u64, size: usize) -> Result<Self> {
        let handle = file.mmap(offset, size)?;
        let data = handle.as_ref();
        let root_stream = MappedStream::new(
            &data[0],
            crate::directory::Directory::<File>::INIT_BLOCK_SIZE,
        );
        let mut content = Vec::new();
        root_stream.copy_content(&mut content);

        let mut dir_table = HashMap::new();
        let mut cursor = &content[..];
        while let Some(entry) = Directory::<File>::read_next_entry(0, &mut cursor)? {
            if entry.kind == EntryKind::Stream {
                dir_table.insert(
                    entry.name,
                    (
                        &data[entry.primary_offset as usize] as *const u8 as usize,
                        entry.primary_size as usize,
                    ),
                );
            }
        }
        Ok(MappedDirectory {
            streams: dir_table,
            handle,
        })
    }
}
