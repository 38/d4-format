use crate::mode::{AccessMode, CanRead, CanWrite, ReadOnly, ReadWrite};
use crate::randfile::RandFile;

use std::io::{Read, Result, Seek, Write};
use std::marker::PhantomData;
use std::num::NonZeroI64;

#[repr(packed)]
#[derive(Default)]
pub(crate) struct FrameHeader {
    pub(crate) linked_frame: Option<NonZeroI64>,
    /// Since we have maximum frame size 4096 plus usize doesn't have same size for
    /// different arch, so we just use a u32 for the size of the frame
    pub(crate) linked_frame_size: u64,
}

/// The frame describes a single block in each stream
#[derive(Default)]
struct Frame {
    header: FrameHeader,
    /// The offset from the beginning of the file to the head of this frame
    offset: Option<u64>,
    /// The offset for it's parent frame
    parent_frame: Option<u64>,
    /// The size of the current frame
    current_frame_size: usize,
    /// The flag indicates if the buffer is dirty
    dirty: bool,
    /// The offset from the start of the frame to the payload
    payload_offset: usize,
    /// The actual data block
    data: Vec<u8>,
}

impl Frame {
    fn update_frame_link<M: CanWrite<W>, W: Seek + Write>(
        &mut self,
        file: &mut RandFile<M, W>,
        offset: u64,
        size: usize,
    ) -> Result<()> {
        if let Some(parent_frame) = self.parent_frame {
            let mut buf = [0u8; std::mem::size_of::<FrameHeader>()];
            buf[0..8].clone_from_slice(&(offset as i64 - parent_frame as i64).to_le_bytes());
            buf[8..16].clone_from_slice(&(size as u64).to_le_bytes());
            file.update_block(parent_frame, &buf)
        } else {
            Ok(())
        }
    }

    fn sync_current_frame<M: CanWrite<W>, W: Seek + Write>(
        &mut self,
        file: &mut RandFile<M, W>,
    ) -> Result<()> {
        if self.dirty == false {
            return Ok(());
        }

        if let Some(offset) = self.offset {
            file.update_block(offset, &self.data)?;
        } else {
            let offset = file.append_block(&self.data)?;
            self.offset = Some(offset);
            self.update_frame_link(file, offset, self.current_frame_size)?;
        };
        Ok(())
    }

    fn reserve_frame<M: CanWrite<W>, W: Write + Seek>(
        &mut self,
        file: &mut RandFile<M, W>,
        size: usize,
    ) -> Result<()> {
        if self.offset.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Invalid reservation",
            ));
        }

        let offset = file.reserve_block(size)?;
        self.offset = Some(offset);
        self.current_frame_size = size;
        self.update_frame_link(file, offset, size)?;
        Ok(())
    }

    fn zero_frame(&mut self) {
        self.data[self.payload_offset..]
            .iter_mut()
            .for_each(|m| *m = 0);
        self.data.resize(self.current_frame_size, 0);
    }

    fn alloc_new_frame<M: CanWrite<W>, W: Seek + Write>(
        this: Option<Self>,
        file: &mut RandFile<M, W>,
        reserve: usize,
    ) -> Result<Self> {
        let (mut ret, parent) = if let Some(mut current) = this {
            current.sync_current_frame(file)?;
            let parent = current.offset;
            (current, parent)
        } else {
            (Self::default(), None)
        };
        ret.header.linked_frame = None;
        ret.header.linked_frame_size = 0;
        ret.parent_frame = parent;
        ret.offset = None;
        ret.current_frame_size = std::mem::size_of::<FrameHeader>();
        ret.dirty = true;
        ret.payload_offset = std::mem::size_of::<FrameHeader>();
        ret.data.resize(std::mem::size_of::<FrameHeader>(), 0);
        if reserve > 0 {
            ret.reserve_frame(file, reserve)?;
        }
        Ok(ret)
    }

    fn load_from_file<M: CanRead<R>, R: Seek + Read>(
        file: &mut RandFile<M, R>,
        offset: u64,
        size: usize,
        read_payload: bool,
        buf: Option<Self>,
    ) -> Result<Self> {
        let bytes_to_read = if !read_payload {
            std::mem::size_of::<FrameHeader>()
        } else {
            size
        };

        let mut ret = if let Some(buf) = buf {
            buf
        } else {
            Self::default()
        };

        ret.data.resize(bytes_to_read, 0);
        if ret.data.len() != file.read_block(offset, &mut ret.data[..])? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Invalid frame size",
            ));
        }

        let mut linked_frame_buf = [0u8; 8];
        let mut linked_frame_size_buf = [0u8; 8];

        linked_frame_buf.clone_from_slice(&ret.data[..8]);
        linked_frame_size_buf.clone_from_slice(&ret.data[8..16]);

        ret.header.linked_frame = NonZeroI64::new(i64::from_le_bytes(linked_frame_buf));
        ret.header.linked_frame_size = u64::from_le_bytes(linked_frame_size_buf);
        ret.dirty = false;
        ret.payload_offset = std::mem::size_of::<FrameHeader>();
        ret.current_frame_size = size;
        ret.parent_frame = ret.offset;
        ret.offset = Some(offset);

        Ok(ret)
    }

    #[allow(dead_code)]
    fn load_next_frame<M: CanRead<R>, R: Seek + Read>(
        self,
        file: &mut RandFile<M, R>,
        read_payload: bool,
    ) -> Result<Option<Self>> {
        if let Some(offset) = self.offset {
            if let Some(rel_addr) = self.header.linked_frame.map(i64::from) {
                let size = self.header.linked_frame_size as usize;
                let addr = (offset as i64 + rel_addr) as u64;
                return Self::load_from_file(file, addr, size, read_payload, Some(self)).map(Some);
            }
        }
        Ok(None)
    }
}

pub struct Stream<'a, Mode: AccessMode, T: 'a> {
    file: RandFile<'a, Mode, T>,
    current_frame: Option<Frame>,
    cursor: usize,
    frame_size: usize,
    pre_alloc: bool,
    on_drop: Box<dyn FnOnce(&mut Self) + Send + Sync>,
    _phantom: PhantomData<Mode>,
}
impl<'a, M: AccessMode, T: 'a> Stream<'a, M, T> {
    pub fn double_frame_size(&mut self, limit: usize) {
        if self.frame_size * 2 > limit {
            self.frame_size = limit;
            return;
        }
        self.frame_size *= 2;
    }
    pub(crate) fn clone_underlying_file<'b>(&'b self) -> RandFile<'a, M, T> {
        self.file.clone()
    }

    pub(crate) fn get_frame_offset(&self) -> Option<u64> {
        self.current_frame
            .as_ref()
            .map_or(None, |frame| frame.offset)
    }

    pub(crate) fn get_frame_size(&self) -> Option<usize> {
        self.current_frame
            .as_ref()
            .map(|frame| frame.current_frame_size)
    }

    pub fn get_frame_capacity(&self) -> usize {
        self.frame_size - std::mem::size_of::<FrameHeader>()
    }
}
impl<M: CanWrite<T>, T: Write + Seek> Stream<'_, M, T> {
    pub fn flush(&mut self) -> Result<()> {
        let current_frame = std::mem::replace(&mut self.current_frame, None);
        self.current_frame = Some(Frame::alloc_new_frame(current_frame, &mut self.file, 0)?);
        self.cursor = 0;
        Ok(())
    }

    pub fn write(&mut self, buffer: &[u8]) -> Result<usize> {
        self.write_with_alloc_callback(buffer, |_| ())
    }

    pub fn disable_pre_alloc(&mut self) {
        self.pre_alloc = false;
    }

    pub fn write_frame(&mut self, buffer: &[u8]) -> Result<()> {
        self.flush()?;
        if let Some(frame) = self.current_frame.as_mut() {
            frame.data.extend_from_slice(buffer);
            frame.current_frame_size = frame.data.len();
        }
        Ok(())
    }

    /// Append data to the given stream. Similar to write, but this allows to inject a callback function,
    /// which you can modify the configuration of the stream before the stream actually synced to the file.
    pub fn write_with_alloc_callback<R: FnMut(&mut Self)>(
        &mut self,
        buffer: &[u8],
        mut callback: R,
    ) -> Result<usize> {
        let mut ret = 0;
        let mut ptr = buffer;
        while ptr.len() > 0 {
            // First, let's determine the size we can write for this iteration
            let bytes_can_write = if self
                .current_frame
                .as_ref()
                .map_or(false, |s| s.offset.is_some())
            {
                // If we are actually writing some block that is backend in the target file,
                // we are limited by the size of current frame
                self.current_frame
                    .as_ref()
                    .map_or(0, |f| f.current_frame_size - f.payload_offset - self.cursor)
                    .min(ptr.len())
            } else {
                // Otherwise we are free to extend the frame size if the frame size limit is unspecified
                if self.frame_size > 0 {
                    self.current_frame
                        .as_ref()
                        .map_or(0, |f| self.frame_size - f.payload_offset - self.cursor)
                        .min(ptr.len())
                } else {
                    ptr.len()
                }
            };

            if bytes_can_write == 0 {
                callback(self);
                let current_frame = std::mem::replace(&mut self.current_frame, None);
                self.current_frame =
                    Some(Frame::alloc_new_frame(current_frame, &mut self.file, 0)?);
                if self.frame_size > 0 && self.pre_alloc {
                    let frame = self.current_frame.as_mut().unwrap();
                    frame.reserve_frame(&mut self.file, self.frame_size)?;
                    frame.zero_frame();
                }
                self.cursor = 0;
                continue;
            }
            let cursor = self.cursor;
            self.current_frame.as_mut().map(|frame| {
                let start = frame.payload_offset + cursor;
                let end = start + bytes_can_write;
                if frame.data.len() < end {
                    frame.data.resize(end, 0);
                }
                frame.data[start..end].copy_from_slice(&ptr[..bytes_can_write]);
                frame.current_frame_size = frame.current_frame_size.max(end);
            });
            ptr = &ptr[bytes_can_write..];
            self.cursor += bytes_can_write;
            ret += bytes_can_write;
        }
        Ok(ret)
    }
}
impl<M: CanRead<T>, T: Read + Seek> Stream<'_, M, T> {
    pub fn read(&mut self, buffer: &mut [u8]) -> Result<usize> {
        let mut ret = 0;
        let mut ptr = buffer;
        while self.current_frame.is_some() && !ptr.is_empty() {
            let bytes_read = {
                let can_read = self
                    .current_frame
                    .as_ref()
                    .map_or(0, |f| f.data.len() - f.payload_offset)
                    .max(self.cursor)
                    - self.cursor;
                if can_read == 0 {
                    let this_frame = std::mem::replace(&mut self.current_frame, None);
                    self.current_frame =
                        this_frame.unwrap().load_next_frame(&mut self.file, true)?;
                    self.cursor = 0;
                    continue;
                }
                can_read
            }
            .min(ptr.len());
            ptr[..bytes_read].copy_from_slice(
                self.current_frame
                    .as_ref()
                    .map(|f| {
                        let start = f.payload_offset + self.cursor;
                        let end = start + bytes_read;
                        &f.data[start..end]
                    })
                    .unwrap(),
            );
            ret += bytes_read;
            ptr = &mut ptr[bytes_read..];
            self.cursor += bytes_read;
        }
        Ok(ret)
    }
}
impl<'a, T: Read + Seek> Stream<'a, ReadOnly, T> {
    #[allow(dead_code)]
    pub(crate) fn open_ro(
        mut file: RandFile<'a, ReadOnly, T>,
        primary_frame: (u64, usize),
    ) -> Result<Self> {
        let current_frame = Some(Frame::load_from_file(
            &mut file,
            primary_frame.0,
            primary_frame.1,
            true,
            None,
        )?);
        Ok(Self {
            file,
            current_frame,
            cursor: 0,
            frame_size: 0,
            on_drop: Box::new(|_| {}),
            pre_alloc: true,
            _phantom: PhantomData,
        })
    }
}

impl<'a, T: Read + Write + Seek> Stream<'a, ReadWrite, T> {
    pub(crate) fn create_rw(
        mut file: RandFile<'a, ReadWrite, T>,
        frame_size: usize,
    ) -> Result<Self> {
        let current_frame = Some(Frame::alloc_new_frame(None, &mut file, frame_size)?);
        Ok(Self {
            file,
            current_frame,
            cursor: 0,
            frame_size: frame_size,
            on_drop: Box::new(|this| {
                this.flush().unwrap();
            }),
            pre_alloc: true,
            _phantom: PhantomData,
        })
    }
}

impl<M: AccessMode, T> Drop for Stream<'_, M, T> {
    fn drop(&mut self) {
        let drop_callback = std::mem::replace(&mut self.on_drop, Box::new(|_| {}));
        drop_callback(self);
    }
}

#[cfg(test)]
mod test {
    use super::Stream;
    use crate::randfile::RandFile;
    use std::io::Cursor;
    type TestResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;
    #[test]
    fn test_stream_send() {
        fn check_send<T: Send>() {}
        check_send::<Stream<'static, crate::mode::ReadWrite, std::fs::File>>();
    }
    #[test]
    fn test_compose_stream() -> TestResult<()> {
        let mut buffer = vec![];
        {
            let fp = Cursor::new(&mut buffer);
            let file = RandFile::for_read_write(fp);

            let mut stream = Stream::create_rw(file.clone(), 0)?;
            let mut stream2 = Stream::create_rw(file, 0)?;

            stream.write(b"This is a test frame")?;
            stream2.write(b"This is another stream")?;

            stream.flush()?;

            stream.write(b"This is the second block")?;
            stream2.write(b"This is another stream - 2")?;
            stream2.flush()?;
            stream.flush()?;
        }

        {
            let fp = Cursor::new(&mut buffer);
            let file = RandFile::for_read_only(fp);
            let mut stream = Stream::open_ro(file, (0, 30))?;
            let mut data = [0; 100];
            assert_eq!(38, stream.read(&mut data)?);
        }

        Ok(())
    }
    #[test]
    fn test_traverse_file() -> TestResult<()> {
        let test_blob: Vec<_> = vec![
            19, 0, 0, 0, 0, 0, 0, 0, //Linked Frame
            20, 0, 0, 0, 0, 0, 0, 0, // Linked Frame size
            0xdd, 0xdd, 0xdd, // Frame data
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let reader = Cursor::new(test_blob);
        let file = RandFile::for_read_only(reader);

        let mut stream = Stream::open_ro(file, (0, 19))?;

        let mut buffer = vec![0; 100];

        assert_eq!(7, stream.read(&mut buffer)?);

        Ok(())
    }
}
