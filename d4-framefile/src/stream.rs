use crate::randfile::RandFile;

use std::io::{Read, Result, Seek, Write};
use std::num::NonZeroI64;

#[repr(packed)]
#[derive(Default, Clone, Copy)]
pub(crate) struct FrameHeader {
    pub(crate) linked_frame: Option<NonZeroI64>,
    pub(crate) linked_frame_size: u64,
}

impl FrameHeader {
    fn new(relative_offset: i64, frame_size: u64) -> Self {
        Self {
            linked_frame: NonZeroI64::new(relative_offset.to_le()),
            linked_frame_size: frame_size.to_le(),
        }
    }
    fn from_bytes(data: &[u8]) -> FrameHeader {
        assert!(data.len() >= std::mem::size_of::<Self>());
        let data = *unsafe { &*data.as_ptr().cast::<FrameHeader>() };
        let offset = data.linked_frame.map_or(0, |x| x.get().to_le());
        let size = data.linked_frame_size;
        Self::new(offset, size)
    }

    fn as_bytes(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
    }
}

/// The frame is a consecutive data block in any variant length stream.
/// Note this is the in-memory representation of a stream
/// It also have mapped form, please see crate::mapped::MappedStream
#[derive(Default)]
struct Frame {
    header: FrameHeader,
    /// The offset from the beginning of the file to the head of this frame. If this frame haven't flushed to disk yet, this field is None
    /// Once this frame is flushed, this field should be updated to the absolute offset of this frame
    offset: Option<u64>,
    /// The absolute offset for it's parent frame, if this is the first frame of stream, this should be None
    parent_frame: Option<(u64, usize)>,
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
    fn update_frame_link<W: Seek + Write>(
        &mut self,
        file: &mut RandFile<W>,
        offset: u64,
        size: usize,
    ) -> Result<()> {
        if let Some((parent_frame, _parent_size)) = self.parent_frame {
            let new_header = FrameHeader::new(offset as i64 - parent_frame as i64, size as u64);
            file.update_block(parent_frame, new_header.as_bytes())
        } else {
            Ok(())
        }
    }

    fn sync_current_frame<W: Seek + Write>(&mut self, file: &mut RandFile<W>) -> Result<()> {
        if !self.dirty {
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

    fn reserve_frame<W: Write + Seek>(
        &mut self,
        file: &mut RandFile<W>,
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

    fn alloc_new_frame<W: Seek + Write>(
        this: Option<Self>,
        file: &mut RandFile<W>,
        reserve: usize,
    ) -> Result<Self> {
        let (mut ret, parent) = if let Some(mut current) = this {
            current.sync_current_frame(file)?;
            let parent = current.offset.map(|ofs| (ofs, current.current_frame_size));
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

    fn load_from_file<R: Seek + Read>(
        file: &mut RandFile<R>,
        offset: u64,
        size: usize,
        read_payload: bool,
        buf: Option<Self>,
        backward: bool,
    ) -> Result<Self> {
        let bytes_to_read = if !read_payload {
            std::mem::size_of::<FrameHeader>()
        } else {
            size
        };

        let mut ret = buf.unwrap_or_default();

        ret.data.resize(bytes_to_read, 0);
        if ret.data.len() != file.read_block(offset, &mut ret.data[..])? {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Invalid frame size",
            ));
        }

        ret.header = FrameHeader::from_bytes(&ret.data);

        ret.dirty = false;
        ret.payload_offset = std::mem::size_of::<FrameHeader>();
        ret.current_frame_size = size;
        if !backward {
            ret.parent_frame = ret.offset.map(|offset| (offset, ret.current_frame_size));
        } else {
            ret.parent_frame = None;
        }
        ret.offset = Some(offset);

        Ok(ret)
    }

    fn load_next_frame<R: Seek + Read>(
        self,
        file: &mut RandFile<R>,
        read_payload: bool,
    ) -> Result<Option<Self>> {
        if let Some(offset) = self.offset {
            if let Some(rel_addr) = self.header.linked_frame.map(i64::from) {
                let size = self.header.linked_frame_size as usize;
                let addr = (offset as i64 + rel_addr) as u64;
                return Self::load_from_file(file, addr, size, read_payload, Some(self), false)
                    .map(Some);
            }
        }
        Ok(None)
    }

    fn load_previous_frame<R: Seek + Read>(
        self,
        file: &mut RandFile<R>,
        read_payload: bool,
    ) -> Result<Option<Self>> {
        if let Some((parent_ofs, parent_size)) = self.parent_frame {
            return Self::load_from_file(
                file,
                parent_ofs,
                parent_size,
                read_payload,
                Some(self),
                true,
            )
            .map(Some);
        }
        Ok(None)
    }
}

pub struct Stream<T> {
    file: RandFile<T>,
    current_frame: Option<Frame>,
    cursor: usize,
    frame_size: usize,
    pre_alloc: bool,
    on_drop: Box<dyn FnOnce(&mut Self) + Send + Sync>,
}
impl<T> Stream<T> {
    pub fn set_frame_size(&mut self, size: usize) {
        self.frame_size = size;
    }
    pub fn double_frame_size(&mut self, limit: usize) {
        if self.frame_size * 2 > limit {
            self.frame_size = limit;
            return;
        }
        self.frame_size *= 2;
    }
    pub(crate) fn clone_underlying_file(&self) -> RandFile<T> {
        self.file.clone()
    }

    pub(crate) fn get_frame_offset(&self) -> Option<u64> {
        self.current_frame.as_ref().and_then(|frame| frame.offset)
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
impl<T: Read + Write + Seek> Stream<T> {
    pub fn flush(&mut self) -> Result<()> {
        let current_frame = self.current_frame.take();
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
        while !ptr.is_empty() {
            // First, let's determine the size we can write for this iteration
            let bytes_can_write = if self
                .current_frame
                .as_ref()
                .map_or(false, |s| s.offset.is_some())
            {
                // If we are actually writing some block that is backed in the target file,
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
                if let Some(Some(_)) = self.current_frame.as_ref().map(|f| f.header.linked_frame) {
                    let mut current_frame = self.current_frame.take().unwrap();
                    current_frame.sync_current_frame(&mut self.file)?;
                    self.current_frame = current_frame.load_next_frame(&mut self.file, true)?;
                } else {
                    callback(self);
                    let current_frame = self.current_frame.take();
                    self.current_frame =
                        Some(Frame::alloc_new_frame(current_frame, &mut self.file, 0)?);
                    if self.frame_size > 0 && self.pre_alloc {
                        let frame = self.current_frame.as_mut().unwrap();
                        frame.reserve_frame(&mut self.file, self.frame_size)?;
                        frame.zero_frame();
                    }
                }
                self.cursor = 0;
                continue;
            }

            let cursor = self.cursor;
            if let Some(ref mut frame) = self.current_frame {
                let start = frame.payload_offset + cursor;
                let end = start + bytes_can_write;
                if frame.data.len() < end {
                    frame.data.resize(end, 0);
                }
                frame.data[start..end].copy_from_slice(&ptr[..bytes_can_write]);
                frame.current_frame_size = frame.current_frame_size.max(end);
                frame.dirty = true;
            }
            ptr = &ptr[bytes_can_write..];
            self.cursor += bytes_can_write;
            ret += bytes_can_write;
        }
        Ok(ret)
    }
}
impl<T: Read + Seek> Read for Stream<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        Stream::read(self, buf)
    }
}
impl<T: Read + Write + Seek> Write for Stream<T> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        Stream::write(self, buf)
    }
    fn flush(&mut self) -> Result<()> {
        self.flush()
    }
}
impl<T: Read + Seek> AsRef<[u8]> for Stream<T> {
    fn as_ref(&self) -> &[u8] {
        self.read_current_frame().unwrap_or_default()
    }
}
impl<T: Read + Seek> Stream<T> {
    pub fn load_next_frame(&mut self) -> Result<()> {
        if let Some(this_frame) = self.current_frame.take() {
            self.cursor = 0;
            self.current_frame = this_frame.load_next_frame(&mut self.file, true)?;
        }
        Ok(())
    }
    pub fn read_current_frame(&self) -> Option<&[u8]> {
        if let Some(this_frame) = self.current_frame.as_ref() {
            return Some(&this_frame.data[this_frame.payload_offset..]);
        }
        None
    }
    pub fn copy_current_frame_data(&self, buf: &mut Vec<u8>) {
        buf.clear();
        buf.extend_from_slice(self.read_current_frame().unwrap_or(&[]));
    }
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
                    let this_frame = self.current_frame.take();
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
    pub(crate) fn open_for_read(file: RandFile<T>, primary_frame: (u64, usize)) -> Result<Self> {
        Self::open_with_ondrop(file, primary_frame, |_| {})
    }
    pub(crate) fn open_for_update(file: RandFile<T>, primary_frame: (u64, usize)) -> Result<Self>
    where
        T: Write,
    {
        Self::open_with_ondrop(file, primary_frame, |s| s.flush().unwrap())
    }
    pub(crate) fn open_with_ondrop<D: FnOnce(&mut Self) + Send + Sync + 'static>(
        mut file: RandFile<T>,
        primary_frame: (u64, usize),
        on_drop: D,
    ) -> Result<Self> {
        let primary_frame = Frame::load_from_file(
            &mut file,
            primary_frame.0,
            primary_frame.1,
            true,
            None,
            false,
        )?;
        let frame_size = primary_frame.current_frame_size;
        let current_frame = Some(primary_frame);
        Ok(Self {
            file,
            current_frame,
            cursor: 0,
            frame_size,
            on_drop: Box::new(on_drop),
            pre_alloc: true,
        })
    }
}

impl<T: Read + Write + Seek> Stream<T> {
    pub fn update_current_byte(&mut self, byte: u8) -> Result<usize> {
        if let Some(current_frame) = &self.current_frame {
            if self.cursor > 0 {
                self.cursor -= 1;
            } else if current_frame.parent_frame.is_some() {
                let this_frame = self.current_frame.take().unwrap();
                this_frame.load_previous_frame(&mut self.file, true)?;
            }
            self.write(&[byte])?;
            return Ok(1);
        }
        Ok(0)
    }
    pub(crate) fn create(mut file: RandFile<T>, frame_size: usize) -> Result<Self> {
        let current_frame = Some(Frame::alloc_new_frame(None, &mut file, frame_size)?);
        Ok(Self {
            file,
            current_frame,
            cursor: 0,
            frame_size,
            on_drop: Box::new(|this| {
                this.flush().unwrap();
            }),
            pre_alloc: true,
        })
    }
}

impl<T> Drop for Stream<T> {
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
        check_send::<Stream<std::fs::File>>();
    }
    #[test]
    fn test_compose_stream() -> TestResult<()> {
        let mut buffer = vec![];
        {
            let fp = Cursor::new(&mut buffer);
            let file = RandFile::new(fp);

            let mut stream = Stream::create(file.clone(), 0)?;
            let mut stream2 = Stream::create(file, 0)?;

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
            let file = RandFile::new(fp);
            let mut stream = Stream::open_for_read(file, (0, 30))?;
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
        let file = RandFile::new(reader);

        let mut stream = Stream::open_for_read(file, (0, 19))?;

        let mut buffer = vec![0; 100];

        assert_eq!(7, stream.read(&mut buffer)?);

        Ok(())
    }

    #[test]
    fn test_modify_stream() -> TestResult<()> {
        let test_blob: Vec<_> = vec![
            19, 0, 0, 0, 0, 0, 0, 0, //Linked Frame
            20, 0, 0, 0, 0, 0, 0, 0, // Linked Frame size
            0xdd, 0xdd, 0x00, // Frame data
            0, 0, 0, 0, 0, 0, 0, 0, // Linked Frame
            0, 0, 0, 0, 0, 0, 0, 0, // Linked Frame size
            0, 0, 0, 0,
        ];
        let reader = Cursor::new(test_blob);
        let file = RandFile::new(reader);

        let mut stream = Stream::open_for_read(file, (0, 19))?;

        loop {
            let mut buf = [0; 1];
            stream.read(&mut buf[..]).unwrap();
            if buf[0] == 0 {
                break;
            }
        }
        assert_eq!(stream.update_current_byte(0xdd).unwrap(), 1);
        stream.write(&[0xdd]).unwrap();
        stream.write(&[0xdd]).unwrap();
        stream.flush().unwrap();

        let test_blob = stream
            .clone_underlying_file()
            .clone_inner()
            .unwrap()
            .into_inner();

        assert_eq!(test_blob[18], 0xdd);
        assert_eq!(test_blob[35], 0xdd);
        assert_eq!(test_blob[36], 0xdd);

        Ok(())
    }
}
