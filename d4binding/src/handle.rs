use std::fs::File;
use std::io::Result;
use std::path::Path;

use d4::Header;
use d4::{D4FileBuilder, D4FileWriter, D4TrackReader};

/*
    use d4::ssio::http::HttpReader;
    use d4::ssio::{D4TrackReader as SsioReader};
*/

use crate::c_api::d4_file_t;
use crate::stream::{StreamReader, StreamWriter};

type ReaderType = D4TrackReader;
type WriterType = D4FileWriter;
//type NetworkReader = SsioReader<HttpReader>;

pub enum D4FileHandle {
    Empty,
    Builder(Box<D4FileBuilder>),
    Writer(Box<WriterType>),
    Reader(Box<ReaderType>),
    StreamReader(Box<StreamReader>),
    StreamWriter(Box<StreamWriter>),
    //RemoteReader(Box<NetworkReader>),
}

impl D4FileHandle {
    pub fn into_ffi_object(this: Box<D4FileHandle>) -> *mut d4_file_t {
        Box::leak(this) as *mut _ as *mut _
    }

    pub fn drop_ffi_object(object: *mut d4_file_t) {
        let boxed_obj = unsafe { Box::from_raw(object as *mut D4FileHandle) };
        drop(boxed_obj);
    }

    pub fn new_for_create<P: AsRef<Path>>(path: P) -> Result<Box<Self>> {
        File::create(path.as_ref())?;
        Ok(Box::new(Self::Builder(Box::new(D4FileBuilder::new(
            path.as_ref(),
        )))))
    }

    pub fn new_for_read<P: AsRef<Path>>(path: P) -> Result<Box<D4FileHandle>> {
        D4TrackReader::open_first_track(path.as_ref())
            .map(|reader| Box::new(D4FileHandle::Reader(Box::new(reader))))
    }

    /*pub fn new_remote_reader(url: &str) -> Result<Box<D4FileHandle>> {
        let (url, tag) = if let Some(pos) = url.rfind('#') {
            (&url[..pos], Some(&url[pos+1..]))
        } else {
            (url, None)
        };
        let http_reader = HttpReader::new(url)?;
        let reader = NetworkReader::from_reader(http_reader, tag)?;
        Ok(Box::new(Self::RemoteReader(Box::new(reader))))
    }*/

    pub fn get_header(&self) -> Option<&Header> {
        match self {
            D4FileHandle::Reader(r) => Some(r.header()),
            //D4FileHandle::RemoteReader(r) => Some(r.get_header()),
            _ => None,
        }
    }

    pub fn as_stream_reader(&self) -> Option<&StreamReader> {
        match self {
            D4FileHandle::StreamReader(sr) => Some(sr),
            _ => None,
        }
    }
    pub fn as_stream_reader_mut(&mut self) -> Option<&mut StreamReader> {
        if matches!(self, D4FileHandle::Reader(_)) {
            let actual = std::mem::replace(self, Self::Empty);
            match actual {
                D4FileHandle::Reader(r) => {
                    let sr = Box::new(StreamReader::new(*r).ok()?);
                    *self = D4FileHandle::StreamReader(sr);
                    return self.as_stream_reader_mut();
                }
                _ => unreachable!(),
            }
        }
        match self {
            D4FileHandle::StreamReader(sr) => Some(sr.as_mut()),
            _ => None,
        }
    }

    pub fn do_build(&mut self) -> Result<()> {
        let is_builder = matches!(self, D4FileHandle::Builder(_));
        if !is_builder {
            return Ok(());
        }
        let actual = std::mem::replace(self, D4FileHandle::Empty);
        let mut builder = match actual {
            D4FileHandle::Builder(b) => b,
            _ => unreachable!(),
        };

        let writer = builder.create()?;

        *self = D4FileHandle::Writer(Box::new(writer));

        Ok(())
    }

    pub fn as_stream_writer(&self) -> Option<&StreamWriter> {
        match self {
            D4FileHandle::StreamWriter(sw) => Some(sw),
            _ => None,
        }
    }

    pub fn as_stream_writer_mut(&mut self) -> Option<&mut StreamWriter> {
        if let Err(_what) = self.do_build() {
            println!("{:?}", _what);
            return None;
        }
        if matches!(self, D4FileHandle::Writer(_)) {
            let actual = std::mem::replace(self, Self::Empty);
            match actual {
                D4FileHandle::Writer(w) => {
                    let sw = Box::new(StreamWriter::new(*w).ok()?);
                    *self = D4FileHandle::StreamWriter(sw);
                    return self.as_stream_writer_mut();
                }
                _ => unreachable!(),
            }
        }
        match self {
            D4FileHandle::StreamWriter(w) => Some(w.as_mut()),
            _ => None,
        }
    }
    pub fn as_reader_mut(&mut self) -> Option<&mut ReaderType> {
        match self {
            Self::Reader(r) => Some(r),
            _ => None,
        }
    }

    pub fn as_reader(&self) -> Option<&ReaderType> {
        match self {
            Self::Reader(r) => Some(r),
            _ => None,
        }
    }
}

impl From<*const d4_file_t> for &'static D4FileHandle {
    fn from(raw: *const d4_file_t) -> &'static D4FileHandle {
        let raw = raw as *const D4FileHandle;
        unsafe { raw.as_ref().unwrap() }
    }
}

impl From<*mut d4_file_t> for &'static mut D4FileHandle {
    fn from(raw: *mut d4_file_t) -> &'static mut D4FileHandle {
        let raw = raw as *mut D4FileHandle;
        unsafe { raw.as_mut().unwrap() }
    }
}
