use std::io::{Read, Seek, Write};

pub trait AccessMode {
    fn can_read() -> bool {
        false
    }
    fn can_write() -> bool {
        false
    }
}
pub trait CanRead<T: Read + Seek>: AccessMode {}
pub trait CanWrite<T: Write + Seek>: AccessMode {}

pub struct ReadOnly;
pub struct ReadWrite;

impl AccessMode for ReadOnly {
    fn can_read() -> bool {
        true
    }
}
impl<T: Read + Seek> CanRead<T> for ReadOnly {}

impl AccessMode for ReadWrite {
    fn can_read() -> bool {
        true
    }
    fn can_write() -> bool {
        true
    }
}
impl<T: Read + Seek> CanRead<T> for ReadWrite {}
impl<T: Write + Seek> CanWrite<T> for ReadWrite {}
