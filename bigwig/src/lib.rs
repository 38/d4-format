mod bigwig_sys;

use bigwig_sys::*;
use std::ffi::{CStr, CString};
use std::os::unix::ffi::OsStrExt;
use std::path::Path;
use std::ptr::null_mut;

pub struct BigWigFile(*mut bigWigFile_t);

impl Drop for BigWigFile {
    fn drop(&mut self) {
        unsafe {
            bwClose(self.0);
        }
    }
}

pub struct BigWigInterval {
    pub begin: u32,
    pub end: u32,
    pub value: f32,
}

pub struct BigWigIntervalIter {
    result: *mut bwOverlappingIntervals_t,
    idx: isize,
}

impl Drop for BigWigIntervalIter {
    fn drop(&mut self) {
        unsafe {
            bwDestroyOverlappingIntervals(self.result);
        }
    }
}

impl Iterator for BigWigIntervalIter {
    type Item = BigWigInterval;
    fn next(&mut self) -> Option<BigWigInterval> {
        let size = unsafe { *self.result }.l;
        if size as isize <= self.idx {
            return None;
        }
        let begin = unsafe { *self.result.as_ref().unwrap().start.offset(self.idx) };
        let end = unsafe { *self.result.as_ref().unwrap().end.offset(self.idx) };
        let value = unsafe { *self.result.as_ref().unwrap().value.offset(self.idx) };

        self.idx += 1;
        Some(BigWigInterval { begin, end, value })
    }
}

impl BigWigFile {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let handle = unsafe {
            bwOpen(
                CString::new(path.as_ref().as_os_str().as_bytes())
                    .unwrap()
                    .as_ptr() as *mut _,
                None,
                CString::new("r").unwrap().as_ptr() as *mut _,
            )
        };

        if null_mut() == handle {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Cannot open BW file",
            ));
        }
        Ok(BigWigFile(handle))
    }

    pub fn chroms(&self) -> Vec<(String, usize)> {
        let chrom_info = unsafe { (*self.0).cl.as_ref().unwrap() };

        let mut ret = Vec::with_capacity(chrom_info.nKeys as usize);

        for i in 0..chrom_info.nKeys as isize {
            let (name, size) = unsafe {
                let ptr = *chrom_info.chrom.offset(i);
                let str = CStr::from_ptr(ptr);
                let name = str.to_string_lossy();

                let size = *chrom_info.len.offset(i) as usize;
                (name.to_string(), size)
            };

            ret.push((name, size));
        }
        ret
    }

    pub fn query_range(&self, chrom: &str, left: u32, right: u32) -> Option<BigWigIntervalIter> {
        let str = CString::new(chrom).unwrap();
        let chrom = str.as_ptr();
        let handle = unsafe { bwGetOverlappingIntervals(self.0 as _, chrom as _, left, right) };
        if null_mut() == handle {
            return None;
        }
        Some(BigWigIntervalIter {
            result: handle,
            idx: 0,
        })
    }
}
