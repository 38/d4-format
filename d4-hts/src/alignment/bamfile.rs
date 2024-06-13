use std::cell::Cell;
use std::ffi::{c_void, c_char};
use std::ffi::{CStr, CString};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::ptr::null_mut;

use super::error::AlignmentError;
use super::htslib::*;
use super::{Alignment, AlignmentReader};

/// A BAM/CRAM/SAM File
pub struct BamFile {
    chrom_list: Vec<(String, usize)>,
    fp: *mut htsFile,
    hdr: *mut bam_hdr_t,
    idx: *mut hts_idx_t,
    mp_size_limit: usize,
    mp_free: Cell<Vec<*mut bam1_t>>,
    path: Box<PathBuf>,
}

impl Drop for BamFile {
    fn drop(&mut self) {
        if !self.idx.is_null() {
            unsafe {
                hts_idx_destroy(self.idx);
            }
            self.idx = null_mut();
        }

        if !self.hdr.is_null() {
            unsafe {
                bam_hdr_destroy(self.hdr);
            }
            self.hdr = null_mut();
        }

        if !self.fp.is_null() {
            unsafe { hts_close(self.fp) };
            self.fp = null_mut();
        }

        let bams = self.mp_free.replace(vec![]);

        for bam in bams {
            unsafe { bam_destroy1(bam) };
        }
    }
}

impl BamFile {
    pub fn set_required_fields(&mut self, flag: u32) {
        unsafe {
            hts_set_opt(self.fp, hts_fmt_option_CRAM_OPT_REQUIRED_FIELDS, flag);
        }
    }
    /// Set the path to the reference FAI file. Only used for CRAM
    pub fn reference_path<P: AsRef<Path>>(&self, path: P) {
        unsafe {
            let path_buf = CString::new(path.as_ref().as_os_str().as_bytes()).unwrap();
            hts_set_fai_filename(self.fp, path_buf.as_ptr());
        }
    }
    /// Open a BAM/CRAM/SAM file on the disk
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, AlignmentError> {
        let mut ret = BamFile {
            path: Box::new(path.as_ref().to_path_buf()),
            chrom_list: vec![],
            fp: null_mut(),
            hdr: null_mut(),
            idx: null_mut(),
            mp_free: Cell::new(vec![]),
            mp_size_limit: 20,
        };

        ret.fp = unsafe {
            let path_buf = CString::new(path.as_ref().as_os_str().as_bytes()).unwrap();
            let mod_buf = CString::new("rb").unwrap();
            let ptr = hts_open(path_buf.as_ptr(), mod_buf.as_ptr());
            if ptr.is_null() {
                return Err((-1).into());
            }
            ptr
        };

        ret.hdr = unsafe { sam_hdr_read(ret.fp) };

        if ret.hdr.is_null() {
            return Err((-1).into());
        }

        let raw_names = unsafe {
            std::slice::from_raw_parts((*ret.hdr).target_name, (*ret.hdr).n_targets as usize)
        };

        let sizes = unsafe {
            std::slice::from_raw_parts((*ret.hdr).target_len, (*ret.hdr).n_targets as usize)
        };

        for (size, raw_name) in sizes.iter().zip(raw_names) {
            let raw_name = unsafe { CStr::from_ptr(*raw_name as *const c_char) };

            ret.chrom_list
                .push((raw_name.to_string_lossy().to_string(), *size as usize));
        }

        Ok(ret)
    }

    pub fn chroms(&self) -> &[(String, usize)] {
        &self.chrom_list[..]
    }

    pub(super) fn alloc_inner_obj(&self) -> Result<*mut bam1_t, AlignmentError> {
        let ret;

        let mut cur_list = self.mp_free.replace(vec![]);

        if cur_list.is_empty() {
            ret = unsafe { bam_init1() };
            if ret.is_null() {
                self.mp_free.replace(cur_list);
                return Err((-1).into());
            }
        } else {
            ret = cur_list.pop().unwrap();
        }

        self.mp_free.replace(cur_list);
        Ok(ret)
    }

    pub(super) fn free_inner_obj(&self, obj: *mut bam1_t) {
        if obj.is_null() {
            return;
        }

        let mut cur_list = self.mp_free.replace(vec![]);

        if cur_list.len() >= self.mp_size_limit {
            unsafe {
                bam_destroy1(obj);
            }
            return;
        }

        cur_list.push(obj);

        self.mp_free.replace(cur_list);
    }

    pub fn range(
        &mut self,
        chrom: &str,
        from: usize,
        to: usize,
    ) -> Result<Ranged<'_>, AlignmentError> {
        if self.idx.is_null() {
            self.idx = unsafe {
                let path_buf = CString::new(self.path.as_path().as_os_str().as_bytes()).unwrap();
                sam_index_load(self.fp, path_buf.as_ptr())
            };
            if self.idx.is_null() {
                return Err((-1).into());
            }
        }

        let mut chrom_iter = self.chrom_list[..].iter().zip(0..);
        let chrom = loop {
            if let Some(((name, _), idx)) = chrom_iter.next() {
                if name == chrom {
                    break idx;
                }
            } else {
                return Err(AlignmentError::BadPosition);
            }
        };

        let iter = unsafe {
            sam_itr_queryi(
                self.idx,
                chrom as i32,
                (from as i32).into(),
                (to as i32).into(),
            )
        };

        if iter.is_null() {
            return Err((-1).into());
        }

        Ok(Ranged {
            file: self,
            iter,
            start: from as u32,
            chrom,
        })
    }
}

pub struct Ranged<'a> {
    chrom: u32,
    start: u32,
    file: &'a BamFile,
    iter: *mut hts_itr_t,
}

impl<'a> Drop for Ranged<'a> {
    fn drop(&mut self) {
        if !self.iter.is_null() {
            unsafe { hts_itr_destroy(self.iter) };
            self.iter = null_mut();
        }
    }
}

impl<'a> AlignmentReader<'a> for &'a BamFile {
    fn start(&self) -> (u32, u32) {
        (0, 0)
    }
    fn get_file(&self) -> &'a BamFile {
        self
    }

    #[allow(clippy::not_unsafe_ptr_arg_deref)]
    fn next(&self, buf: *mut bam1_t) -> Result<Option<Alignment<'a>>, AlignmentError> {
        let rc = unsafe { sam_read1(self.fp, self.hdr, buf) };

        if rc > 0 {
            return Ok(Some(Alignment::new(buf, self)));
        }

        if rc == 0 {
            return Ok(None);
        }

        Err(rc.into())
    }
}

impl<'a> AlignmentReader<'a> for Ranged<'a> {
    fn start(&self) -> (u32, u32) {
        (self.chrom, self.start)
    }
    fn get_file(&self) -> &'a BamFile {
        self.file
    }

    fn next(&self, buf: *mut bam1_t) -> Result<Option<Alignment<'a>>, AlignmentError> {
        let rc = unsafe {
            hts_itr_next(
                (*self.file.fp).fp.bgzf,
                self.iter,
                buf as *mut c_void,
                self.file.fp as *mut c_void,
            )
        };

        if rc > 0 {
            return Ok(Some(Alignment::new(buf, self.file)));
        }

        if rc == 0 {
            return Ok(None);
        }

        Err(rc.into())
    }
}
