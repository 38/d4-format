#![allow(non_upper_case_globals)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(improper_ctypes)]
include!("../../generated/hts.rs");

#[cfg(no_bam_hdr_destroy)]
#[no_mangle]
pub unsafe extern "C" fn bam_hdr_destroy(h: *mut bam_hdr_t) {
    sam_hdr_destroy(h as _)
}
