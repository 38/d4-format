#![allow(non_upper_case_globals)]
#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(improper_ctypes)]
#![allow(clippy::redundant_static_lifetimes)]
#![allow(clippy::upper_case_acronyms)]
include!("../../generated/hts.rs");

#[cfg(no_bam_hdr_destroy)]
pub unsafe extern "C" fn bam_hdr_destroy(h: *mut bam_hdr_t) {
    sam_hdr_destroy(h as _)
}
