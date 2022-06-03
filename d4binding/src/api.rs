use d4::index::{D4IndexCollection, Sum};
use d4_framefile::Directory;
use rayon::prelude::*;

use std::ffi::{CStr, OsStr};
use std::io::{Read, Seek};
use std::os::raw::c_char;
use std::path::Path;
use std::ptr::{null, null_mut};

use crate::c_api::*;
use crate::handle::D4FileHandle;
use crate::stream::RootContainer;
use crate::task::TaskHandle;

use d4::Chrom;
use d4::Dictionary;
use std::cell::RefCell;

thread_local! {
    static LAST_ERROR :  RefCell<Option<Box<dyn std::error::Error>>> = RefCell::new(None);
}

fn set_last_error<E: std::error::Error + 'static>(e: E) {
    LAST_ERROR.with(|eb| {
        let mut eb = eb.borrow_mut();
        *eb = Some(Box::new(e));
    })
}

fn set_einval<R>(ret: R) -> R {
    set_last_error(std::io::Error::from_raw_os_error(22));
    ret
}

#[no_mangle]
pub extern "C" fn d4_error_clear() {
    LAST_ERROR.with(|eb| {
        *eb.borrow_mut() = None;
    });
}

#[no_mangle]
pub extern "C" fn d4_error_message(buf: *mut c_char, size: u64) -> *const c_char {
    if buf == null_mut() || size == 0 {
        return null();
    }
    let data = unsafe { std::slice::from_raw_parts_mut(buf as _, size as usize) };

    let message = LAST_ERROR.with(|e| {
        e.borrow()
            .as_ref()
            .map_or_else(|| "ok".to_string(), |e| format!("{}", e))
    });

    let bytes = message.as_bytes();
    let bytes_to_copy = bytes.len().min(size as usize - 1);
    data[..bytes_to_copy].copy_from_slice(&bytes[..bytes_to_copy]);
    data[bytes_to_copy] = 0;
    return buf;
}

#[no_mangle]
pub extern "C" fn d4_error_num() -> i32 {
    LAST_ERROR.with(|eb| {
        if let Some(error) = (*eb.borrow()).as_ref() {
            if let Some(system_error) = error.downcast_ref::<std::io::Error>() {
                return system_error.raw_os_error().unwrap_or(-1);
            }
        }
        return -1;
    })
}

#[no_mangle]
pub extern "C" fn d4_open(path: *const c_char, mode: *const c_char) -> *mut d4_file_t {
    if path != null() && mode != null() {
        let path = unsafe { CStr::from_ptr(path) };
        #[cfg(any(unix))]
        let path: &Path = {
            use std::os::unix::ffi::OsStrExt;
            OsStr::from_bytes(path.to_bytes()).as_ref()
        };

        let url = path.to_string_lossy();

        #[cfg(windows)]
        let str = std::str::from_utf8(path.to_bytes()).expect("keep your surrogates paired");
        #[cfg(windows)]
        let path = str.as_ref();

        if let Ok(mode_str) = unsafe { CStr::from_ptr(mode).to_str() } {
            if mode_str == "w" {
                return D4FileHandle::new_for_create(path).map_or_else(
                    |e| {
                        set_last_error(e);
                        null_mut()
                    },
                    D4FileHandle::into_ffi_object,
                );
            } else if mode_str == "r" {
                if url.starts_with("http://") || url.starts_with("https://") {
                    return D4FileHandle::new_remote_reader(&url).map_or_else(
                        |e| {
                            set_last_error(e);
                            null_mut()
                        },
                        D4FileHandle::into_ffi_object,
                    );
                } else {
                    return D4FileHandle::new_for_read(path).map_or_else(
                        |e| {
                            set_last_error(e);
                            null_mut()
                        },
                        D4FileHandle::into_ffi_object,
                    );
                }
            }
        }
    }
    set_einval(null_mut())
}

#[no_mangle]
pub extern "C" fn d4_close(handle: *mut d4_file_t) -> i32 {
    if handle == null_mut() {
        return set_einval(-1);
    }
    D4FileHandle::drop_ffi_object(handle);
    0
}
#[no_mangle]
#[allow(non_upper_case_globals)]
pub extern "C" fn d4_file_update_metadata(
    handle: *mut d4_file_t,
    metadata: *const d4_file_metadata_t,
) -> i32 {
    if null() == handle || null() == metadata {
        return set_einval(-1);
    }
    let handle: &mut D4FileHandle = handle.into();
    match handle {
        D4FileHandle::Builder(b) => {
            let metadata = unsafe { &*metadata };
            let dictionary = match metadata.dict_type {
                d4_dict_type_t_D4_DICT_SIMPLE_RANGE => unsafe {
                    if let Ok(result) = Dictionary::new_simple_range_dict(
                        metadata.dict_data.simple_range.low,
                        metadata.dict_data.simple_range.high,
                    )
                    .map_err(set_last_error)
                    {
                        result
                    } else {
                        return -1;
                    }
                },
                d4_dict_type_t_D4_DICT_VALUE_MAP => unsafe {
                    let value_map = {
                        if metadata.dict_data.value_map.values == null_mut() {
                            return -1;
                        }
                        std::slice::from_raw_parts(
                            metadata.dict_data.value_map.values,
                            metadata.dict_data.value_map.size as usize,
                        )
                    };
                    if let Ok(result) =
                        Dictionary::from_dict_list(value_map.to_owned()).map_err(set_last_error)
                    {
                        result
                    } else {
                        return -1;
                    }
                },
                _ => {
                    set_last_error(std::io::Error::from_raw_os_error(22));
                    return -1;
                }
            };
            b.set_dictionary(dictionary);
            if metadata.chrom_name == null_mut() || metadata.chrom_size == null_mut() {
                set_last_error(std::io::Error::from_raw_os_error(22));
                return -1;
            }
            let names: Vec<_> = unsafe {
                let raw_slice = std::slice::from_raw_parts(
                    metadata.chrom_name as *const *const c_char,
                    metadata.chrom_count as usize,
                );
                raw_slice
                    .iter()
                    .map(|p| {
                        CStr::from_ptr(*p)
                            .to_str()
                            .unwrap_or("<Invalid Str>")
                            .to_owned()
                    })
                    .collect()
            };
            let sizes = unsafe {
                std::slice::from_raw_parts(
                    metadata.chrom_size as *const u32,
                    metadata.chrom_count as usize,
                )
            };
            b.append_chrom(
                names
                    .into_iter()
                    .zip(sizes.iter())
                    .map(|(name, &size)| Chrom {
                        name,
                        size: size as usize,
                    }),
            );
            if (metadata.denominator - 1.0).abs() >= 1e-10 {
                b.set_denominator(metadata.denominator);
            }
            return 0;
        }
        _ => {
            return set_einval(-1);
        }
    }
}
#[no_mangle]
pub extern "C" fn d4_file_load_metadata(
    handle: *const d4_file_t,
    buf: *mut d4_file_metadata_t,
) -> i32 {
    if null() == handle {
        return set_einval(-1);
    }
    let handle: &D4FileHandle = handle.into();
    if let Some(header) = handle.get_header() {
        let chrom_list = header.chrom_list();
        unsafe {
            (*buf).chrom_count = chrom_list.len() as u64;
            (*buf).chrom_name = crate::c_api::malloc(
                (std::mem::size_of::<*mut c_char>() * chrom_list.len()) as u64,
            ) as *mut _;
            (*buf).chrom_size =
                crate::c_api::malloc((std::mem::size_of::<i32>() * chrom_list.len()) as u64)
                    as *mut _;
            for (id, chrom) in chrom_list.iter().enumerate() {
                *(*buf).chrom_size.offset(id as isize) = chrom.size as u32;
                *(*buf).chrom_name.offset(id as isize) = {
                    let name_bytes = chrom.name.as_bytes();
                    let buf = crate::c_api::malloc(name_bytes.len() as u64 + 1) as *mut c_char;
                    extern "C" {
                        fn memcpy(dst: *mut c_char, src: *const c_char, size: usize);
                    }
                    memcpy(
                        buf,
                        &name_bytes[0] as *const _ as *const _,
                        name_bytes.len(),
                    );
                    *buf.offset(name_bytes.len() as isize) = 0;
                    buf
                };
            }

            match header.dictionary() {
                Dictionary::SimpleRange { low, high } => {
                    (*buf).dict_type = crate::c_api::d4_dict_type_t_D4_DICT_SIMPLE_RANGE;
                    (*buf).dict_data.simple_range.low = *low;
                    (*buf).dict_data.simple_range.high = *high;
                }
                Dictionary::Dictionary { i2v_map, .. } => {
                    (*buf).dict_type = crate::c_api::d4_dict_type_t_D4_DICT_VALUE_MAP;
                    (*buf).dict_data.value_map.size = i2v_map.len() as u64;
                    (*buf).dict_data.value_map.values =
                        crate::c_api::malloc((std::mem::size_of::<i32>() * i2v_map.len()) as u64)
                            as *mut _;
                    for (idx, val) in i2v_map.iter().enumerate() {
                        *(*buf).dict_data.value_map.values.offset(idx as isize) = *val;
                    }
                }
            }

            if let Some(denominator) = handle
                .as_reader()
                .map(|reader| reader.header().get_denominator())
            {
                (*buf).denominator = denominator;
            }
        }
    }
    0
}

#[no_mangle]
pub extern "C" fn d4_file_read_values(
    handle: *mut d4_file_t,
    buf: *mut i32,
    count: usize,
) -> isize {
    if null_mut() == handle || null_mut() == buf {
        return set_einval(-1);
    }

    let handle: &mut D4FileHandle = handle.into();

    if let Some(sr) = handle.as_stream_reader_mut() {
        let mut ret = 0;
        for offset in 0..count {
            if let Some(result) = sr.next(ret > 0) {
                unsafe {
                    *buf.offset(offset as isize) = result;
                }
                ret += 1;
            } else {
                break;
            }
        }
        return ret;
    } else {
        return set_einval(-1);
    }
}

#[no_mangle]
pub extern "C" fn d4_file_tell(
    handle: *const d4_file_t,
    name_buf: *mut ::std::os::raw::c_char,
    size_buf: *mut size_t,
    pos_buf: *mut u32,
) -> i32 {
    if null() == handle {
        return set_einval(-1);
    }
    let handle: &D4FileHandle = handle.into();

    fn copy_buffer(name_buf: *mut ::std::os::raw::c_char, size_buf: *mut size_t, data: &str) {
        if null_mut() == name_buf {
            return;
        }
        let buffer: &mut [u8] =
            unsafe { std::slice::from_raw_parts_mut(name_buf as *mut _, size_buf as usize) };
        let size = (size_buf as usize - 1).min(data.as_bytes().len());
        buffer[..size].copy_from_slice(data.as_bytes());
        buffer[size] = 0;
    }

    if let Some(sr) = handle.as_stream_reader() {
        if let Some((chr, pos)) = sr.tell() {
            copy_buffer(name_buf, size_buf, chr);
            unsafe {
                *pos_buf = pos;
            }
            return 0;
        }
    } else if let Some(sw) = handle.as_stream_writer() {
        if let Some((chr, pos)) = sw.tell() {
            copy_buffer(name_buf, size_buf, chr);
            unsafe {
                *pos_buf = pos;
            }
            return 0;
        }
    }
    if let Some(hdr) = handle.get_header() {
        if let Some(chrom) = hdr.chrom_list().get(0) {
            copy_buffer(name_buf, size_buf, chrom.name.as_ref());
            unsafe {
                *pos_buf = 0;
            }
            return 0;
        }
    }
    set_einval(-1)
}

#[no_mangle]
pub fn d4_file_seek(handle: *mut d4_file_t, chrom: *const ::std::os::raw::c_char, pos: u32) -> i32 {
    if null() == handle || null() == chrom {
        return set_einval(-1);
    }
    let handle: &mut D4FileHandle = handle.into();

    if let Some(sr) = handle.as_stream_reader_mut() {
        let chrom = unsafe { CStr::from_ptr(chrom) };
        if let Ok(chrom) = chrom.to_str() {
            return if sr.seek(chrom, pos) { 0 } else { -1 };
        }
    } else if let Some(sw) = handle.as_stream_writer_mut() {
        let chrom = unsafe { CStr::from_ptr(chrom) };
        if let Ok(chrom) = chrom.to_str() {
            return if sw.seek(chrom, pos) { 0 } else { -1 };
        }
    }
    return -1;
}
#[no_mangle]
pub extern "C" fn d4_file_read_intervals(
    handle: *mut d4_file_t,
    buf: *mut d4_interval_t,
    count: size_t,
) -> ssize_t {
    if null() == handle {
        return -1;
    }
    if null() == buf {
        return -1;
    }
    if count == 0 {
        return 0;
    }

    let handle: &mut D4FileHandle = handle.into();

    if let Some(sr) = handle.as_stream_reader_mut() {
        let mut ret = 0;
        for offset in 0..count {
            if let Some(result) = sr.next_interval(ret > 0) {
                unsafe {
                    (*buf.offset(offset as isize)).left = result.0.start;
                    (*buf.offset(offset as isize)).right = result.0.end;
                    (*buf.offset(offset as isize)).value = result.1;
                }
                ret += 1;
            } else {
                break;
            }
        }
        return ret;
    } else {
        return -1;
    }
}

#[no_mangle]
pub fn d4_file_write_values(handle: *mut d4_file_t, buf: *const i32, count: size_t) -> ssize_t {
    if null_mut() == handle {
        return -1;
    }
    if null() == buf {
        return -1;
    }
    let handle: &mut D4FileHandle = handle.into();

    if let Some(sr) = handle.as_stream_writer_mut() {
        let count = count as usize;
        let data_buf = unsafe { std::slice::from_raw_parts(buf, count) };
        let mut first = true;
        let mut ret = 0;
        for data in data_buf {
            match sr.write_value(*data, !first) {
                Ok(false) => {
                    break;
                }
                Ok(true) => {
                    ret += 1;
                }
                Err(_) => return -1,
            }
            first = false;
        }
        sr.flush();
        return ret;
    }
    return -1;
}

#[no_mangle]
pub fn d4_file_write_intervals(
    handle: *mut d4_file_t,
    buf: *const d4_interval_t,
    count: size_t,
) -> ssize_t {
    if null_mut() == handle {
        return -1;
    }
    if null() == buf {
        return -1;
    }
    let handle: &mut D4FileHandle = handle.into();

    if let Some(sr) = handle.as_stream_writer_mut() {
        let count = count as usize;
        let data_buf = unsafe { std::slice::from_raw_parts(buf, count) };
        let mut ret = 0;
        for data in data_buf {
            let data = &*data;
            match sr.write_interval(data.left as u32, data.right as u32, data.value as i32) {
                Ok(false) => {
                    break;
                }
                Ok(true) => {
                    ret += 1;
                }
                Err(_) => return -1,
            }
        }
        sr.flush();
        return ret;
    }

    -1
}

#[no_mangle]
pub fn d4_file_run_task(handle: *mut d4_file_t, task: *const d4_task_desc_t) -> i32 {
    if null_mut() == handle || null() == task {
        return set_einval(-1);
    }
    let handle: &mut D4FileHandle = handle.into();
    let task = unsafe { &*task };

    if let Some(reader) = handle.as_reader_mut() {
        if let Ok(mut task_parts) =
            TaskHandle::from_reader(reader, task.part_size_limit).map_err(set_last_error)
        {
            if rayon::ThreadPoolBuilder::new()
                .num_threads(task.num_cpus as usize)
                .build_global()
                .map_err(set_last_error)
                .is_err()
            {}

            let task_data: Vec<_> = task_parts
                .iter_mut()
                .map(|handle| {
                    let local_data = if let Some(callback) = task.part_context_create_cb {
                        unsafe { callback(handle as *mut _ as *mut _, task.extra_data) }
                    } else {
                        null_mut()
                    };
                    (
                        local_data as usize,
                        task.extra_data as usize,
                        task.part_process_cb.map(|foo| foo as usize),
                    )
                })
                .collect();

            let status: Vec<_> = task_parts
                .par_iter_mut()
                .zip(task_data.clone())
                .map(|(handle, (local_data, extra_data, entry_point))| {
                    if let Some(task_main) = entry_point {
                        let task_main: unsafe extern "C" fn(
                            handle: usize,
                            task_context: usize,
                            extra_data: usize,
                        ) -> i32 = unsafe { std::mem::transmute(task_main) };
                        let result =
                            unsafe { task_main(handle as *mut _ as usize, local_data, extra_data) };
                        result
                    } else {
                        0
                    }
                })
                .collect();
            let mut task_results: Vec<_> = task_data
                .into_iter()
                .zip(status)
                .map(|((local, _, _), status)| {
                    let data = local as *mut std::ffi::c_void;
                    d4_task_part_result_t {
                        task_context: data,
                        status,
                    }
                })
                .collect();
            if let Some(finalize) = task.part_finalize_cb {
                let data = &mut task_results[0] as *mut _;
                let size = task_results.len();
                return unsafe { finalize(data, size.try_into().unwrap(), task.extra_data) };
            } else {
                return 0;
            }
        } else {
            return -1;
        }
    }
    set_einval(-1)
}

#[no_mangle]
pub fn d4_task_range(task: *const d4_task_part_t, left_buf: *mut u32, right_buf: *mut u32) -> i32 {
    if null() == task {
        return set_einval(-1);
    }
    let handle: &TaskHandle = unsafe { std::mem::transmute(task) };
    let (_, l, r) = handle.range();

    if left_buf != null_mut() {
        unsafe { *left_buf = l };
    }

    if right_buf != null_mut() {
        unsafe { *right_buf = r };
    }

    0
}

#[no_mangle]
pub extern "C" fn d4_task_chrom(
    task: *const d4_task_part_t,
    name_buf: *mut c_char,
    name_buf_size: size_t,
) -> i32 {
    if null() == task {
        return set_einval(-1);
    }
    let handle: &TaskHandle = unsafe { std::mem::transmute(task) };
    let (name, _, _) = handle.range();

    let data = name.as_bytes();

    let bytes_can_copy = (name_buf_size as usize - 1).min(data.len());
    let slice = unsafe { std::slice::from_raw_parts_mut(name_buf as *mut _, bytes_can_copy + 1) };
    slice[bytes_can_copy] = 0;
    slice[..bytes_can_copy].copy_from_slice(&data[..bytes_can_copy]);
    0
}

#[no_mangle]
pub extern "C" fn d4_task_read_values(
    task: *mut d4_task_part_t,
    offset: u32,
    buffer: *mut i32,
    count: size_t,
) -> ssize_t {
    if null() == task {
        return set_einval(-1);
    }
    let handle: &mut TaskHandle = unsafe { std::mem::transmute(task) };
    let buffer = unsafe { std::slice::from_raw_parts_mut(buffer, count as usize) };
    let (_, _, end) = handle.range();
    let mut ret = 0;

    for pos in offset..(offset + count as u32).min(end) {
        if let Some(read_result) = handle.read(pos) {
            buffer[ret] = read_result
        } else {
            return -1;
        }
        ret += 1;
    }

    ret as ssize_t
}

#[no_mangle]
pub extern "C" fn d4_index_check(handle: *mut d4_file_t, kind: d4_index_kind_t) -> i32 {
    if null_mut() == handle {
        return set_einval(-1);
    }
    let handle: &mut D4FileHandle = handle.into();
    #[allow(non_upper_case_globals)]
    fn check_impl<R: Read + Seek>(
        root: &Directory<R>,
        kind: d4_index_kind_t,
    ) -> std::io::Result<bool> {
        let ic = D4IndexCollection::from_root_container(root)?;
        match kind {
            d4_index_kind_t_D4_INDEX_KIND_SUM => Ok(ic.load_data_index::<Sum>().is_ok()),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid index kind",
            ))?,
        }
    }
    let result = if let Some(sr) = handle.as_stream_reader() {
        match sr.root_container() {
            RootContainer::Local(local) => check_impl(local, kind),
            RootContainer::Remote(remote) => check_impl(remote, kind),
        }
    } else {
        return 0;
    };
    match result {
        Ok(ret) => return if ret { 1 } else { 0 },
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    }
}

#[no_mangle]
pub fn d4_index_query(
    handle: *mut d4_file_t,
    kind: d4_index_kind_t,
    chrom: *const ::std::os::raw::c_char,
    start: u32,
    end: u32,
    buf: *mut d4_index_result_t,
) -> i32 {
    if null_mut() == handle || null_mut() == buf {
        return set_einval(-1);
    }
    let handle: &mut D4FileHandle = handle.into();
    #[allow(non_upper_case_globals)]
    fn query_impl<R: Read + Seek>(
        root: &Directory<R>,
        kind: d4_index_kind_t,
        chrom: &str,
        start: u32,
        end: u32,
    ) -> std::io::Result<f64> {
        let ic = D4IndexCollection::from_root_container(root)?;
        let mut stream_reader = d4::ssio::D4TrackReader::from_track_root(root.clone())?;
        match kind {
            d4_index_kind_t_D4_INDEX_KIND_SUM => {
                let index = ic.load_data_index::<Sum>()?;
                let result = index.query(chrom, start, end).unwrap();
                Ok(result.get_result(&mut stream_reader)?.sum())
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid index kind",
            ))?,
        }
    }
    let chrom = unsafe { CStr::from_ptr(chrom) };
    let chr_ref = if let Ok(chrom) = chrom.to_str() {
        chrom
    } else {
        return set_einval(-1);
    };
    let buf = unsafe { &mut *buf };
    let result = if let Some(sr) = handle.as_stream_reader() {
        match sr.root_container() {
            RootContainer::Local(local) => query_impl(local, kind, chr_ref, start, end),
            RootContainer::Remote(remote) => query_impl(remote, kind, chr_ref, start, end),
        }
    } else {
        return 0;
    };
    match result {
        Ok(value) => {
            buf.sum = value;
            return 0;
        }
        Err(e) => {
            set_last_error(e);
            return -1;
        }
    }
}
