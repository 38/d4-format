use std::io::{Read, Seek};

use wasm_bindgen::prelude::*;

use crate::ssio::{D4TrackReader as D4TrackReaderImpl};

struct HttpReaderCallbacks {
	get_size: js_sys::Function,
	get_data: js_sys::Function,
	cursor: u64,
	size: Option<u64>
}

impl Read for HttpReaderCallbacks {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        todo!()
    }
}

impl Seek for HttpReaderCallbacks {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        todo!()
    }
}
#[wasm_bindgen]
pub struct D4TrackReader {
	inner: D4TrackReaderImpl<'static, HttpReaderCallbacks>,
}

fn new_reader(get_size: js_sys::Function, get_data: js_sys::Function, track_name: Option<&str>) -> D4TrackReader {
	let reader = HttpReaderCallbacks {
		get_data,
		get_size,
		cursor: 0,
		size: None,
	};
	let inner = D4TrackReaderImpl::from_reader(reader, track_name).unwrap();
	D4TrackReader { inner }
}

#[wasm_bindgen]
extern "C" {
	#[wasm_bindgen(constructor)]
	fn new(get_size: js_sys::Function, get_data: js_sys::Function, track_name: Option<&str>) -> D4TrackReader;
}