use std::io::{Read, Seek};

use wasm_bindgen::prelude::*;
use web_sys::{Request, RequestInit};

struct WasmHttpStream {
    url: String,
    size: u64,
    cursor: u64,
}

impl Read for WasmHttpStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let size_to_read = (buf.len() as u64).min(self.size - self.cursor);
        let mut opts = RequestInit::new();
        opts.method("GET").mode(web_sys::RequestMode::Cors);

        let request = Request::new_with_str_and_init(&self.url, &opts).unwrap();
        request.headers().set(
            "range",
            &format!("bytes={}-{}", self.cursor, self.cursor + size_to_read - 1),
        );

        if let Some(window) = web_sys::window() {
            let response = window.fetch_with_request(&request);
        }

        todo!()
    }
}

impl Seek for WasmHttpStream {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        todo!()
    }
}
