use std::io::{Error, Read, Result, Seek, SeekFrom};

use reqwest::{blocking::Client, IntoUrl, Url};

pub struct HttpReader {
    client: Client,
    url: Url,
    size: usize,
    cursor: usize,
}

fn map_result<T, E: std::error::Error + Sync + Send + 'static>(
    input: std::result::Result<T, E>,
) -> Result<T> {
    input.map_err(|e| Error::new(std::io::ErrorKind::Other, e))
}

impl HttpReader {
    pub fn new<U: IntoUrl>(url: U) -> Result<Self> {
        let url = map_result(url.into_url())?;
        let client = Client::new();

        let size: usize = {
            let response = map_result(
                client
                    .head(url.clone())
                    .header("connection", "keep-alive")
                    .send(),
            )?;
            let size_text = response.headers()["content-length"].as_bytes();
            let size_text = String::from_utf8_lossy(size_text);
            map_result(size_text.parse())?
        };

        Ok(Self {
            client,
            url,
            size,
            cursor: 0,
        })
    }
}

impl Read for HttpReader {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let to = self.size.min(self.cursor + buf.len() - 1);
        if self.cursor > to {
            return Ok(0);
        }
        let mut request = map_result(
            self.client
                .get(self.url.as_ref())
                .header("range", format!("bytes={}-{}", self.cursor, to))
                .header("connection", "keep-alive")
                .send(),
        )?;
        let sz = request.read(buf)?;
        self.cursor += sz;
        Ok(sz)
    }
}

impl Seek for HttpReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> Result<u64> {
        let delta = match pos {
            SeekFrom::Current(delta) => delta,
            SeekFrom::End(delta) => {
                self.cursor = self.size;
                delta
            }
            SeekFrom::Start(delta) => {
                self.cursor = 0;
                delta as i64
            }
        };
        if delta > 0 {
            self.cursor += delta as usize;
            self.cursor = self.cursor.min(self.size);
        } else {
            if self.cursor < (-delta) as usize {
                self.cursor = 0;
            } else {
                self.cursor -= (-delta) as usize;
            }
        }
        Ok(self.cursor as u64)
    }
}
