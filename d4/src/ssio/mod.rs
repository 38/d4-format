/// # The Seek-able Stream Input and Output
/// Currently normal local D4 file reading is heavily relies on mapped IO
/// This gives D4 amazing performance, however, when we want to deal with
/// some other types of data sources rather than the local file system,
/// we need a reader that is purely written with random IO APIs with seek and
/// read.
/// This module is used in this purpose

#[cfg(feature = "http_reader")]
pub mod http;

mod reader;
mod table;
mod view;

pub use reader::D4TrackReader;

#[test]
fn test_open_hg002() {
    //use std::fs::File;
    env_logger::init();
    let hg002_file =
        http::HttpReader::new("https://home.chpc.utah.edu/~u0875014/hg002.d4").unwrap();
    let mut reader = D4TrackReader::from_reader(hg002_file, None).unwrap();
    let view = reader.get_view("1", 19250459, 19450000).unwrap();
    for read_result in view {
        let (pos, value) = read_result.unwrap();
        println!("{} {}", pos, value);
    }
}
