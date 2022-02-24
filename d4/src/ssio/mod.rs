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

pub use reader::{D4MatrixReader, D4TrackReader};
pub use view::D4TrackView;
