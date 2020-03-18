mod reader;
mod writer;

pub use reader::D4FileReader;
pub use writer::{D4FileBuilder, D4FileWriter};

const FILE_MAGIC_NUM: &'static [u8] = b"d4\xdd\xdd";
