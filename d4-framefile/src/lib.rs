mod blob;
mod directory;
mod randfile;
mod stream;

pub use blob::Blob;
pub use directory::{Directory, EntryKind, OpenResult};
pub use randfile::RandFile;
pub use stream::Stream;

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
pub mod mapped;
//pub mod mode;

use async_trait::async_trait;
#[async_trait]
pub trait AsyncReader {
    fn seek(&mut self, pos: u64) -> std::io::Result<()>;
    async fn read_async(&mut self, buf: &mut [u8]) -> std::io::Result<usize>;
}
