/**
 * This is the Rust implementation of the D4 file format.
 **/
mod chrom;
mod d4file;
mod dict;
mod header;
#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
pub mod ptab;

pub mod stab;
#[cfg(all(feature = "task", not(target_arch = "wasm32")))]
pub mod task;

pub mod ssio;

pub use chrom::Chrom;

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
pub use d4file::{
    find_tracks_in_file, D4FileBuilder, D4FileMerger, D4FileWriter, D4MatrixReader, D4TrackReader,
    MultiTrackReader,
};

pub use dict::Dictionary;

pub use header::Header;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
