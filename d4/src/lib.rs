/**
 * This is the Rust implementation of the D4 file format.
 **/
mod chrom;
mod d4file;
mod dict;
mod header;

pub mod ptab;
pub mod stab;
pub mod task;

pub use chrom::Chrom;
pub use d4file::{find_tracks_in_file, D4FileBuilder, D4FileMerger, D4FileWriter, D4TrackReader};
pub use dict::Dictionary;

pub use header::Header;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
