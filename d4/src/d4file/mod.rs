#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
mod merger;

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
mod reader;

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
mod track;

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
mod writer;

use std::{
    fs::File,
    io::{Read, Result, Seek},
    path::{Path, PathBuf},
};

use d4_framefile::{Directory, EntryKind};

#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
mod mapped {
    use super::*;
    pub use merger::D4FileMerger;
    pub use reader::D4TrackReader;
    pub use track::{
        D4MatrixReader, DataScanner, MultiTrackPartitionReader, MultiTrackReader,
    };

    pub use writer::{D4FileBuilder, D4FileWriter, D4FileWriterExt};
}
#[cfg(all(feature = "mapped_io", not(target_arch = "wasm32")))]
pub use mapped::*;

/// The D4 magic number
pub const FILE_MAGIC_NUM: &[u8] = b"d4\xdd\xdd";

pub(crate) fn validate_header<R: Read>(mut reader: R) -> Result<()> {
    let mut signature = [0u8; 8];
    reader.read_exact(&mut signature[..])?;
    if signature[..4] != FILE_MAGIC_NUM[..] {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Invalid D4 File magic number",
        ));
    }
    Ok(())
}

fn open_file_and_validate_header<P: AsRef<Path>>(path: P) -> Result<File> {
    let mut fp = File::open(path.as_ref())?;
    validate_header(&mut fp)?;
    Ok(fp)
}

pub fn find_tracks<Pat: FnMut(Option<&Path>) -> bool, R: Read + Seek>(
    mut input: R,
    mut pattern: Pat,
    buf: &mut Vec<PathBuf>,
) -> Result<()> {
    validate_header(&mut input)?;
    let file_root = Directory::open_root(input, 8)?;
    file_root.recurse(|path, kind| {
        if path.file_name().unwrap_or_default() == ".metadata"
            && kind == EntryKind::Stream
            && pattern(path.parent())
        {
            buf.push(path.parent().map(ToOwned::to_owned).unwrap_or_default());
            return false;
        }
        true
    });

    Ok(())
}

pub fn find_tracks_in_file<Pat: FnMut(Option<&Path>) -> bool, PathType: AsRef<Path>>(
    path: PathType,
    pattern: Pat,
    buf: &mut Vec<PathBuf>,
) -> Result<()> {
    let fp = File::open(path.as_ref())?;
    find_tracks(fp, pattern, buf)
}
