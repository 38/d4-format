mod reader;
mod writer;
mod merger;
mod track;

use std::{
    io::Result,
    path::{Path, PathBuf},
};

use d4_framefile::{Directory, EntryKind};
pub use reader::D4FileReader;
pub use writer::{D4FileBuilder, D4FileWriter};
pub use merger::D4FileMerger;
pub use track::{MultiTrackPartitionReader, MultiTrackReader, MultiTrackRow};

/// The D4 magic number
pub const FILE_MAGIC_NUM: &[u8] = b"d4\xdd\xdd";

pub fn find_tracks_in_file<Pat: Fn(Option<&Path>) -> bool, PathType: AsRef<Path>>(
    path: PathType,
    pattern: Pat,
    buf: &mut Vec<PathBuf>,
) -> Result<()> {
    let fp = reader::open_file_and_validate_header(path)?;
    let file_root = Directory::open_root(fp, 8)?;
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
