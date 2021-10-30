use std::{io::Result, path::Path};

#[repr(packed)]
pub struct SecondaryTableFrameIndexItem {
	chrom_id: u32,
	start_pos: u32,
	end_pos: u32,
	/// This offset is defined as the offset from the begining of the start of the stream's parent container
	offset: u64,
}

pub struct SecondaryTableFrameIndex {}
