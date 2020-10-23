pub use crate::chrom::Chrom;
use crate::dict::Dictionary;
use serde_derive::{Deserialize, Serialize};

/// The D4 file header struct, this is store in the ".metadata" stream in a D4 file in JSON format
#[derive(Serialize, Deserialize)]
pub struct Header {
    pub(crate) chrom_list: Vec<Chrom>,
    pub(crate) dictionary: Dictionary,
}

impl Header {
    /// Build a new header
    pub fn new() -> Self {
        Header {
            chrom_list: vec![],
            dictionary: Dictionary::SimpleRange { low: 0, high: 64 },
        }
    }

    /// Assign dictionary to the header
    pub fn dictionary(&self) -> &Dictionary {
        &self.dictionary
    }

    /// Get a reference to current chromosome list
    pub fn chrom_list(&self) -> &[Chrom] {
        self.chrom_list.as_ref()
    }

    /// Set new chromosome list
    pub fn set_chrom_list(&mut self, new_value: Vec<Chrom>) {
        self.chrom_list = new_value
    }

    pub(crate) fn primary_table_size(&self) -> usize {
        let bit_width = self.dictionary.bit_width();
        self.chrom_list
            .iter()
            .map(|chr| (chr.size as usize * bit_width + 7) / 8)
            .sum()
    }
}
