pub use crate::chrom::Chrom;
use crate::dict::Dictionary;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct Header {
    pub(crate) chrom_list: Vec<Chrom>,
    pub(crate) dictionary: Dictionary,
}

impl Header {
    pub fn new() -> Self {
        Header {
            chrom_list: vec![],
            dictionary: Dictionary::SimpleRange { low: 0, high: 64 },
        }
    }
    pub fn dictionary(&self) -> &Dictionary {
        &self.dictionary
    }
    pub fn chrom_list(&self) -> &[Chrom] {
        self.chrom_list.as_ref()
    }
    pub(crate) fn primary_table_size(&self) -> usize {
        let bit_width = self.dictionary.bit_width();
        self.chrom_list
            .iter()
            .map(|chr| (chr.size as usize * bit_width + 7) / 8)
            .sum()
    }
}
