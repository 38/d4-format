use std::io::{Read, Result};

pub use crate::chrom::Chrom;
use crate::dict::Dictionary;
use serde_derive::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy)]
pub enum Denominator {
    One,
    Value(f64),
}

impl Default for Denominator {
    fn default() -> Self {
        Self::One
    }
}

/// The D4 file header struct, this is store in the ".metadata" stream in a D4 file in JSON format
#[derive(Serialize, Deserialize)]
pub struct Header {
    pub(crate) chrom_list: Vec<Chrom>,
    pub(crate) dictionary: Dictionary,
    #[serde(default)]
    pub(crate) denominator: Denominator,
}

impl Default for Header {
    fn default() -> Self {
        Self::new()
    }
}

impl Header {
    pub const HEADER_STREAM_NAME: &'static str = ".metadata";
    /// Build a new header
    pub fn new() -> Self {
        Header {
            chrom_list: vec![],
            dictionary: Dictionary::SimpleRange { low: 0, high: 64 },
            denominator: Denominator::One,
        }
    }

    pub fn read<R: Read>(mut input: R) -> Result<Self> {
        let header_data = {
            let mut ret = vec![];
            loop {
                let mut buf = [0u8; 4096];
                let sz = input.read(&mut buf)?;
                let mut actual_size = sz;
                while actual_size > 0 && buf[actual_size - 1] == 0 {
                    actual_size -= 1;
                }
                ret.extend_from_slice(&buf[..actual_size]);
                if actual_size != sz {
                    break ret;
                }
            }
        };
        let header = serde_json::from_str(String::from_utf8_lossy(&header_data).as_ref())
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Invalid Metadata"))?;
        Ok(header)
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

    pub fn set_denominator(&mut self, value: f64) {
        self.denominator = Denominator::Value(value);
    }

    pub fn is_integral(&self) -> bool {
        matches!(self.denominator, Denominator::One)
    }

    pub fn get_denominator(&self) -> f64 {
        match self.denominator {
            Denominator::One => 1.0,
            Denominator::Value(v) => v,
        }
    }

    pub(crate) fn primary_table_offset_of_chrom(&self, chrom: &str) -> usize {
        let bw = self.dictionary.bit_width();
        self.chrom_list
            .iter()
            .take_while(|chr| chr.name != chrom)
            .map(|chr| (chr.size * bw + 7) / 8)
            .sum()
    }

    pub fn get_chrom_id(&self, chrom: &str) -> Option<usize> {
        self.chrom_list
            .iter()
            .enumerate()
            .find(|(_, chr)| chr.name == chrom)
            .map(|(id, _)| id)
    }

    pub(crate) fn primary_table_size_of_chrom(&self, chrom: &str) -> usize {
        let bw = self.dictionary.bit_width();
        self.chrom_list
            .iter()
            .find(|chr| chr.name == chrom)
            .map(|chr| (chr.size * bw + 7) / 8)
            .unwrap_or(0)
    }

    pub(crate) fn primary_table_size(&self) -> usize {
        let bit_width = self.dictionary.bit_width();
        self.chrom_list
            .iter()
            .map(|chr| (chr.size * bit_width + 7) / 8)
            .sum()
    }
}
