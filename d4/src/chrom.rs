use serde_derive::{Deserialize, Serialize};

/// The information of a chromosome
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Chrom {
    /// The human-readable name for this chromosome
    pub name: String,
    /// The size of current chromosome
    pub size: usize,
}
