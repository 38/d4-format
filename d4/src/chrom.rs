use serde_derive::{Deserialize, Serialize};

/// The chromosome information type
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Chrom {
    /// The human-readable name for this chromosome
    pub name: String,
    /// The size of current chromosome
    pub size: usize,
}
