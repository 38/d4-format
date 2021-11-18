mod d4file;
mod iter;
mod builder;

use pyo3::prelude::*;
use d4file::D4File;
use iter::D4Iter;
use builder::{D4Builder, D4Writer};

#[pymodule]
pub fn pyd4(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<D4File>()?;
    m.add_class::<D4Iter>()?;
    m.add_class::<D4Builder>()?;
    m.add_class::<D4Writer>()?;
    Ok(())
}
