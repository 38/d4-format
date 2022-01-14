mod d4file;
mod iter;
mod builder;

use d4::Chrom;
use pyo3::prelude::*;
use d4file::D4File;
use iter::D4Iter;
use builder::{D4Builder, D4Writer, D4Merger};

enum ReaderWrapper {
    LocalReader(d4::D4TrackReader),
    RemoteReader(d4::ssio::D4TrackReader<d4::ssio::http::HttpReader>)
}

impl ReaderWrapper {
    fn open(path: &str) -> PyResult<ReaderWrapper> {
        if path.starts_with("http://") || path.starts_with("https://") {
            let (path, track) = if let Some(split_pos) = path.rfind('#') {
                    (&path[..split_pos], &path[split_pos + 1..])
            } else {
                (path, "")
            };
            let conn = d4::ssio::http::HttpReader::new(path)?;
            let reader = d4::ssio::D4TrackReader::from_reader(
                conn, 
                if track == "" { None } else { Some(track) }
            )?;
            Ok(Self::RemoteReader(reader))
        } else {
            let local_reader = d4::D4TrackReader::open(path)?;
            Ok(Self::LocalReader(local_reader))
        }
    }
    fn get_chroms(&self) -> &[Chrom] {
        match self {
            Self::LocalReader(local) => local.header().chrom_list(),
            Self::RemoteReader(remote) => &remote.chrom_list(),
        }
    }
    fn as_local_reader_mut(&mut self) -> PyResult<&mut d4::D4TrackReader> {
        match self {
            Self::LocalReader(what) => Ok(what),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other, 
                    "Operation only supports local D4 file"
                ).into());
            } 
        }
    }
    fn into_local_reader(self) -> PyResult<d4::D4TrackReader> {
        match self {
            Self::LocalReader(what) => Ok(what),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other, 
                    "Operation only supports local D4 file"
                ).into());
            } 
        }
    }
    fn into_remote_reader(self) -> PyResult<d4::ssio::D4TrackReader<d4::ssio::http::HttpReader>> {
        match self {
            Self::RemoteReader(what) => Ok(what),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other, 
                    "Operation only supports remote D4 file"
                ).into());
            } 
        }
    }
}

#[pymodule]
pub fn pyd4(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    env_logger::init();
    m.add_class::<D4File>()?;
    m.add_class::<D4Iter>()?;
    m.add_class::<D4Builder>()?;
    m.add_class::<D4Writer>()?;
    m.add_class::<D4Merger>()?;
    Ok(())
}
