use d4::ptab::{DecodeResult, Decoder};
use d4::stab::SecondaryTablePartReader;
use d4::task::{Histogram, Mean, Task, TaskContext};
use d4::Chrom;
use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PyString, PyTuple};
use rayon::prelude::*;
use crate::ReaderWrapper;

use super::D4Iter;

/// Python object for reading a D4 file
#[pyclass(subclass)]
pub struct D4File {
    path: String,
}

impl D4File {
    pub(crate) fn open(&self) -> PyResult<ReaderWrapper> {
        ReaderWrapper::open(self.path.as_str())
    }

    fn parse_range_spec(
        chroms: &[Chrom],
        regions: &PyList,
    ) -> PyResult<Vec<(String, u32, u32)>> {
        let mut spec = vec![];
        for item in regions.iter() {
            let (chr, begin, end) = if let Ok(chr) = item.downcast::<PyString>() {
                (chr, None, None)
            } else if let Ok(tuple) = item.downcast::<PyTuple>() {
                let tuple = tuple.as_slice();
                let chr = tuple[0].downcast()?;
                let begin = tuple
                    .get(1)
                    .map(|x| x.downcast::<PyInt>().ok())
                    .unwrap_or(None);
                let end = tuple
                    .get(2)
                    .map(|x| x.downcast::<PyInt>().ok())
                    .unwrap_or(None);
                (chr, begin, end)
            } else {
                return Err(
                    std::io::Error::new(std::io::ErrorKind::Other, "Invalid range spec").into(),
                );
            };
            let chr = chr.to_str()?;
            let chrom = chroms.iter().find(|x| x.name == chr);
            if chrom.is_none() {
                let msg = format!("Chrom {} doesn't exists", chr);
                return Err(std::io::Error::new(std::io::ErrorKind::Other, msg).into());
            }
            let (begin, end) = match (begin, end) {
                (Some(start), None) => (start.extract()?, chrom.unwrap().size as u32),
                (Some(start), Some(end)) => (start.extract()?, end.extract()?),
                _ => (0, chrom.unwrap().size as u32),
            };
            spec.push((chr.to_string(), begin, end));
        }
        Ok(spec)
    }
}

#[pymethods]
impl D4File {
    /// new(path)
    /// --
    /// 
    /// Open a new D4 file for read
    ///
    /// Path: path to the D4 file
    #[new]
    pub fn new(path: &str) -> PyResult<Self> {
        let ret = Self {
            path: path.to_string(),
        };
        ret.open()?;
        Ok(ret)
    }

    /// list_tracks()
    /// --
    /// 
    /// List all the tracks living in this file.
    pub fn list_tracks(&self) -> PyResult<Vec<String>> {        
        let mut tracks = Vec::new();
        if self.path.starts_with("http://") || self.path.starts_with("https://") {
            let path = if let Some(sep) = self.path.rfind('#') {
                &self.path[..sep]
            } else {
                &self.path
            };
            let reader = d4::ssio::http::HttpReader::new(path)?;
            d4::find_tracks(reader, |_| true, &mut tracks)?;
        } else {
            d4::find_tracks_in_file(&self.path, |_| true, &mut tracks)?;
        }
        Ok(tracks
            .into_iter()
            .map(|x| x.to_string_lossy().to_string())
            .collect())
    }

    /// is_remote_file()
    /// --
    /// 
    /// Check if the file is on remote server or local disk
    pub fn is_remote_file(&self) -> PyResult<bool> {
        Ok(self.path.starts_with("http://") || self.path.starts_with("https://"))
    }

    pub fn get_track_specifier(&self, track: &str) -> PyResult<String> {
        Ok(if self.path.starts_with("http://") || self.path.starts_with("https://") {
            format!("{}#{}", self.path, track)
        } else {
            format!("{}:{}", self.path, track)
        })
    }

    /// open_track(name)
    /// --
    /// 
    /// Open a track with the specified name.
    pub fn open_track(&self, track: &str) -> PyResult<Self> {
        let path = self.get_track_specifier(track)?;
        let ret = Self{ path };
        ret.open()?;
        Ok(ret)
    }

    /// chroms()
    /// --
    /// 
    /// Returns a list of chromosomes defined in the D4 file
    pub fn chroms(&self) -> PyResult<Vec<(String, usize)>> {
        Ok(self
            .open()?
            .get_chroms()
            .iter()
            .map(|x| (x.name.clone(), x.size))
            .collect())
    }

    /// histogram(regions, min, max)
    /// --
    /// 
    /// Returns the hisgoram of values in the given regions
    ///
    /// regions: The list of regions we are asking
    /// min: The smallest bucket of the histogram
    /// max: The biggest bucket of the histogram
    ///
    /// The return value is a list of histograms (including the conut of below min and above max
    /// items)
    pub fn histogram(
        &self,
        regions: &pyo3::types::PyList,
        min: i32,
        max: i32,
    ) -> PyResult<Vec<(Vec<(i32, u32)>, u32, u32)>> {
        let mut input = self.open()?.into_local_reader()?;
        let spec = Self::parse_range_spec(input.header().chrom_list(), regions)?
            .into_iter()
            .map(|(chr, beg, end)| Histogram::with_bin_range(&chr, beg, end, min..max))
            .collect();
        let result = TaskContext::new(&mut input, spec)?.run();
        let mut buf = vec![];
        for item in &result {
            let (below, hist, above) = item.output;
            let hist: Vec<_> = hist
                .iter()
                .enumerate()
                .map(|(a, &b)| (a as i32, b))
                .collect();
            buf.push((hist, *below, *above));
        }
        Ok(buf)
    }

    /// mean(regions)
    /// --
    /// 
    /// Compute the mean dpeth for the given region
    pub fn mean(&self, regions: &pyo3::types::PyList) -> PyResult<Vec<f64>> {
        let mut input = self.open()?;
        let spec = Self::parse_range_spec(input.get_chroms(), regions)?;
        if let Ok(input) = input.as_local_reader_mut() {
            let result = Mean::create_task(input, &spec)?.run();
            let mut buf = vec![];
            for item in &result {
                buf.push(*item.output);
            }
            Ok(buf)
        } else {
            let mut input = input.into_remote_reader()?;
            let index = input.load_data_index::<d4::index::Sum>()?;
            let mut ret = Vec::with_capacity(spec.len());
            for (chr, begin, end) in spec {
                let index_res = index.query(chr.as_str(), begin, end).unwrap();
                let sum_res = index_res.get_result(&mut input)?;
                let mean = sum_res.mean(index_res.query_size());
                ret.push(mean)
            }
            Ok(ret)
        }
    }

    pub fn load_values_to_buffer(
        &self,
        chr: &str,
        left: u32,
        right: u32,
        buf: i64,
    ) -> PyResult<()> {
        let mut reader = self.open()?;
        if let Ok(local) = reader.as_local_reader_mut() {
            let partition = local.split(Some(100_0000))?;

            let chr = chr.to_string();

            partition
                .into_par_iter()
                .for_each(move |(mut ptab, mut stab)| {
                    let (part_chr, begin, end) = ptab.region();
                    let part_chr = part_chr.to_string();
                    let mut pd = ptab.to_codec();
                    let (from, to) = if part_chr != chr {
                        return;
                    } else {
                        (left.max(begin), right.min(end))
                    };
                    if from >= to {
                        return;
                    }
                    let target = unsafe {
                        std::slice::from_raw_parts_mut(
                            ((buf as u64) + std::mem::size_of::<i32>() as u64 * ((from - left) as u64))
                                as *mut i32,
                            (to - from) as usize,
                        )
                    };
                    pd.decode_block(from as usize, (to - from) as usize, |pos, value| {
                        let value = match value {
                            DecodeResult::Definitely(value) => value,
                            DecodeResult::Maybe(value) => {
                                if let Some(st_value) = stab.decode(pos as u32) {
                                    st_value
                                } else {
                                    value
                                }
                            }
                        };
                        target[pos - from as usize] = value;
                    });
                });
        } else {
            let mut remote = reader.into_remote_reader()?;
            let view = remote.get_view(chr, left, right)?;
            let target = unsafe {
                std::slice::from_raw_parts_mut(
                    buf as u64 as *mut i32,
                    (right - left) as usize,
                )
            };
            for value in view {
                let (pos, idx) = value?;
                target[(pos - left) as usize] = idx;
            }
        }
        Ok(())
    }

    /// value_iter()
    /// --
    /// 
    /// Returns a value iterator that iterates over the given region
    pub fn value_iter(&self, chr: &str, left: u32, right: u32) -> PyResult<D4Iter> {
        if self.is_remote_file()? {
            let inner = self.open()?.into_remote_reader()?;
            D4Iter::from_remote_reader(inner, chr, left, right)
        } else {
            let inner = self.open()?.into_local_reader()?;
            D4Iter::from_local_reader(inner, chr, left, right)
        }
    }
}
