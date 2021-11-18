use d4::ptab::{DecodeResult, Decoder};
use d4::stab::SecondaryTablePartReader;
use d4::task::{Histogram, Mean, Task, TaskContext};
use d4::D4TrackReader;
use pyo3::class::iter::{IterNextOutput, PyIterProtocol};
use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PyString, PyTuple};
use rayon::prelude::*;
use std::io::Result;

/// Python object for reading a D4 file
#[pyclass(subclass)]
pub struct D4File {
    path: String,
}

/// Value iterator for D4 file
#[pyclass]
pub struct D4Iter {
    _inner: D4TrackReader,
    iter: Box<dyn Iterator<Item = i32> + Send + 'static>,
}
impl D4File {
    fn open(&self) -> Result<D4TrackReader> {
        D4TrackReader::open(&self.path)
    }

    fn parse_range_spec(
        input: &D4TrackReader,
        regions: &PyList,
    ) -> PyResult<Vec<(String, u32, u32)>> {
        let chroms = input.header().chrom_list();
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

#[pyproto]
impl PyIterProtocol for D4Iter {
    fn __iter__(slf: PyRefMut<Self>) -> Result<PyRefMut<Self>> {
        Ok(slf)
    }
    fn __next__(mut slf: PyRefMut<Self>) -> IterNextOutput<i32, &'static str> {
        if let Some(next) = slf.iter.next() {
            IterNextOutput::Yield(next)
        } else {
            IterNextOutput::Return("Ended")
        }
    }
}

#[pymethods]
impl D4File {
    /// Open a new D4 file for read
    ///
    /// Path: path to the D4 file
    #[new]
    pub fn new(path: &str) -> PyResult<Self> {
        let _inner: D4TrackReader = D4TrackReader::open(path)?;
        Ok(Self {
            path: path.to_string(),
        })
    }

    pub fn list_tracks(&self) -> PyResult<Vec<String>> {
        let mut tracks = Vec::new();
        d4::find_tracks_in_file(&self.path, |_| true, &mut tracks)?;
        Ok(tracks
            .into_iter()
            .map(|x| x.to_string_lossy().to_string())
            .collect())
    }

    pub fn open_track(&self, track: &str) -> PyResult<Self> {
        let path = format!("{}:{}", self.path, track);
        let _inner: D4TrackReader = D4TrackReader::open(&path)?;
        Ok(Self { path })
    }

    /// Returns a list of chromosomes defined in the D4 file
    pub fn chroms(&self) -> PyResult<Vec<(String, usize)>> {
        Ok(self
            .open()?
            .header()
            .chrom_list()
            .iter()
            .map(|x| (x.name.clone(), x.size))
            .collect())
    }

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
        let mut input = self.open()?;
        let spec = Self::parse_range_spec(&input, regions)?
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

    /// Compute the mean dpeth for the given region
    pub fn mean(&self, regions: &pyo3::types::PyList) -> PyResult<Vec<f64>> {
        let mut input = self.open()?;
        let spec = Self::parse_range_spec(&input, regions)?;
        let result = Mean::create_task(&mut input, &spec)?.run();
        let mut buf = vec![];
        for item in &result {
            buf.push(*item.output);
        }
        Ok(buf)
    }

    pub fn load_values_to_buffer(
        &self,
        chr: &str,
        left: u32,
        right: u32,
        buf: i64,
    ) -> PyResult<()> {
        let mut inner = self.open()?;
        let partition = inner.split(Some(100_0000))?;

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
                let target = unsafe {
                    std::slice::from_raw_parts_mut(
                        ((buf as u64) + std::mem::size_of::<i32>() as u64 * (from as u64))
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
        Ok(())
    }

    /// Returns a value iterator that iterates over the given region
    pub fn value_iter(&self, chr: &str, left: u32, right: u32) -> PyResult<D4Iter> {
        let mut inner = self.open()?;
        let partition = inner.split(None)?;

        let chr = chr.to_string();

        let iter = partition
            .into_iter()
            .map(move |(mut ptab, mut stab)| {
                let (part_chr, begin, end) = ptab.region();
                let part_chr = part_chr.to_string();
                let pd = ptab.to_codec();
                (if part_chr != chr {
                    0..0
                } else {
                    left.max(begin)..right.min(end)
                })
                .map(move |pos| match pd.decode(pos as usize) {
                    DecodeResult::Definitely(value) => value,
                    DecodeResult::Maybe(value) => {
                        if let Some(st_value) = stab.decode(pos) {
                            st_value
                        } else {
                            value
                        }
                    }
                })
            })
            .flatten();
        Ok(D4Iter {
            _inner: inner,
            iter: Box::new(iter),
        })
    }
}

#[pymodule]
pub fn pyd4(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<D4File>()?;
    m.add_class::<D4Iter>()?;
    Ok(())
}
