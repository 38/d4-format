use d4::{Chrom, D4FileBuilder, D4FileWriter, D4FileWriterExt, Dictionary, ptab::PTablePartitionWriter, stab::SecondaryTablePartWriter};
use pyo3::{exceptions::PyKeyError, prelude::*};
use rayon::prelude::*;

#[pyclass(subclass)]
pub struct D4Builder {
    genome_size: Vec<(String, usize)>,
    dictionary: Dictionary,
    compression: Option<u32>,
}

struct WriterPartHandle {
    chrom: String,
    end: u32,
    frontier: u32,
    inner: <D4FileWriter as D4FileWriterExt>::Partition,
}

impl WriterPartHandle {
    fn flush(&mut self) -> PyResult<()> {
        let mut p_enc = self.inner.0.make_encoder();
        for pos in self.frontier..self.end {
            if !p_enc.encode(pos as usize, 0) {
                self.inner.1.encode(pos, 0)?;
            }
        }
        Ok(())
    }
    fn encode(&mut self, begin: u32, data: &[i32]) -> PyResult<()> {
        let mut p_enc = self.inner.0.make_encoder();
        for pos in self.frontier..begin {
            if !p_enc.encode(pos as usize, 0) {
                self.inner.1.encode(pos, 0)?;
            }
        }
        for (pos, &data) in (begin..self.end).zip(data.iter()) {
            if !p_enc.encode(pos as usize, data) {
                self.inner.1.encode(pos, data)?;
            }
        }
        self.frontier = begin + (data.len() as u32).min(self.end - begin);
        Ok(())
    }
    unsafe fn get_data_view(
        &self, 
        chr: &str, 
        data_begin: u32, 
        data: *const i32, 
        count: usize
    ) -> Option<(u32, u64, usize)> {
        if self.chrom != chr {
            return None;
        }
        let end = (data_begin + count as u32).min(self.end);
        let begin = self.frontier.max(data_begin);
        if begin >= end {
            return None;
        }
        Some((
                begin,
                data.offset((begin - data_begin) as isize) as u64,
                (end - begin) as usize
        ))
    }
}

#[pyclass(subclass)]
pub struct D4Writer {
    writer_obj: Option<D4FileWriter>,
    parts: Vec<WriterPartHandle>,
}

#[pymethods]
impl D4Writer {
    fn close(&mut self) {
        let parts = std::mem::replace(&mut self.parts, Vec::new());
        parts.into_par_iter().for_each(|mut part| part.flush().unwrap());
        self.writer_obj.take();
    }
    fn write(&mut self, chr: &str, start_pos: u32, data_addr: i64, count: usize) -> PyResult<()> {
        let active_parts: Vec<_> = self.parts
            .iter_mut()
            .filter_map(|part| unsafe {
                part
                    .get_data_view(chr, start_pos, data_addr as *const i32, count)
                    .and_then(|view| Some((part, view)))
            })
            .collect();
        active_parts
            .into_par_iter()
            .for_each(|(part, view)| {
                let data = unsafe {
                    std::slice::from_raw_parts(view.1 as *const i32, view.2)
                };
                part.encode(view.0, data).unwrap();
            });
        Ok(())
    }
}


#[pymethods]
impl D4Builder {
    #[new]
    fn new() -> PyResult<Self> {
        let dictionary = Dictionary::new_simple_range_dict(0, 64)?;
        Ok(Self {
            genome_size: Vec::new(),
            dictionary,
            compression: None,
        })
    }
    fn dict_range(&mut self, low: i32, high: i32) -> PyResult<()> {
        self.dictionary = Dictionary::new_simple_range_dict(low, high)?;
        Ok(())
    }
    fn add_seq(&mut self, name: &str, size: usize) -> PyResult<()> {
        if self.genome_size.iter().any(|(x, _)| name == x) {
            return Err(PyKeyError::new_err("Sequence is already defined"));
        }
        self.genome_size.push((name.to_string(), size));
        Ok(())
    }
    fn dup_dict(&mut self, that: &super::D4File) -> PyResult<()> {
        let reader = that.open()?.into_local_reader()?;
        self.dictionary = reader.header().dictionary().clone();
        Ok(())
    }
    fn dup_seqs(&mut self, that: &super::D4File) -> PyResult<()> {
        let reader = that.open()?.into_local_reader()?;
        self.genome_size = reader.header()
            .chrom_list()
            .iter()
            .map(|chrom| (chrom.name.clone(), chrom.size))
            .collect();
        Ok(())
    }
    fn set_compression(&mut self, level: i32) -> PyResult<()> {
        if level < 0 {
            self.compression = None;
        } else {
            self.compression = Some(level as u32);
        }
        Ok(())
    }
    fn into_writer(&mut self, path: &str) -> PyResult<D4Writer> {
        let mut writer : D4FileWriter = D4FileBuilder::new(path)
            .set_dictionary(self.dictionary.clone())
            .append_chrom(self.genome_size.iter().map(|(name, size)| {
                Chrom { name: name.to_string(), size: *size }
            }))
            .create()?;
        
        if let Some(level) = self.compression {
            writer.enable_secondary_table_compression(level);
        }

        let parts = writer.parallel_parts(Some(100_0000))?.into_iter()
            .map(|(p,s)| {
                let (chr, begin, end) = p.region();
                let frontier = begin;
                WriterPartHandle {
                    chrom: chr.to_string(),
                    end,
                    frontier,
                    inner: (p, s),
                }
            })
            .collect();
        Ok(D4Writer{
            writer_obj: Some(writer),
            parts,
        })
    }
}