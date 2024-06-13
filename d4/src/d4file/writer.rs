use d4_framefile::Directory;
use d4_hts::BamFile;
use std::fs::{File, OpenOptions};
use std::io::{Result, Write};
use std::path::{Path, PathBuf};

use crate::chrom::Chrom;
use crate::dict::Dictionary;
use crate::header::{Denominator, Header};
use crate::ptab::{BitArrayWriter, PTablePartitionWriter, PrimaryTableWriter};
use crate::stab::{RangeRecord, SecondaryTableWriter, SparseArrayWriter};

use super::FILE_MAGIC_NUM;

/// Create a D4 file
pub struct D4FileWriter<
    PT: PrimaryTableWriter = BitArrayWriter,
    ST: SecondaryTableWriter = SparseArrayWriter<RangeRecord>,
> {
    _file_root: Directory<File>,
    pub(crate) header: Header,
    pub(crate) p_table: PT,
    pub(crate) s_table: Option<ST>,
}

pub trait D4FileWriterExt {
    type Partition;
}

impl<PT: PrimaryTableWriter, ST: SecondaryTableWriter> D4FileWriterExt for D4FileWriter<PT, ST> {
    type Partition = (PT::Partition, ST::Partition);
}

impl<PT: PrimaryTableWriter, ST: SecondaryTableWriter> D4FileWriter<PT, ST> {
    /// Split the file writer into parts for parallel writing
    pub fn parallel_parts(
        &mut self,
        size_limit: Option<usize>,
    ) -> Result<
        Vec<(
            <PT as PrimaryTableWriter>::Partition,
            <ST as SecondaryTableWriter>::Partition,
        )>,
    > {
        let p_table_parts = self.p_table.split(&self.header, size_limit)?;
        let partitions: Vec<_> = p_table_parts.iter().map(|part| part.region()).collect();
        let s_table_parts = self.s_table.as_mut().unwrap().split(&partitions)?;
        let ret = p_table_parts.into_iter().zip(s_table_parts).collect();
        Ok(ret)
    }

    /// Enable the secondary table compression
    pub fn enable_secondary_table_compression(&mut self, level: u32) {
        self.s_table
            .as_mut()
            .unwrap()
            .enable_deflate_encoding(level);
    }
}

impl<PT: PrimaryTableWriter, ST: SecondaryTableWriter> Drop for D4FileWriter<PT, ST> {
    fn drop(&mut self) {
        drop(self.s_table.take());
    }
}

#[allow(clippy::type_complexity)]
/// The builder that is used to build a D4 file
pub struct D4FileBuilder {
    path: PathBuf,
    chrom_info: Vec<Chrom>,
    dict: Dictionary,
    chrom_filter: Box<dyn Fn(&str, usize) -> bool>,
    denominator: Denominator,
}

impl D4FileBuilder {
    /// Create a new D4 file builder
    pub fn new<P: AsRef<Path>>(path: P) -> D4FileBuilder {
        Self {
            path: path.as_ref().to_owned(),
            chrom_info: vec![],
            dict: Dictionary::SimpleRange { low: 0, high: 64 },
            chrom_filter: Box::new(|_, _| true),
            denominator: Default::default(),
        }
    }

    pub fn set_denominator(&mut self, value: f64) -> &mut Self {
        self.denominator = Denominator::Value(value);
        self
    }

    /// Set a chromosome filter lambda, this will be used to determine if the chromosome should be
    /// in the output
    pub fn set_filter<T: Fn(&str, usize) -> bool + 'static>(&mut self, filter: T) -> &mut Self {
        self.chrom_filter = Box::new(filter);
        self
    }

    /// Append chromosomes to the chrom list
    pub fn append_chrom<I: Iterator<Item = Chrom>>(&mut self, chrom_it: I) -> &mut Self {
        for chrom in chrom_it {
            if (self.chrom_filter)(chrom.name.as_str(), chrom.size) {
                self.chrom_info.push(chrom);
            }
        }
        self
    }

    /// Load the chromosome information from a input BAM file
    pub fn load_chrom_info_from_bam<P: AsRef<Path>>(&mut self, path: P) -> Result<&mut Self> {
        let bam_file = BamFile::open(path)
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "Bam Read error"))?;
        self.chrom_info.clear();
        Ok(
            self.append_chrom(bam_file.chroms().iter().map(|(n, s)| Chrom {
                name: n.to_string(),
                size: *s,
            })),
        )
    }

    /// Set the file's dictionary
    pub fn set_dictionary(&mut self, dict: Dictionary) -> &mut Self {
        self.dict = dict;
        self
    }

    /// Get a reference to the dictionary
    pub fn dictionary(&self) -> &Dictionary {
        &self.dict
    }

    pub(crate) fn write_d4_header<P: AsRef<Path>>(path: P) -> Result<Directory<File>> {
        let mut target = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        target.write_all(FILE_MAGIC_NUM)?;
        target.write_all(&[0, 0, 0, 0])?;
        Directory::make_root(target)
    }

    /// Create the D4 file writer for this file
    pub fn create<PT: PrimaryTableWriter, ST: SecondaryTableWriter>(
        &mut self,
    ) -> Result<D4FileWriter<PT, ST>> {
        let mut directory = Self::write_d4_header(self.path.as_path())?;
        let mut metadata_stream = directory.create_stream(".metadata", 512)?;
        let header = Header {
            chrom_list: std::mem::take(&mut self.chrom_info),
            dictionary: self.dict.clone(),
            denominator: self.denominator,
        };

        metadata_stream.write(serde_json::to_string(&header).unwrap().as_bytes())?;

        drop(metadata_stream);

        let p_table = PT::create(&mut directory, &header)?;
        let s_table = Some(ST::create(&mut directory, &header)?);

        Ok(D4FileWriter {
            _file_root: directory,
            header,
            p_table,
            s_table,
        })
    }
}
