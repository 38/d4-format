use clap::ArgMatches;
use d4::{Chrom, Dictionary};
use log::warn;
use rayon::ThreadPoolBuildError;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

pub type AppResult<T> = Result<T, Box<dyn std::error::Error>>;

pub enum InputType {
    Alignment,
    BedGraph,
    BiwWig,
    Unsupported,
}

impl InputType {
    pub fn detect(path: &Path) -> InputType {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();
        match ext.as_str() {
            "sam" | "bam" | "cram" => Self::Alignment,
            "bw" | "bigwig" => Self::BiwWig,
            "txt" | "bedgraph" => Self::BedGraph,
            _ => Self::Unsupported,
        }
    }
}

pub fn parse_genome_file<P: AsRef<Path>>(file: P) -> std::io::Result<Vec<Chrom>> {
    let file = BufReader::new(File::open(file)?);
    Ok(file
        .lines()
        .filter_map(|line| {
            if let Ok(line) = line {
                let tokenized: Vec<_> = line.split(|c| c == '\t').take(2).collect();
                if tokenized.len() == 2 {
                    if let Ok(size) = tokenized[1].parse() {
                        return Some((tokenized[0].to_owned(), size));
                    }
                }
            }
            None
        })
        .map(|(name, size)| Chrom { name, size })
        .collect())
}
pub fn parse_bed_file<P: AsRef<Path>>(
    file: P,
) -> std::io::Result<impl Iterator<Item = (String, u32, u32, f64)>> {
    let file = BufReader::new(File::open(file)?);
    let mut warned = false;
    Ok(file.lines().filter_map(move |line| {
        if let Ok(line) = line {
            let tokenized: Vec<_> = line.split(|c| c == '\t').take(4).collect();
            if tokenized.len() == 3 {
                if let Ok(pos) = tokenized[1].parse() {
                    if let Ok(dep) = tokenized[2].parse() {
                        return Some((tokenized[0].to_owned(), pos, pos + 1, dep));
                    }
                }
            } else if tokenized.len() == 4 {
                if let Ok(begin) = tokenized[1].parse() {
                    if let Ok(end) = tokenized[2].parse() {
                        if let Ok(dep) = tokenized[3].parse() {
                            return Some((tokenized[0].to_owned(), begin, end, dep));
                        }
                    }
                }
            } else if !warned && !line.starts_with('#') {
                warn!("Invalid input line: {}", line.trim_end());
                warned = true;
            }
        }
        None
    }))
}

pub fn make_dictionary(
    range_spec: Option<&str>,
    file_spec: Option<&str>,
) -> std::io::Result<Dictionary> {
    if let Some(spec) = range_spec {
        let pattern = regex::Regex::new(r"(?P<from>\d*)-(?P<to>\d*)").unwrap();
        if let Some(caps) = pattern.captures(spec) {
            let from = caps.name("from").unwrap().as_str().parse().unwrap();
            let to = caps.name("to").unwrap().as_str().parse().unwrap();
            return d4::Dictionary::new_simple_range_dict(from, to);
        }
    }
    if let Some(spec) = file_spec {
        let fp = File::open(spec)?;
        return d4::Dictionary::new_dictionary_from_file(fp);
    }
    d4::Dictionary::new_simple_range_dict(0, 64)
}

pub fn setup_thread_pool(matches: &ArgMatches) -> Result<(), ThreadPoolBuildError> {
    if let Some(threads) = matches.value_of("threads") {
        if let Ok(threads) = threads.parse() {
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build_global()?;
        }
    }
    Ok(())
}

pub fn check_reference_consistency<'a, I: Iterator<Item = &'a [Chrom]>>(iter: I) -> bool {
    let chrom_matrix: Vec<_> = iter.collect();

    if chrom_matrix.len() > 1 {
        for (i, chrom) in chrom_matrix[0].iter().enumerate() {
            #[allow(clippy::needless_range_loop)]
            for j in 1..chrom_matrix.len() {
                if &chrom_matrix[j][i] != chrom {
                    return false;
                }
            }
        }
    }

    true
}
