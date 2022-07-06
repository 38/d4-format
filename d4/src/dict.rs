use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Result};

#[cfg(feature = "depth_profiler")]
use std::path::Path;

#[cfg(feature = "depth_profiler")]
use d4_hts::{BamFile, DepthIter, Alignment};
#[cfg(feature = "depth_profiler")]
use rand::Rng;
#[cfg(feature = "depth_profiler")]
use rayon::prelude::*;

/// The primary table encoding dictionary.
#[derive(Clone, Serialize, Deserialize)]
pub enum Dictionary {
    /// The dictionary for a consecutive range
    SimpleRange { low: i32, high: i32 },
    /// The dictionary described as a key-value map
    Dictionary {
        #[serde(skip)]
        _v2i_map: HashMap<i32, u32>,
        i2v_map: Vec<i32>,
    },
}

#[cfg(feature = "writer")]
/// The dictionary encoding result
pub enum EncodeResult {
    /// The dictionary is successfully encoded the value
    DictionaryIndex(u32),
    /// The dictionary can not encode the value, so that you need to store it elsewhere
    OutOfRange(i32),
}

impl Dictionary {
    /// Pretty print the dictionary in JSON format
    pub fn pretty_print(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }

    /// Run the random sampling algorithm on an alignment file, determining the optimal
    /// dictionary configuration
    #[cfg(feature = "depth_profiler")]
    pub fn from_sample_bam<P, F, RF>(
        path: P,
        filter: F,
        reference: Option<&str>,
        read_filter: RF,
    ) -> std::result::Result<Self, Box<dyn std::error::Error>> 
    where
        P: AsRef<Path>, 
        F: Fn(&str, usize) -> bool, 
        RF: Fn(&Alignment) -> bool + Send + Sync + Copy
    {

        let bam = BamFile::open(path.as_ref())?;
        let mut parts = vec![];
        let mut rng = rand::thread_rng();
        let mut total_size = 0;
        let mut sample_size = 0;
        for (chr, size) in bam.chroms() {
            if !filter(chr, *size) {
                continue;
            }
            total_size += size;
            let mut from = 0;
            while from < *size {
                if rng.gen::<f64>() < 0.01 {
                    let to = (from + 100_000).min(*size);
                    parts.push((chr.as_str(), from, to));
                    sample_size += to - from;
                }
                from += 100_000;
            }
        }
        let mut histogram = HashMap::new();
        let mut range_count = HashMap::new();
        let path = path.as_ref();

        let part_results: Vec<_> = parts
            .into_par_iter()
            .map(|(chr, from, to)| {
                let mut bam = BamFile::open(path).unwrap();
                if let Some(reference) = reference {
                    bam.reference_path(reference);
                }
                let range = bam.range(chr, from, to).unwrap();
                let mut previous_value = None;
                let mut histogram = HashMap::new();
                let mut range_count = HashMap::new();
                for (_, _, dep) in DepthIter::with_filter(range, read_filter) {
                    match previous_value {
                        Some(d) if d == dep => continue,
                        Some(_) | None => {
                            previous_value = Some(dep);
                            *range_count.entry(dep).or_insert(0) += 1;
                        }
                    }
                    *histogram.entry(dep).or_insert(0) += 1;
                }
                (histogram, range_count)
            })
            .collect();
        for (part_hist, part_rc) in part_results {
            for (k, v) in part_hist.into_iter() {
                *histogram.entry(k).or_insert(0) += v;
            }
            for (k, v) in part_rc.into_iter() {
                *range_count.entry(k).or_insert(0) += v;
            }
        }
        let mut histogram: Vec<_> = histogram.into_iter().collect();
        histogram.sort_by_key(|(_, count)| total_size - count);
        let total_intervals: usize = range_count.values().sum();

        let best_bit_width = (0..=16)
            .map(|b| {
                let mut out_of_range_values = total_intervals;
                for &(key, _) in histogram.iter().take(1 << b) {
                    out_of_range_values -= range_count[&key];
                }
                let p_size = total_size as f64 * b as f64 / 8.0;
                let s_size =
                    (out_of_range_values as f64) * 4.0 / sample_size as f64 * total_size as f64;
                (b, (p_size + s_size).round() as usize)
            })
            .min_by_key(|&(_, size)| size)
            .unwrap();

        let mut dict = vec![];
        if histogram.is_empty() {
            dict.push(0);
        } else {
            for i in 0..(1 << best_bit_width.0) {
                dict.push(histogram[i as usize].0 as i32);
            }
        }

        let min = dict.iter().min().unwrap();
        let max = dict.iter().max().unwrap();
        if max - min + 1 == dict.len() as i32 && (dict.len() == 1 || *min != dict[0]) {
            dict.sort_unstable();
        }
        Ok(Self::from_dict_list(dict)?)
    }

    /// Create a dictionary from the mapping vector
    pub fn from_dict_list(mapping: Vec<i32>) -> Result<Self> {
        if mapping.is_empty() {
            return Err(
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid dictionary range setting - At least one value should be present in the file"
                )
            );
        }
        if mapping.last().unwrap_or(&0) - mapping.first().unwrap_or(&0) == mapping.len() as i32 - 1
        {
            return Self::new_simple_range_dict(
                *mapping.first().unwrap(),
                *mapping.last().unwrap() + 1,
            );
        }
        let mut ret = Self::Dictionary {
            _v2i_map: Default::default(),
            i2v_map: mapping,
        };
        ret.ensure_v2i_map();
        Ok(ret)
    }

    /// Create a dictionary from a dictionary specification file.
    /// The dictionary has 2^K lines, the N-th lines contains the actual value that
    /// code N-1 encodes.
    pub fn new_dictionary_from_file<R: Read>(file: R) -> Result<Self> {
        let fp = BufReader::new(file);
        let mut mapping = vec![];
        for line in fp.lines() {
            let parsed: i32 = line?.trim().parse().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::Other, "Invalid dictionary file")
            })?;
            mapping.push(parsed);
        }
        Self::from_dict_list(mapping)
    }

    /// Create a new simple range dictionary
    pub fn new_simple_range_dict(left: i32, right: i32) -> Result<Self> {
        let n_values = (right - left).max(0) as usize;
        if n_values == 0 {
            return Err(
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Invalid dictionary range setting - At least one value should be present in the file"
                )
            );
        }
        if n_values.wrapping_sub(1) & n_values != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Invalid dictionary range setting",
            ));
        }
        Ok(Self::SimpleRange {
            low: left,
            high: right,
        })
    }

    #[inline(always)]
    pub(crate) fn ensure_v2i_map(&mut self) {
        if let Self::Dictionary { _v2i_map, i2v_map } = self {
            if _v2i_map.len() == i2v_map.len() {
                return;
            }
            _v2i_map.clear();
            for (idx, &value) in i2v_map.iter().enumerate() {
                _v2i_map.insert(value, idx as u32);
            }
        }
    }
    #[inline(always)]
    pub fn bit_width(&self) -> usize {
        let mut n_values = match self {
            Self::SimpleRange { low, high } => (high - low).max(0) as usize,
            Self::Dictionary { i2v_map, .. } => i2v_map.len(),
        };
        let mut ret = 0;
        while n_values > 1 {
            n_values >>= 1;
            ret += 1;
        }
        ret
    }

    #[inline(always)]
    pub fn first_value(&self) -> i32 {
        match self {
            Self::SimpleRange { low, .. } => *low,
            Self::Dictionary { i2v_map, .. } => i2v_map[0],
        }
    }

    #[inline(always)]
    pub(crate) fn decode_value(&self, idx: u32) -> Option<i32> {
        match self {
            Self::SimpleRange { low, .. } => Some(*low + idx as i32),
            Self::Dictionary { i2v_map, .. } => i2v_map.get(idx as usize).copied(),
        }
    }

    #[cfg(feature = "writer")]
    #[inline(always)]
    pub(crate) fn encode_value(&self, value: i32) -> EncodeResult {
        match self {
            Self::SimpleRange { low, high } => {
                if value >= *low && value < *high {
                    return EncodeResult::DictionaryIndex((value - *low) as u32);
                }
                EncodeResult::OutOfRange(value)
            }
            Self::Dictionary { _v2i_map, .. } => {
                if let Some(idx) = _v2i_map.get(&value) {
                    return EncodeResult::DictionaryIndex(*idx);
                }
                EncodeResult::OutOfRange(value)
            }
        }
    }
}
