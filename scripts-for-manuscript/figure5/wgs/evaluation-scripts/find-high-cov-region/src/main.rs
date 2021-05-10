use std::env::args;
use d4::ptab::{UncompressedReader, DecodeResult, PTablePartitionReader};
use d4::stab::{RangeRecord, SimpleKeyValueReader, STablePartitionReader};
use d4::D4FileReader;
use rayon::prelude::*;

fn main() {
    if let Some(input) = args().skip(1).next() {
        let mut input = D4FileReader::<UncompressedReader, SimpleKeyValueReader<RangeRecord>>::open(input).unwrap();
        let parts = input.split(Some(1000_0000)).unwrap();

        if let Some(threads) = std::env::var("NTHREADS").ok().and_then(|v| v.parse().ok()) {
            rayon::ThreadPoolBuilder::new().num_threads(threads).build_global().unwrap();
        }

        let mut result:Vec<_> = parts.into_par_iter().map(|(mut pt,mut st)| {
            let (chr, left, right) = pt.region();
            let chr = chr.to_string();
            let dec = pt.as_decoder();
            let mut result = vec![];
            let mut current_left = None;
            for pos in left..=right {
                let value = if pos < right {
                    match dec.decode(pos as usize) {
                        DecodeResult::Definitely(value) => value,
                        DecodeResult::Maybe(default) => st.decode(pos).unwrap_or(default),
                    }
                } else {
                    0
                };

                if value > 120 {
                    if current_left.is_none() {
                        current_left = Some(pos);
                    }
                } else {
                    if let Some(left) = current_left {
                        if pos - left > 1000 {
                            result.push((left, pos));
                        }
                        current_left = None;
                    }
                }
            }
            (chr, result)
        }).collect();
        result.sort();
        for (chr, range) in result.iter().map(|(chr, regions)| regions.iter().map(move |r| (chr, *r))).flatten() {
            println!("{}\t{}\t{}", chr, range.0, range.1);
        }
    } else {
        println!("Missing input file");
    }
}
