use d4::ptab::{DecodeResult, PTablePartitionReader, UncompressedReader};
use d4::stab::{RangeRecord, STablePartitionReader, SimpleKeyValueReader};
use d4::D4FileReader;
use rayon::prelude::*;
use std::env::args;

fn main() {
    if let Some(input) = args().skip(1).next() {
        let mut input =
            D4FileReader::<UncompressedReader, SimpleKeyValueReader<RangeRecord>>::open(input)
                .unwrap();
        let parts = input.split(Some(1000_0000)).unwrap();

        if let Some(threads) = std::env::var("NTHREADS").ok().and_then(|v| v.parse().ok()) {
            rayon::ThreadPoolBuilder::new()
                .num_threads(threads)
                .build_global()
                .unwrap();
        }

        let mut result: Vec<_> = parts
            .into_par_iter()
            .map(|(mut pt, mut st)| {
                let (chr, left, right) = pt.region();
                let chr = chr.to_string();
                let dec = pt.as_decoder();
                let mut result = vec![];
                let mut current_left = None;
                let mut current_sum = 0;
                for pos in left..=right {
                    let value = if pos < right {
                        match dec.decode(pos as usize) {
                            DecodeResult::Definitely(value) => value,
                            DecodeResult::Maybe(default) => st.decode(pos).unwrap_or(default),
                        }
                    } else {
                        0
                    };

                    if value > 10000 {
                        if current_left.is_none() {
                            current_left = Some(pos);
                            current_sum = 0;
                        }
                        current_sum += value as usize;
                    } else {
                        if let Some(left) = current_left {
                            if pos - left > 1000 {
                                result.push((
                                    left,
                                    pos,
                                    (current_sum as f64) / (pos - left) as f64,
                                ));
                            }
                            current_left = None;
                            current_sum = 0;
                        }
                    }
                }
                (chr, result)
            })
            .collect();
        result.sort_by_key(|(a, _)| a.to_string());
        for (chr, range) in result
            .iter()
            .map(|(chr, regions)| regions.iter().map(move |r| (chr, *r)))
            .flatten()
        {
            println!("{}\t{}\t{}\t{}", chr, range.0, range.1, range.2);
        }
    } else {
        println!("Missing input file");
    }
}
