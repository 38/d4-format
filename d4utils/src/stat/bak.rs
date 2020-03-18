use d4::ptab::{DecodeResult, PTablePartitionReader, UncompressedReader};
use d4::stab::{STablePartitionReader, RangeRecord, SimpleKeyValueReader};
use rayon::prelude::*;

mod create;
mod framedump;
mod show;

fn main() {
    let mut reader =
        d4::D4FileReader::<UncompressedReader, SimpleKeyValueReader<RangeRecord>>::open("/tmp/test.d4").unwrap();
    let parts = reader.split(None).unwrap();

    let result: Vec<_> = parts
        .into_par_iter()
        .map(|(mut p, mut s)| {
            let mut d = p.as_decoder();
            let (_name, start, end) = p.region();
            let mut histogram = vec![0;10240];
            if p.bit_width() > 0 {
                for pos in start..end {
                    let value = match d.decode(pos as usize) {
                        DecodeResult::Definitely(value) => value,
                        DecodeResult::Maybe(back_value) => {
                            if let Some(value) = s.decode(pos) {
                                value
                            } else {
                                back_value
                            }
                        }
                    } as i64;
                    if histogram.len() > value as usize { 
                        histogram[value as usize] += 1;
                    }
                }
            } else {
                let iter = s.into_iter();
                histogram[0] = end - start;
                for (l,r,d) in iter {
                    if (d as usize) < histogram.len() {
                        histogram[d as usize] += r - l;
                        histogram[0] -= r - l;
                    }
                }
            }
            histogram
        })
        .collect();
    let mut histogram = vec![0;10240];
    for part_result in result {
        for (k,v) in part_result.iter().enumerate() {
            histogram[k] += v;
        }
    } 
    for (k,v) in histogram.iter().enumerate() {
        if *v == 0 {continue;}
        println!("{} {}", k, v)
    }
}