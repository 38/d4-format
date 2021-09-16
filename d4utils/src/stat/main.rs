use clap::{load_yaml, App, ArgMatches};

use d4::ptab::UncompressedReader;
use d4::stab::{RangeRecord, SimpleKeyValueReader};
use d4::task::{Histogram, Mean, Task, TaskContext, TaskPartition};
use d4::D4FileReader;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

fn parse_bed_file<P: AsRef<Path>>(
    file: P,
) -> std::io::Result<impl Iterator<Item = (String, u32, u32)>> {
    let file = BufReader::new(File::open(file)?);
    Ok(file.lines().filter_map(|line| {
        if let Ok(line) = line {
            let tokenized: Vec<_> = line.split(|c| c == '\t').take(3).collect();
            if tokenized.len() == 3 {
                if let Ok(left) = tokenized[1].parse() {
                    if let Ok(right) = tokenized[2].parse() {
                        return Some((tokenized[0].to_owned(), left, right));
                    }
                }
            }
        }
        None
    }))
}

#[allow(clippy::type_complexity)]
fn run_task<T: Task>(
    matches: ArgMatches,
    param: <T::Partition as TaskPartition>::PartitionParam,
) -> Result<Vec<(String, u32, u32, T::Output)>, Box<dyn std::error::Error>> {
    let d4_path = matches.value_of("input").unwrap();

    let mut input =
        D4FileReader::<UncompressedReader, SimpleKeyValueReader<RangeRecord>>::open(d4_path)?;

    let region_spec: Vec<_> = if let Some(path) = matches.value_of("region") {
        parse_bed_file(path)?
            .map(|(chr, left, right)| (chr, left, right))
            .collect()
    } else {
        input
            .header()
            .chrom_list()
            .iter()
            .map(|chrom| (chrom.name.clone(), 0u32, chrom.size as u32))
            .collect()
    };

    let tc = TaskContext::<_, _, T>::new(&mut input, &region_spec, param)?;

    Ok(tc.run())
}

fn percentile_stat(matches: ArgMatches, percentile: f64) -> Result<(), Box<dyn std::error::Error>> {
    let histograms = run_task::<Histogram>(matches, 0..1000)?;
    for (chr, begin, end, (below, hist, above)) in histograms {
        let count: u32 = below + hist.iter().sum::<u32>() + above;
        let below_count = (count as f64 * percentile.min(1.0).max(0.0)).round() as u32;
        let mut current = below;
        let mut idx = 0;
        while current < below_count && (idx as usize) < hist.len() {
            current += hist[idx];
            idx += 1;
        }
        println!("{}\t{}\t{}\t{}", chr, begin, end, idx);
    }
    Ok(())
}

fn hist_stat(matches: ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    let max_bin = matches.value_of("max-bin").unwrap_or("1000").parse()?;
    let histograms = run_task::<Histogram>(matches, 0..max_bin)?;
    let mut hist_result = vec![0; max_bin as usize + 1];
    let (mut below, mut above) = (0, 0);
    for (_, _, _, (b, hist, a)) in histograms {
        below += b;
        above += a;
        for (id, val) in hist.iter().enumerate() {
            hist_result[id + 1] += val;
        }
    }

    println!("<0\t{}", below);
    for (val, cnt) in hist_result[1..].iter().enumerate() {
        println!("{}\t{}", val, cnt);
    }
    println!(">{}\t{}", max_bin, above);

    Ok(())
}

pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches_from(&args);
    if let Some(threads) = matches.value_of("threads") {
        let threads = threads.parse().unwrap();
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()?;
    }
    match matches.value_of("stat") {
        None | Some("mean") | Some("avg") => {
            for result in run_task::<Mean>(matches, ())? {
                println!("{}\t{}\t{}\t{}", result.0, result.1, result.2, result.3);
            }
        }
        Some("median") => {
            percentile_stat(matches, 0.5)?;
        }
        Some("hist") => {
            hist_stat(matches)?;
        }
        Some(whatever) if whatever.starts_with("percentile=") => {
            let prefix_len = "percentile=".len();
            let percentile: f64 = whatever[prefix_len..].parse()?;
            percentile_stat(matches, percentile / 100.0)?;
        }
        _ => panic!("Unsupported stat type"),
    }
    Ok(())
}
