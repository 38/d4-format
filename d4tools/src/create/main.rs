use clap::{load_yaml, App, ArgMatches};
use d4::ptab::PTablePartitionWriter;
use d4::stab::SecondaryTablePartWriter;
use d4::{Chrom, D4FileWriter, Dictionary};
use d4_hts::{BamFile, DepthIter};
use d4tools::{make_dictionary, parse_bed_file, parse_genome_file, setup_thread_pool, InputType};
use log::info;
use rayon::prelude::*;
use regex::Regex;
use std::path::Path;

fn main_impl(matches: ArgMatches<'_>) -> Result<(), Box<dyn std::error::Error>> {
    setup_thread_pool(&matches)?;

    let input_path: &Path = matches.value_of("input-file").unwrap().as_ref();
    let input_type = InputType::detect(input_path);

    let min_mq = matches.value_of("min-mqual").map_or(60, |v| {
        v.parse().expect("Invalid minimal mapping quality option")
    });

    let bam_flags = matches
        .value_of("bam-flag")
        .map(|v| v.parse::<u16>().expect("Invalid BAM flag"));

    let output_path = matches.value_of("output-file").map_or_else(
        || {
            let mut ret = input_path.to_owned();
            ret.set_extension("d4");
            ret
        },
        |path| path.into(),
    );
    let mut d4_builder = d4::D4FileBuilder::new(output_path);

    d4_builder.set_dictionary(make_dictionary(
        matches.value_of("dict-range"),
        matches.value_of("dict-file"),
    )?);

    if matches.is_present("sparse") {
        d4_builder.set_dictionary(d4::Dictionary::new_simple_range_dict(0, 1)?);
    }

    let chr_filter = Regex::new(matches.value_of("filter").unwrap_or(".*"))?;

    if matches.values_of("dict-auto").is_some() {
        d4_builder.set_dictionary(Dictionary::from_sample_bam(
            input_path,
            |chr, _size| chr_filter.is_match(chr),
            matches.value_of("ref"),
            min_mq,
        )?);
    }

    d4_builder.set_filter(move |chr, _size| chr_filter.is_match(chr));

    if matches.values_of("dump-dict").is_some() {
        println!("{}", d4_builder.dictionary().pretty_print()?);
        std::process::exit(0);
    }

    let reference = matches.value_of("ref");

    let enable_compression = matches.is_present("deflate") || matches.is_present("sparse");
    let compression_level: u32 = matches.value_of("deflate-level").unwrap_or("5").parse()?;

    match input_type {
        InputType::Alignment => {
            d4_builder.load_chrom_info_from_bam(input_path)?;
            let mut d4_writer: D4FileWriter = d4_builder.create()?;

            if enable_compression {
                d4_writer.enable_secondary_table_compression(compression_level);
            }

            let partitions = d4_writer.parallel_parts(Some(10_000_000))?;

            info!("Total number of parallel tasks: {}", partitions.len());

            partitions
                .into_par_iter()
                .for_each(|(mut p_table, mut s_table)| {
                    let (chr, from, to) = p_table.region();
                    let chr = chr.to_owned();
                    let mut alignment = BamFile::open(input_path).unwrap();
                    if let Some(reference) = reference {
                        alignment.reference_path(reference);
                    }
                    let al_from = from - from.min(5000);
                    let time_begin = std::time::SystemTime::now();
                    info!("Task begin: {}:{}-{}", chr, from, to);
                    let range_iter = alignment
                        .range(&chr, al_from as usize, to as usize)
                        .unwrap();
                    let mut p_encoder = p_table.make_encoder();
                    let mut last_pos = 0;
                    for (_, pos, depth) in DepthIter::with_filter(range_iter, |r| {
                        r.map_qual() >= min_mq
                            && (bam_flags.is_none() || bam_flags.unwrap() == r.flag())
                    }) {
                        last_pos = pos;
                        if pos < from as usize {
                            continue;
                        }
                        if pos as u32 >= to {
                            break;
                        }
                        if !p_encoder.encode(pos, depth as i32) {
                            s_table.encode(pos as u32, depth as i32).unwrap();
                        }
                    }
                    for pos in last_pos..to as usize {
                        if !p_encoder.encode(pos, 0) {
                            s_table.encode(pos as u32, 0).unwrap();
                        }
                    }
                    s_table.flush().unwrap();
                    s_table.finish().unwrap();
                    let time_end = std::time::SystemTime::now();
                    let duration = time_end.duration_since(time_begin).unwrap_or_default();
                    info!(
                        "Task completed: {}:{}-{} Duration: {}ms",
                        chr,
                        from,
                        to,
                        duration.as_millis()
                    );
                });
        }
        InputType::BiwWig => {
            let bw_file = d4_bigwig::BigWigFile::open(input_path)?;
            d4_builder.append_chrom(
                bw_file
                    .chroms()
                    .into_iter()
                    .map(|(name, size)| Chrom { name, size }),
            );
            let mut d4_writer: D4FileWriter = d4_builder.create()?;
            if enable_compression {
                d4_writer.enable_secondary_table_compression(compression_level);
            }
            let partition = d4_writer.parallel_parts(None)?;
            for (mut pt, mut st) in partition {
                let (chrom, left, right) = pt.region();
                let chrom = chrom.to_string();
                let mut last = left;
                let mut p_encoder = pt.make_encoder();

                let mut write_value = |pos: u32, value: i32| {
                    if !p_encoder.encode(pos as usize, value) {
                        st.encode(pos as u32, value).unwrap();
                    }
                };
                if let Some(iter) = bw_file.query_range(&chrom, left, right) {
                    for d4_bigwig::BigWigInterval {
                        begin: left,
                        end: right,
                        value,
                    } in iter
                    {
                        for pos in last..left {
                            write_value(pos, 0);
                        }
                        for pos in left..right {
                            write_value(pos, value as i32);
                        }
                        last = right;
                    }
                }
                for pos in last..right {
                    write_value(pos, 0);
                }
            }
        }
        InputType::BedGraph => {
            d4_builder.append_chrom(
                parse_genome_file(
                    matches
                        .value_of("genome")
                        .expect("Genome file is required for text file format"),
                )?
                .into_iter(),
            );
            let mut d4_writer: D4FileWriter = d4_builder.create()?;
            if enable_compression {
                d4_writer.enable_secondary_table_compression(compression_level);
            }
            let mut partition = d4_writer.parallel_parts(None)?;
            let input = parse_bed_file(input_path)?;
            let mut current = 0;
            for (chr, from, to, depth) in input {
                for pos in from..to {
                    let region = partition[current].0.region();
                    if region.0 != chr || region.1 < pos || region.2 >= pos {
                        if let Some((idx, _)) = (0..).zip(partition.iter()).find(|(_, part)| {
                            let reg = part.0.region();
                            reg.0 == chr && reg.1 <= pos && pos < reg.2
                        }) {
                            current = idx;
                        } else {
                            continue;
                        }
                    }
                    let mut encoder = partition[current].0.make_encoder();
                    if !encoder.encode(pos as usize, depth) {
                        partition[current].1.encode(pos, depth)?;
                    }
                }
                partition[current].1.flush()?;
            }
            for (_, mut stab) in partition {
                stab.finish()?;
            }
        }
        _ => {
            panic!("Unsupported input file format");
        }
    }
    Ok(())
}

pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .version(d4::VERSION)
        .get_matches_from(args);

    main_impl(matches)
}
