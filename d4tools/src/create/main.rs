use clap::{load_yaml, App, ArgMatches};
use d4::ptab::PTablePartitionWriter;
use d4::stab::SecondaryTablePartWriter;
use d4::{Chrom, D4FileBuilder, D4FileWriter, Dictionary};
use d4_hts::{BamFile, DepthIter, Alignment};
use d4tools::{make_dictionary, parse_bed_file, parse_genome_file, setup_thread_pool, InputType};
use log::{info, warn};
use rayon::prelude::*;
use regex::Regex;
use std::path::{Path, PathBuf};

type DynErr = Box<dyn std::error::Error>;

struct CreateAppCtx {
    input_path: PathBuf,
    input_type: InputType,
    min_mq: u8,
    bam_flags: Option<u16>,
    inclusive_flag: u16,
    exclusive_flag: u16,
    chr_filter: Regex,
    compression: bool,
    denominator: Option<f64>,
    compression_level: u32,
    builder: D4FileBuilder,
}

#[derive(Clone, Copy)]
struct BamFilter {
    min_mq: u8,
    bam_flags: Option<u16>,
    inclusive_flag: u16,
    exclusive_flag: u16,
}

impl BamFilter {
    fn filter_alignment(&self, read: &Alignment) -> bool {
        let quality = read.map_qual() >= self.min_mq;
        let flag = read.flag();
        let exact_match = self.bam_flags.map_or(true, |expected| expected == flag);
        let inclusive_match = (self.inclusive_flag & flag) == self.inclusive_flag;
        let exclusive_match = (self.exclusive_flag & flag) == 0;
        quality && exact_match && inclusive_match && exclusive_match
    }
}

impl CreateAppCtx {
    fn get_bam_filter(&self) -> BamFilter {
        BamFilter { min_mq: self.min_mq, bam_flags: self.bam_flags, inclusive_flag: self.inclusive_flag, exclusive_flag: self.exclusive_flag }
    }
    fn new(matches: &ArgMatches) -> Result<Self, DynErr> {
        let input_path: &Path = matches.value_of("input-file").unwrap().as_ref();
        let input_type = InputType::detect(input_path);

        let min_mq = matches.value_of("min-mqual").map_or(60, |v| {
            v.parse().expect("Invalid minimal mapping quality option")
        });

        let mut bam_flags = None;
        let mut inclusive_flag = 0;
        let mut exclusive_flag = 0;

        if let Some(bam_flag_expr) = matches.value_of("bam-flag") {
            for fragment in bam_flag_expr.split(',') {
                let (opcode, val_str) = match fragment.chars().next() {
                    Some('+') => ('+', &fragment[1..]),
                    Some('-') => ('-', &fragment[1..]),
                    Some('~') => ('~', &fragment[1..]),
                    _ => ('=', fragment),
                };
                let value:u16 = val_str.parse().expect("Invalid BAM flag");
                match opcode {
                    '+' => inclusive_flag |= value,
                    '-' | '~' => exclusive_flag |= value,
                    '=' => bam_flags = Some(value),
                    _ => unreachable!(),
                }
            }
        }
        
        let output_path = matches.value_of("output-file").map_or_else(
            || {
                let mut ret = input_path.to_owned();
                ret.set_extension("d4");
                ret
            },
            |path| path.into(),
        );
        let denominator: Option<f64> = matches
            .value_of("denominator")
            .map(|what| what.parse().unwrap());
        let mut builder = d4::D4FileBuilder::new(output_path);

        let chr_filter = Regex::new(matches.value_of("filter").unwrap_or(".*"))?;

        builder.set_filter(move |chr, _size| chr_filter.is_match(chr));

        let compression = matches.is_present("deflate") || matches.is_present("sparse");
        let compression_level: u32 = matches.value_of("deflate-level").unwrap_or("5").parse()?;

        Ok(Self {
            input_path: input_path.to_owned(),
            input_type,
            min_mq,
            bam_flags,
            inclusive_flag,
            exclusive_flag,
            chr_filter: Regex::new(matches.value_of("filter").unwrap_or(".*"))?,
            compression,
            compression_level,
            denominator,
            builder,
        })
    }
    fn auto_dict_for_bam(&mut self, matches: &ArgMatches) -> Result<(), DynErr> {
        let filter = self.get_bam_filter();
        let dict = Dictionary::from_sample_bam(
            self.input_path.as_path(),
            |chr, _size| self.chr_filter.is_match(chr),
            matches.value_of("ref"),
            move |r| filter.filter_alignment(r),
        )?;
        self.builder.set_dictionary(dict);
        Ok(())
    }
    fn auto_dict_for_bw(&mut self) -> Result<(), DynErr> {
        let fp = std::fs::metadata(self.input_path.as_path())?;
        let bw_file = d4_bigwig::BigWigFile::open(self.input_path.as_path())?;

        let genome_size: u64 = bw_file.chroms().into_iter().map(|(_, sz)| sz as u64).sum();

        let file_size = fp.len();

        if file_size < genome_size / 8 {
            self.builder
                .set_dictionary(Dictionary::new_simple_range_dict(0, 1)?);
            self.compression = true;
        }

        Ok(())
    }
    fn auto_dict_for_bedgraph(&mut self, matches: &ArgMatches) -> Result<(), DynErr> {
        let genomes = parse_genome_file(
            matches
                .value_of("genome")
                .expect("Genome file is required for text file format"),
        )?;
        let genome_size: u64 = genomes.into_iter().map(|chr| chr.size as u64).sum();

        let fp = std::fs::metadata(self.input_path.as_path())?;
        let file_size = fp.len();

        if file_size < genome_size {
            self.builder
                .set_dictionary(Dictionary::new_simple_range_dict(0, 1)?);
            self.compression = true;
        }

        Ok(())
    }
    fn configure_dict(&mut self, matches: &ArgMatches) -> Result<(), DynErr> {
        self.builder.set_dictionary(make_dictionary(
            matches.value_of("dict-range"),
            matches.value_of("dict-file"),
        )?);

        if matches.is_present("sparse") {
            self.builder
                .set_dictionary(d4::Dictionary::new_simple_range_dict(0, 1)?);
        }

        let auto_dict_detection = (!matches.is_present("dict_range")
            && !matches.is_present("dict-file"))
            || matches.is_present("dict-auto");

        if auto_dict_detection {
            match self.input_type {
                InputType::Alignment => self.auto_dict_for_bam(matches)?,
                InputType::BiwWig => self.auto_dict_for_bw()?,
                InputType::BedGraph => self.auto_dict_for_bedgraph(matches)?,
                _ => {
                    panic!("Unsupported input type")
                }
            }
        }

        Ok(())
    }

    fn detect_default_denominator_for_bigwig(
        &mut self,
        matches: &ArgMatches,
    ) -> Result<(), DynErr> {
        let auto_dict_detection = (!matches.is_present("dict_range")
            && !matches.is_present("dict-file"))
            || matches.is_present("dict-auto");

        let bw_file = d4_bigwig::BigWigFile::open(self.input_path.as_path())?;
        let mut purposed_denominator = 1.0f64;
        let mut num_of_intervals = 0;
        let mut genome_size = 0;
        for (chr_name, chr_size) in bw_file.chroms() {
            genome_size += chr_size;
            if let Some(result) = bw_file.query_range(&chr_name, 0, chr_size as u32) {
                for bw_interval in result {
                    let value = bw_interval.value as f64;
                    if value.abs() < 1e-10 {
                        continue;
                    }
                    num_of_intervals += 1;

                    let mut denominator = 1.0;

                    while ((value * denominator).round() - (value * denominator)).abs() > 1e-10 {
                        denominator *= 10.0;
                    }

                    purposed_denominator = purposed_denominator.max(denominator);
                }
            }
        }
        if auto_dict_detection && num_of_intervals * 10 < genome_size * 6 {
            self.builder
                .set_dictionary(Dictionary::new_simple_range_dict(0, 1)?);
            self.compression = true;
        }
        if purposed_denominator != 1.0 {
            self.builder.set_denominator(purposed_denominator);
            self.denominator = Some(purposed_denominator);
        }
        Ok(())
    }

    fn detect_default_denominator_for_bedgraph(&mut self) -> Result<(), DynErr> {
        let input = parse_bed_file(self.input_path.as_path())?;
        let mut purposed_denominator = 1.0f64;

        for (_, _, _, value) in input {
            if value.abs() < 1e-10 {
                continue;
            }
            let mut denominator = 1.0;

            while ((value * denominator).round() - (value * denominator)).abs() > 1e-10 {
                denominator *= 10.0;
            }

            purposed_denominator = purposed_denominator.max(denominator);
        }

        if purposed_denominator != 1.0 {
            self.builder.set_denominator(purposed_denominator);
            self.denominator = Some(purposed_denominator);
        }
        Ok(())
    }

    fn determine_default_denominator(&mut self, matches: &ArgMatches) -> Result<(), DynErr> {
        if self.denominator.is_some() {
            return Ok(());
        }

        match self.input_type {
            InputType::BiwWig => self.detect_default_denominator_for_bigwig(matches)?,
            InputType::BedGraph => self.detect_default_denominator_for_bedgraph()?,
            _ => (),
        }

        Ok(())
    }
    
    fn create_from_alignment(mut self, matches: &ArgMatches) -> Result<(), DynErr> {
        let reference = matches.value_of("ref");

        self.builder.load_chrom_info_from_bam(&self.input_path)?;
        let mut d4_writer: D4FileWriter = self.builder.create()?;

        if self.compression {
            d4_writer.enable_secondary_table_compression(self.compression_level);
        }

        let partitions = d4_writer.parallel_parts(Some(10_000_000))?;

        info!("Total number of parallel tasks: {}", partitions.len());
        
        let bam_filter = self.get_bam_filter();

        partitions
            .into_par_iter()
            .for_each(|(mut p_table, mut s_table)| {
                let (chr, from, to) = p_table.region();
                let chr = chr.to_owned();
                let mut alignment = BamFile::open(&self.input_path).unwrap();
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
                for (_, pos, depth) in DepthIter::with_filter(range_iter, |r| bam_filter.filter_alignment(r)) {
                    let depth = if let Some(denominator) = self.denominator {
                        (depth as f64 * denominator).round() as u32
                    } else {
                        depth
                    };

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
                for pos in last_pos.max(from as usize)..to as usize {
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
        Ok(())
    }

    fn create_from_bigwig(mut self) -> Result<(), DynErr> {
        let bw_file = d4_bigwig::BigWigFile::open(&self.input_path)?;
        self.builder.append_chrom(
            bw_file
                .chroms()
                .into_iter()
                .map(|(name, size)| Chrom { name, size }),
        );
        let mut d4_writer: D4FileWriter = self.builder.create()?;
        if self.compression {
            d4_writer.enable_secondary_table_compression(self.compression_level);
        }
        let partition = d4_writer.parallel_parts(None)?;
        for (mut pt, mut st) in partition {
            let (chrom, left, right) = pt.region();
            let chrom = chrom.to_string();
            let mut last = left;
            let mut p_encoder = pt.make_encoder();

            let mut write_value = |pos: u32, value: i32| {
                if !p_encoder.encode(pos as usize, value) {
                    st.encode(pos, value).unwrap();
                }
            };
            if let Some(iter) = bw_file.query_range(&chrom, left, right) {
                for d4_bigwig::BigWigInterval {
                    begin: left,
                    end: right,
                    value,
                } in iter
                {
                    let value = if let Some(denominator) = self.denominator {
                        ((value as f64) * denominator).round() as f32
                    } else {
                        value
                    };

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
        Ok(())
    }
    fn create_from_bedgraph(mut self, matches: &ArgMatches) -> Result<(), DynErr> {
        self.builder.append_chrom(
            parse_genome_file(
                matches
                    .value_of("genome")
                    .expect("Genome file is required for text file format"),
            )?
            .into_iter(),
        );
        let default_pt_value = if self.builder.dictionary().bit_width() == 0 {
            Some(self.builder.dictionary().first_value())
        } else {
            None
        };
        let mut d4_writer: D4FileWriter = self.builder.create()?;
        if self.compression {
            d4_writer.enable_secondary_table_compression(self.compression_level);
        }
        let mut partition = d4_writer.parallel_parts(None)?;
        let input = parse_bed_file(&self.input_path)?;
        let mut current = 0;
        for (chr, from, to, depth) in input {
            let depth = if let Some(denominator) = self.denominator {
                (depth * denominator).round() as i32
            } else {
                if depth - depth.floor() > 1e-10 {
                    warn!("Encoding a decimal valued input to a integer D4, fix-point mode is recommended");
                }
                depth as i32
            };

            if let Some(default) = default_pt_value {
                if default == depth {
                    // In this case, we have a zero sized primary table and the vlaue we need to encode is just that value
                    continue;
                } else {
                    let mut from = from;
                    while from < to {
                        let mut region = partition[current].0.region();
                        if region.0 != chr || region.1 < from || region.2 >= from {
                            if let Some((idx, _)) = (0..).zip(partition.iter()).find(|(_, part)| {
                                let reg = part.0.region();
                                reg.0 == chr && reg.1 <= from && from < reg.2
                            }) {
                                current = idx;
                                region = partition[current].0.region();
                            } else {
                                continue;
                            }
                        }
                        let record_from = from;
                        let record_to = region.2.min(to);
                        partition[current]
                            .1
                            .encode_record(record_from, record_to, depth)?;
                        from = record_to;
                    }
                }
            } else {
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
            }
            partition[current].1.flush()?;
        }
        for (_, mut stab) in partition {
            stab.finish()?;
        }
        Ok(())
    }
}

fn main_impl(matches: ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    setup_thread_pool(&matches)?;

    let mut ctx = CreateAppCtx::new(&matches)?;

    ctx.configure_dict(&matches)?;

    if matches.values_of("dump-dict").is_some() {
        println!("{}", ctx.builder.dictionary().pretty_print()?);
        std::process::exit(0);
    }

    ctx.determine_default_denominator(&matches)?;

    match ctx.input_type {
        InputType::Alignment => ctx.create_from_alignment(&matches)?,
        InputType::BiwWig => ctx.create_from_bigwig()?,
        InputType::BedGraph => ctx.create_from_bedgraph(&matches)?,
        _ => panic!("Unsupported input file format"),
    }
    Ok(())
}

pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .version(d4tools::VERSION)
        .get_matches_from(args);

    main_impl(matches)
}
