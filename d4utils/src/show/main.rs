use clap::{load_yaml, App};
use d4::{
    ptab::{DecodeResult, PrimaryTablePartReader},
    stab::SecondaryTablePartReader,
    D4TrackReader,
};
use regex::Regex;
use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
    io::{Error, Result as IOResult, Write},
    path::Path,
};
fn write_bed_record_fast<W: Write>(
    mut writer: W,
    chr: &str,
    left: u32,
    right: u32,
    values: &[i32],
) -> IOResult<()> {
    writer.write_all(chr.as_bytes())?;
    writer.write_all(b"\t")?;
    writer.write_all(left.to_string().as_bytes())?;
    writer.write_all(b"\t")?;
    writer.write_all(right.to_string().as_bytes())?;
    for value in values {
        writer.write_all(b"\t")?;
        writer.write_all(value.to_string().as_bytes())?;
    }
    writer.write_all(b"\n")?;
    Ok(())
}

fn parse_region_spec<'a, T: Iterator<Item = &'a str>>(
    regions: Option<T>,
    inputs: &[D4TrackReader],
) -> Result<HashMap<String, Vec<(u32, u32)>>, Error> {
    if inputs.is_empty() {
        return Ok(Default::default());
    }
    // First, we should check if the inputs are consistent
    let chrom_list = inputs[0].header().chrom_list();
    for input in inputs.iter().skip(1) {
        if chrom_list != input.header().chrom_list() {
            return Err(Error::new(
                std::io::ErrorKind::Other,
                "Inconsistent reference genome",
            ));
        }
    }

    let region_pattern = Regex::new(r"^(?P<CHR>[^:]+)((:(?P<FROM>\d+)-)?(?P<TO>\d+)?)?$").unwrap();
    let mut ret = HashMap::<_, Vec<(_, u32)>>::new();

    if let Some(regions) = regions {
        for region_spec in regions {
            if let Some(captures) = region_pattern.captures(region_spec) {
                let chr = captures.name("CHR").unwrap().as_str();
                let start: u32 = captures
                    .name("FROM")
                    .map_or(0u32, |x| x.as_str().parse().unwrap_or(0));
                let end: u32 = captures
                    .name("TO")
                    .map_or(!0u32, |x| x.as_str().parse().unwrap_or(!0));
                ret.entry(chr.to_string()).or_default().push((start, end));
                continue;
            } else {
                return Err(Error::new(std::io::ErrorKind::Other, "Invalid region spec"));
            }
        }
    } else {
        for chrom in chrom_list {
            ret.insert(chrom.name.to_owned(), vec![(0, chrom.size as u32)]);
        }
    }

    for regions in ret.values_mut() {
        regions.sort_unstable();
    }

    Ok(ret)
}

fn flush_value<W: Write>(
    target: W,
    chr: &str,
    left: u32,
    right: u32,
    values: &[i32],
    print_zeros: bool,
) -> IOResult<()> {
    if (print_zeros || values.iter().any(|&x| x != 0)) && left < right {
        write_bed_record_fast(target, chr, left, right, values)?;
    }
    Ok(())
}

fn show_region(
    inputs: &mut [D4TrackReader],
    regions: &HashMap<String, Vec<(u32, u32)>>,
    print_all_zero: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if inputs.is_empty() {
        return Ok(());
    }

    let mut stdout = std::io::stdout();

    let mut partitions: Vec<_> = inputs
        .iter_mut()
        .map(|input| {
            input
                .split(None)
                .expect("Unable to split data track")
                .into_iter()
        })
        .collect();

    let mut values = vec![0; inputs.len()];
    let mut prev_values = vec![0; inputs.len()];

    while let Some(column_one) = partitions[0].next() {
        let mut row = vec![(column_one, None)];
        for column in &mut partitions[1..] {
            let column = column.next().unwrap();
            row.push((column, None));
        }

        let (chr, _, chr_end) = row[0].0 .0.region();
        let chr = chr.to_string();

        for &(from, mut to) in regions.get(&chr).map(|r| &r[..]).unwrap_or(&[][..]) {
            to = to.min(chr_end);
            let mut last_pos = from;
            for pos in from..to {
                let mut value_changed = false;
                for (idx, ((primary, secondary), primary_decoder)) in row.iter_mut().enumerate() {
                    if primary_decoder.is_none() {
                        *primary_decoder = Some(primary.make_decoder());
                    }
                    let value = match primary_decoder.as_mut().unwrap().decode(pos as usize) {
                        DecodeResult::Definitely(value) => value,
                        DecodeResult::Maybe(value) => secondary.decode(pos).unwrap_or(value),
                    };
                    if values[idx] != value {
                        if !value_changed {
                            prev_values.clone_from(&values);
                            value_changed = true;
                        }
                        values[idx] = value;
                    }
                }
                if value_changed {
                    flush_value(
                        &mut stdout,
                        &chr,
                        last_pos,
                        pos,
                        prev_values.as_slice(),
                        print_all_zero,
                    )?;
                    last_pos = pos;
                }
            }
            if last_pos != to {
                flush_value(
                    &mut stdout,
                    &chr,
                    last_pos,
                    to,
                    values.as_slice(),
                    print_all_zero,
                )?;
            }
        }
    }

    stdout.flush()?;

    Ok(())
}

pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .version(d4::VERSION)
        .get_matches_from(args);
    let mut data_path = vec![];
    let input_filename = matches.value_of("input-file").unwrap();
    let mut d4files: Vec<D4TrackReader> =
        if matches.is_present("first") || input_filename.contains(':') {
            data_path.push("<default>".to_string());
            vec![D4TrackReader::open(input_filename)?]
        } else if let Some(pattern) = matches.value_of("filter") {
            let pattern = regex::Regex::new(pattern)?;
            D4TrackReader::open_tracks(input_filename, |path| {
                let stem = path
                    .map(|what: &Path| {
                        what.file_name()
                            .map(|x| x.to_string_lossy())
                            .unwrap_or_else(|| Cow::<str>::Borrowed(""))
                    })
                    .unwrap_or_default();
                if pattern.is_match(stem.borrow()) {
                    data_path.push(stem.to_string());
                    true
                } else {
                    false
                }
            })?
        } else {
            D4TrackReader::open_tracks(input_filename, |path| {
                let stem = path
                    .map(|what: &Path| {
                        what.file_name()
                            .map(|x| x.to_string_lossy())
                            .unwrap_or_else(|| Cow::<str>::Borrowed(""))
                    })
                    .unwrap_or_default();
                data_path.push(stem.to_string());
                true
            })?
        };

    if matches.values_of("show-genome").is_some() {
        let hdr = d4files[0].header();
        for chrom in hdr.chrom_list() {
            println!("{}\t{}", chrom.name, chrom.size);
        }
        return Ok(());
    }

    let should_print_zero = !matches.is_present("no-missing-data");

    let regions = parse_region_spec(matches.values_of("regions"), &d4files)?;

    print!("#chr\tbegin\tend");
    for tag in data_path {
        print!("\t{}", tag);
    }
    println!();

    show_region(&mut d4files, &regions, should_print_zero)?;

    Ok(())
}
