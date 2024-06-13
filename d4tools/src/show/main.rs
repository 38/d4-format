use clap::{load_yaml, App};
use d4::{
    find_tracks,
    ssio::{http::HttpReader, D4TrackReader},
    Chrom,
};
use d4_framefile::{Directory, OpenResult};
use d4tools::AppResult;
use regex::Regex;
use std::{
    borrow::{Borrow, Cow},
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader, Error, ErrorKind, Read, Result as IOResult, Seek, Write},
    path::Path,
};
fn write_bed_record_fast<W: Write>(
    mut writer: W,
    chr: &str,
    left: u32,
    right: u32,
    values: &[i32],
    denominators: &[Option<f64>],
) -> IOResult<()> {
    writer.write_all(chr.as_bytes())?;
    writer.write_all(b"\t")?;
    writer.write_all(left.to_string().as_bytes())?;
    writer.write_all(b"\t")?;
    writer.write_all(right.to_string().as_bytes())?;
    for (value, denominator) in values.iter().zip(denominators.iter()) {
        writer.write_all(b"\t")?;
        if let Some(denominator) = denominator {
            writer.write_all((*value as f64 / denominator).to_string().as_bytes())?;
        } else {
            writer.write_all(value.to_string().as_bytes())?;
        }
    }
    writer.write_all(b"\n")?;
    Ok(())
}

fn parse_region_spec<T: Iterator<Item = String>>(
    regions: Option<T>,
    chrom_list: &[Chrom],
) -> std::io::Result<Vec<(usize, u32, u32)>> {
    let region_pattern = Regex::new(r"^(?P<CHR>[^:]+)((:(?P<FROM>\d+)-)?(?P<TO>\d+)?)?$").unwrap();
    let mut ret = Vec::new();

    let chr_map: HashMap<_, _> = chrom_list
        .iter()
        .enumerate()
        .map(|(a, b)| (b.name.to_string(), a))
        .collect();

    if let Some(regions) = regions {
        for region_spec in regions {
            if let Some(captures) = region_pattern.captures(&region_spec) {
                let chr = captures.name("CHR").unwrap().as_str();
                let start: u32 = captures
                    .name("FROM")
                    .map_or(0u32, |x| x.as_str().parse().unwrap_or(0));
                let end: u32 = captures.name("TO").map_or_else(
                    || {
                        chr_map
                            .get(chr)
                            .map_or(!0, |&id| chrom_list[id].size as u32)
                    },
                    |x| x.as_str().parse().unwrap_or(!0),
                );
                if let Some(&chr) = chr_map.get(chr) {
                    ret.push((chr, start, end));
                } else {
                    eprintln!(
                        "Warning: ignore chromosome {} which is not defined in d4 file",
                        chr
                    );
                }
                continue;
            } else {
                return Err(Error::new(std::io::ErrorKind::Other, "Invalid region spec"));
            }
        }
    } else {
        for (id, chrom) in chrom_list.iter().enumerate() {
            ret.push((id, 0, chrom.size as u32));
        }
    }

    ret.sort_unstable();

    Ok(ret)
}

fn flush_value<W: Write>(
    target: W,
    chr: &str,
    left: u32,
    right: u32,
    values: &[i32],
    denominators: &[Option<f64>],
    print_zeros: bool,
) -> IOResult<()> {
    if (print_zeros || values.iter().any(|&x| x != 0)) && left < right {
        write_bed_record_fast(target, chr, left, right, values, denominators)?;
    }
    Ok(())
}

fn show_region<R: Read + Seek>(
    inputs: &mut [D4TrackReader<R>],
    regions: &[(usize, u32, u32)],
    print_all_zero: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if inputs.is_empty() {
        return Ok(());
    }

    let mut stdout = std::io::stdout();
    let denominators: Vec<_> = inputs.iter().map(|x| x.get_denominator()).collect();

    for &(cid, begin, end) in regions {
        let chrom_list = inputs[0].chrom_list();
        let chrom = chrom_list[cid].name.as_str().to_string();

        let mut values = vec![0; inputs.len()];
        let mut prev_values = vec![0; inputs.len()];

        let mut views: Vec<_> = inputs
            .iter_mut()
            .map(|x| x.get_view(&chrom, begin, end).unwrap())
            .collect();

        let mut value_changed = false;
        let mut last_pos = begin;

        for pos in begin..end {
            for (input_id, input) in views.iter_mut().enumerate() {
                let (reported_pos, value) = input.next().unwrap()?;
                assert_eq!(reported_pos, pos);
                if values[input_id] != value {
                    if !value_changed {
                        prev_values.clone_from(&values);
                        value_changed = true;
                    }
                    values[input_id] = value;
                }
            }
            if value_changed {
                flush_value(
                    &mut stdout,
                    &chrom,
                    last_pos,
                    pos,
                    prev_values.as_slice(),
                    denominators.as_slice(),
                    print_all_zero,
                )?;
                last_pos = pos;
                value_changed = false;
            }
        }

        if last_pos != end {
            flush_value(
                &mut stdout,
                &chrom,
                last_pos,
                end,
                values.as_slice(),
                denominators.as_slice(),
                print_all_zero,
            )?;
        }
    }

    stdout.flush()?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn show_impl<R: Read + Seek, I: Iterator<Item = String>>(
    mut reader: R,
    pattern: Regex,
    track: Option<&str>,
    regions: Option<I>,
    first: bool,
    print_all_zero: bool,
    show_genome: bool,
    print_header: bool,
) -> AppResult<()> {
    let mut path_buf = vec![];
    let mut first_found = false;
    if let Some(track_path) = track {
        path_buf.push(track_path.into());
    } else {
        find_tracks(
            &mut reader,
            |path| {
                let stem = path
                    .map(|what: &Path| {
                        what.file_name()
                            .map(|x| x.to_string_lossy())
                            .unwrap_or_else(|| Cow::<str>::Borrowed(""))
                    })
                    .unwrap_or_default();
                if pattern.is_match(stem.borrow()) {
                    if first && first_found {
                        false
                    } else {
                        first_found = true;
                        true
                    }
                } else {
                    false
                }
            },
            &mut path_buf,
        )?;
    }

    let file_root = Directory::open_root(reader, 8)?;

    let mut readers = vec![];

    if print_header {
        print!("#Chr\tStart\tEnd");
    }

    for path in path_buf.iter() {
        let track_root = match file_root.open(path)? {
            OpenResult::SubDir(track_root) => track_root,
            _ => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("Unable to open track {}", path.to_string_lossy()),
                )
                .into())
            }
        };

        if print_header {
            print!(
                "\t{}",
                path.file_name()
                    .map(|x| x.to_string_lossy().to_string())
                    .unwrap_or_else(|| "<null>".to_string())
            );
        }

        let reader = D4TrackReader::from_track_root(track_root)?;
        readers.push(reader);
    }

    if print_header {
        println!();
    }

    if !d4tools::check_reference_consistency(readers.iter().map(|r| r.chrom_list())) {
        return Err(Error::new(
            ErrorKind::Other,
            "Inconsistent reference genome".to_string(),
        )
        .into());
    }

    if show_genome {
        let chrom_list = readers[0].chrom_list();
        for chrom in chrom_list {
            println!("{}\t{}", chrom.name, chrom.size);
        }
        return Ok(());
    }

    let regions = parse_region_spec(regions, readers[0].chrom_list())?;

    show_region(&mut readers, &regions, print_all_zero)
}

pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .version(d4tools::VERSION)
        .get_matches_from(args);
    //let mut data_path = vec![];
    let mut input_filename = matches.value_of("input-file").unwrap();

    let (track_pattern, track_path) = if let Some(ofs) = input_filename.find(".d4:") {
        let track_path = &input_filename[ofs + 4..];
        input_filename = &input_filename[..ofs + 3];
        (
            regex::Regex::new(matches.value_of("filter").unwrap_or(".*"))?,
            Some(track_path),
        )
    } else {
        (
            regex::Regex::new(matches.value_of("filter").unwrap_or(".*"))?,
            None,
        )
    };

    let should_print_zero = !matches.is_present("no-missing-data");
    let show_genome = matches.is_present("show-genome");
    let print_header = matches.is_present("header");

    let regions = if let Some(region_file) = matches.value_of("region-file") {
        let mut file = BufReader::new(File::open(region_file)?);
        let mut buf = String::new();
        let mut region_list = Vec::new();
        while file.read_line(&mut buf)? > 0 {
            if &buf[..1] == "#" {
                continue;
            }
            let mut splitted = buf.trim().split('\t');
            let (raw_chr, raw_beg, raw_end) = (splitted.next(), splitted.next(), splitted.next());
            if raw_chr.is_some() && raw_beg.is_some() && raw_end.is_some() {
                if let Ok(begin) = raw_beg.unwrap().parse::<u32>() {
                    if let Ok(end) = raw_end.unwrap().parse::<u32>() {
                        region_list.push(format!("{}:{}-{}", raw_chr.unwrap(), begin, end));
                    }
                }
                buf.clear();
                continue;
            }
            panic!("Invalid bed file");
        }
        Some(region_list.into_iter())
    } else {
        matches
            .values_of("regions")
            .map(|x| x.map(|y| y.to_owned()).collect::<Vec<_>>().into_iter())
    };

    if input_filename.starts_with("http://") || input_filename.starts_with("https://") {
        let reader = HttpReader::new(input_filename)?; //.buffered()?;
        show_impl(
            reader,
            track_pattern,
            track_path,
            regions,
            matches.is_present("first"),
            should_print_zero,
            show_genome,
            print_header,
        )?;
    } else {
        let reader = File::open(input_filename)?;
        show_impl(
            reader,
            track_pattern,
            track_path,
            regions,
            matches.is_present("first"),
            should_print_zero,
            show_genome,
            print_header,
        )?;
    }

    Ok(())
}
