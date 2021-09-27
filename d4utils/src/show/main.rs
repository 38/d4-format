use clap::{load_yaml, App};
use d4::{
    ptab::{DecodeResult, PTablePartitionReader},
    stab::STablePartitionReader,
    D4FileReader,
};
use regex::Regex;
use std::io::{Result as IOResult, Write};
fn write_bed_record_fast<W: Write>(
    mut writer: W,
    chr: &str,
    left: u32,
    right: u32,
    value: i32,
) -> IOResult<()> {
    writer.write_all(chr.as_bytes())?;
    writer.write_all(b"\t")?;
    writer.write_all(left.to_string().as_bytes())?;
    writer.write_all(b"\t")?;
    writer.write_all(right.to_string().as_bytes())?;
    writer.write_all(b"\t")?;
    writer.write_all(value.to_string().as_bytes())?;
    writer.write_all(b"\n")?;
    Ok(())
}
pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches_from(args);
    let mut d4file: D4FileReader = D4FileReader::open(matches.value_of("input-file").unwrap())?;

    if matches.values_of("show-genome").is_some() {
        let hdr = d4file.header();
        for chrom in hdr.chrom_list() {
            println!("{}\t{}", chrom.name, chrom.size);
        }
        return Ok(());
    }

    let should_print_zero = !matches.is_present("no-missing-data");

    let partition = d4file.split(None)?;

    let mut stdout = std::io::BufWriter::new(std::io::stdout());

    let region_pattern = Regex::new(r"^(?P<CHR>[^:]+)((:(?P<FROM>\d+)-)?(?P<TO>\d+)?)?$")?;
    let mut should_print_all = true;
    let mut partition_context: Vec<_> = partition.into_iter().map(|part| (part, vec![])).collect();
    if let Some(regions) = matches.values_of("regions") {
        regions
            .map(move |s: &str| {
                if let Some(captures) = region_pattern.captures(s) {
                    let chr = captures.name("CHR").unwrap().as_str();
                    let start: u32 = captures
                        .name("FROM")
                        .map_or(0u32, |x| x.as_str().parse().unwrap_or(0));
                    let end: u32 = captures
                        .name("TO")
                        .map_or(!0u32, |x| x.as_str().parse().unwrap_or(!0));
                    return (chr.to_string(), start - 1, end - 1);
                }
                panic!("Unexpected region specifier")
            })
            .for_each(|(chr, start, end)| {
                should_print_all = false;
                if let Some(part) = partition_context
                    .iter_mut()
                    .find(|((p, _), _)| p.region().0 == chr)
                {
                    part.1.push((start, end));
                }
            });
    }

    for ((mut ptab, mut stab), mut regions) in partition_context {
        let (chr, ts, te) = ptab.region();
        let chr = chr.to_string();
        let all_region = vec![(ts, te)];
        let regions = if !should_print_all {
            regions.sort_unstable();
            regions
        } else {
            all_region
        };

        let decoder = ptab.as_decoder();
        for (mut from, mut to) in regions {
            to = to.min(te);
            let mut last = None;
            from = from.max(ts);
            to = to.min(te);
            for pos in from..to {
                let value = match decoder.decode(pos as usize) {
                    DecodeResult::Definitely(value) => value,
                    DecodeResult::Maybe(back_value) => {
                        if let Some(value) = stab.decode(pos) {
                            value
                        } else {
                            back_value
                        }
                    }
                };
                if let Some((begin, val)) = last {
                    if value != val {
                        if val != 0 || should_print_zero {
                            write_bed_record_fast(&mut stdout, chr.as_ref(), begin, pos, val)?;
                        }
                        last = Some((pos, value));
                    }
                } else {
                    last = Some((pos, value));
                }
            }
            if let Some(last) = last {
                if last.1 != 0 || should_print_zero {
                    write_bed_record_fast(&mut stdout, chr.as_ref(), last.0, to, last.1)?;
                }
            }
        }
    }

    stdout.flush()?;

    Ok(())
}
