use clap::{load_yaml, App};
use d4::ptab::{DecodeResult, PTablePartitionReader, UncompressedReader};
use d4::stab::{RangeRecord, STablePartitionReader, SimpleKeyValueReader};
use d4::D4FileReader;
use regex::Regex;
pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches_from(args);
    let mut d4file = D4FileReader::<UncompressedReader, SimpleKeyValueReader<RangeRecord>>::open(
        matches.value_of("input-file").unwrap(),
    )?;

    if matches.values_of("show-genome").is_some() {
        let hdr = d4file.header();
        for chrom in hdr.chrom_list() {
            println!("{}\t{}", chrom.name, chrom.size);
        }
        return Ok(());
    }

    let partition = d4file.split(None)?;

    let region_pattern = Regex::new(r"^(?P<CHR>[^:]+)((:(?P<FROM>\d+)-)?(?P<TO>\d+)?)?$")?;
    let mut should_print_all = true;
    let mut partition_context: Vec<_> = partition.into_iter().map(|part| (part, vec![])).collect();
    matches.values_of("regions").map(|regions| {
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
                    return (chr.to_string(), start, end);
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
    });

    for ((mut ptab, mut stab), mut regions) in partition_context {
        let (chr, ts, te) = ptab.region();
        let chr = chr.to_string();
        let all_region = vec![(ts, te)];
        let regions = if !should_print_all {
            regions.sort();
            regions
        } else {
            all_region
        };

        let decoder = ptab.as_decoder();
        for (from, to) in regions {
            let mut last = None;
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
                        println!("{}\t{}\t{}\t{}", chr, begin, pos, val);
                        last = Some((pos, value));
                    }
                } else {
                    last = Some((pos, value));
                }
            }
            if let Some(last) = last {
                println!("{}\t{}\t{}\t{}", chr, last.0, to, last.1);
            }
        }
    }

    Ok(())
}
