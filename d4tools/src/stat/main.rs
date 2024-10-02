use clap::{load_yaml, App, ArgMatches};

use d4::{
    find_tracks,
    index::{D4IndexCollection, Sum},
    ssio::http::HttpReader,
    task::{Histogram, Mean, PercentCov, SimpleTask, Task, TaskOutput},
    Chrom, D4TrackReader,
};

use std::{
    borrow::{Borrow, Cow},
    io::{BufRead, BufReader},
};
use std::{fs::File, iter::Once};
use std::{
    io::{Read, Seek},
    path::Path,
};

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
fn parse_region_spec(
    region_file: Option<&str>,
    chrom_list: &[Chrom],
) -> Result<Vec<(String, u32, u32)>, Box<dyn std::error::Error>> {
    Ok(if let Some(path) = region_file {
        parse_bed_file(path)?.collect()
    } else {
        chrom_list
            .iter()
            .map(|chrom| (chrom.name.clone(), 0u32, chrom.size as u32))
            .collect()
    })
}

fn open_file_parse_region_and_then<T, F>(
    matches: ArgMatches,
    tags: &mut Vec<String>,
    func: F,
) -> Result<T, Box<dyn std::error::Error>>
where
    F: FnOnce(Vec<D4TrackReader>, Vec<(String, u32, u32)>) -> Result<T, Box<dyn std::error::Error>>,
{
    let input_filename = matches.value_of("input").unwrap();
    let mut data_path = vec![];

    let d4files: Vec<D4TrackReader> = if matches.is_present("first") || input_filename.contains(':')
    {
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

    let region_spec =
        parse_region_spec(matches.value_of("region"), d4files[0].header().chrom_list())?;

    *tags = data_path;

    func(d4files, region_spec)
}

pub struct OwnedOutput<T> {
    chrom: String,
    begin: u32,
    end: u32,
    output: T,
}

#[allow(clippy::type_complexity)]
fn run_task<T: Task<Once<i32>> + SimpleTask + Clone>(
    matches: ArgMatches,
    file_tags: &mut Vec<String>,
    denominators: &mut Vec<Option<f64>>,
) -> Result<Vec<OwnedOutput<Vec<T::Output>>>, Box<dyn std::error::Error>>
where
    T::Output: Clone,
{
    open_file_parse_region_and_then(matches, file_tags, |inputs, region_spec| {
        let mut ret = vec![];
        for mut input in inputs {
            if input.header().is_integral() {
                denominators.push(None);
            } else {
                denominators.push(Some(input.header().get_denominator()));
            }
            let result = T::create_task(&mut input, &region_spec)?.run();
            for (idx, result) in result.into_iter().enumerate() {
                if ret.len() <= idx {
                    ret.push(OwnedOutput {
                        output: vec![result.output.clone()],
                        begin: result.begin,
                        end: result.end,
                        chrom: result.chrom.to_string(),
                    });
                } else {
                    ret[idx].output.push(result.output.clone());
                }
            }
        }
        Ok(ret)
    })
}

fn percentile_stat(
    matches: ArgMatches,
    percentile: f64,
    mut print_header: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut tags = Vec::new();
    if print_header {
        print!("#Chr\tBegin\tEnd");
    }
    let mut denominators = Vec::new();
    let histograms = run_task::<Histogram>(matches, &mut tags, &mut denominators)?;
    for OwnedOutput {
        chrom: chr,
        begin,
        end,
        output: results,
    } in histograms
    {
        if print_header {
            for tag in tags.iter() {
                print!("\t{}", tag);
            }
            println!();
            print_header = false;
        }
        print!("{}\t{}\t{}", chr, begin, end);
        for ((below, hist, above), &denominator) in results.into_iter().zip(denominators.iter()) {
            let count: u32 = below + hist.iter().sum::<u32>() + above;
            let below_count = (count as f64 * percentile.clamp(0.0, 1.0)).round() as u32;
            let mut current = below;
            let mut idx = 0;
            while current < below_count && idx < hist.len() {
                current += hist[idx];
                idx += 1;
            }
            print!("\t{}", idx as f64 / denominator.unwrap_or(1.0));
        }
        println!();
    }
    Ok(())
}

fn hist_stat(matches: ArgMatches) -> Result<(), Box<dyn std::error::Error>> {
    let max_bin = matches.value_of("max-bin").unwrap_or("1000").parse()?;
    let mut unused = Vec::new();
    let (histograms, denominators) =
        open_file_parse_region_and_then(matches, &mut unused, |mut input, regions| {
            let tasks: Vec<_> = regions
                .into_iter()
                .map(|(chr, begin, end)| Histogram::with_bin_range(&chr, begin, end, 0..max_bin))
                .collect();
            let denominator: Vec<_> = input.iter().map(|x| x.header().get_denominator()).collect();
            Ok((
                Histogram::create_task(&mut input[0], tasks)?.run(),
                denominator,
            ))
        })?;
    let mut hist_result = vec![0; max_bin as usize + 1];
    let (mut below, mut above) = (0, 0);
    for TaskOutput {
        output: (b, hist, a),
        ..
    } in histograms.into_iter()
    {
        below += b;
        above += a;
        for (id, val) in hist.iter().enumerate() {
            hist_result[id + 1] += val;
        }
    }

    println!("<0\t{}", below);
    for (val, cnt) in hist_result[1..].iter().enumerate() {
        println!("{}\t{}", val as f64 / denominators[0], cnt);
    }
    println!(">{}\t{}", max_bin, above);

    Ok(())
}
fn parse_stat_thresholds(stat: &str) -> Result<Vec<u32>, Box<dyn std::error::Error>> {
    let thresholds_str = &stat["perc_cov=".len()..];

    let mut thresholds: Vec<u32> = thresholds_str
        .split(',')
        .map(|s| s.trim().parse())
        .collect::<Result<Vec<u32>, _>>()?;
    thresholds.sort_unstable();
    Ok(thresholds)
}

fn perc_cov_stat(
    matches: ArgMatches,
    mut print_header: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let stat = matches.value_of("stat").unwrap_or("perc_cov=10,20,30");

    let thresholds = parse_stat_thresholds(stat)?;
    let mut unused = Vec::new();
    let results = open_file_parse_region_and_then(matches, &mut unused, |mut input, regions| {
        let tasks: Vec<_> = regions
            .into_iter()
            .map(|(chr, begin, end)| PercentCov::new(&chr, begin, end, thresholds.clone()))
            .collect();

        Ok(PercentCov::create_task(&mut input[0], tasks)?.run())
    })?;

    if print_header {
        print!("#Chr\tStart\tEnd");
        for t in thresholds.iter() {
            print!("\t{}x", t);
        }
        println!();
        print_header = false;
    }
    for r in results.into_iter() {
        print!("{}\t{}\t{}", r.chrom, r.begin, r.end);
        for value in r.output.iter() {
            if value.abs() >= 0.001 {
                print!("\t{:.3}", value);
            } else {
                print!("\t{:.3e}", value);
            }
        }
        println!();
    }
    Ok(())
}

fn mean_stat_index<R: Read + Seek>(
    mut reader: R,
    track: Option<&str>,
    print_header: bool,
    region_file: Option<&str>,
    sum_only: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let mut tracks = Vec::new();

    if let Some(name) = track {
        tracks.push(name.into());
    } else {
        find_tracks(&mut reader, |_| true, &mut tracks)?;
    }

    if tracks.is_empty() {
        panic!("At least one track should be present in the file");
    }
    let file_root = d4_framefile::Directory::open_root(reader, 8)?;

    let root_dir: Vec<_> = tracks
        .iter()
        .map(|name| match file_root.open(name).unwrap() {
            d4_framefile::OpenResult::SubDir(dir) => dir,
            _ => panic!("Invalid track root"),
        })
        .collect();

    let mut index: Vec<_> = Vec::new();
    for idx_obj in root_dir.iter().map(|root| {
        let index = D4IndexCollection::from_root_container(root)?;
        index.load_data_index::<Sum>()
    }) {
        if idx_obj.is_err() {
            return Ok(false);
        }
        index.push(idx_obj.unwrap());
    }

    let mut ssio_reader: Vec<_> = root_dir
        .iter()
        .map(|root| d4::ssio::D4TrackReader::from_track_root(root.clone()).unwrap())
        .collect();

    let regions = parse_region_spec(region_file, ssio_reader[0].chrom_list())?;

    if print_header {
        print!("#Chr\tBegin\tEnd");
        for track in tracks {
            if let Some(stem) = track.file_stem() {
                print!("\t{}", stem.to_string_lossy());
            } else {
                print!("\t<default>");
            }
        }
        println!();
    }

    for (chr, begin, end) in regions {
        print!("{}\t{}\t{}", chr, begin, end);
        for (sum_index, ssio_reader) in index.iter().zip(ssio_reader.iter_mut()) {
            let index_res = sum_index.query(chr.as_str(), begin, end).unwrap();
            let sum_res = index_res.get_result(ssio_reader)?;
            let value = if sum_only {
                sum_res.sum()
            } else {
                sum_res.mean(index_res.query_size())
            };
            print!("\t{}", value / ssio_reader.get_denominator().unwrap_or(1.0));
        }
        println!();
    }

    Ok(true)
}

pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .version(d4tools::VERSION)
        .get_matches_from(&args);
    if let Some(threads) = matches.value_of("threads") {
        let threads = threads.parse().unwrap();
        rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build_global()?;
    }

    if matches.value_of("stat") == Some("count") {
        let path = matches.value_of("input").unwrap();
        let chrom_list = if path.starts_with("http://") || path.starts_with("https://") {
            d4::ssio::D4TrackReader::from_url(path)?
                .chrom_list()
                .to_owned()
        } else {
            let reader: d4::D4TrackReader = d4::D4TrackReader::open(path)?;
            reader
                .chrom_regions()
                .iter()
                .map(|(c, _s, e)| Chrom {
                    name: c.to_string(),
                    size: *e as usize,
                })
                .collect()
        };
        let region_file = matches.value_of("region");
        let region_spec = parse_region_spec(region_file, &chrom_list)?;
        if matches.is_present("header") {
            print!("#Chr\tBegin\tEnd");
        }
        for (chr, start, end) in region_spec {
            println!("{}\t{}\t{}\t{}", chr, start, end, end - start);
        }
        return Ok(());
    }

    if !matches.is_present("no-index")
        && (matches.value_of("stat") == Some("mean")
            || matches.value_of("stat") == Some("avg")
            || matches.value_of("stat") == Some("sum")
            || matches.value_of("stat") == Some("count")
            || !matches.is_present("stat"))
        && matches.values_of("input").unwrap().len() == 1
    {
        let path = matches.value_of("input").unwrap();
        let region_file = matches.value_of("region");
        let sum_only = matches.value_of("stat") == Some("sum");
        if path.starts_with("http://") || path.starts_with("https://") {
            let (url, track) = if let Some(pos) = path.rfind('#') {
                (&path[..pos], Some(&path[pos + 1..]))
            } else {
                (path, None)
            };
            let reader = HttpReader::new(url)?;
            if mean_stat_index(
                reader,
                track,
                matches.is_present("header"),
                region_file,
                sum_only,
            )? {
                return Ok(());
            }
        } else {
            let (path, track) = if let Some(pos) = path.rfind(':') {
                (&path[..pos], Some(&path[pos + 1..]))
            } else {
                (path, None)
            };
            let reader = File::open(path)?;
            if mean_stat_index(
                reader,
                track,
                matches.is_present("header"),
                region_file,
                sum_only,
            )? {
                return Ok(());
            }
        }
    }

    if matches
        .values_of("input")
        .unwrap()
        .any(|x| x.starts_with("http://") || x.starts_with("https://"))
    {
        panic!(
            "For HTTP/HTTPS stat, we currently only support single track and only for mean dpeth"
        );
    }
    let mut header_printed = !matches.is_present("header");

    match matches.value_of("stat") {
        None | Some("mean") | Some("avg") => {
            if !header_printed {
                print!("#Chr\tBegin\tEnd");
            }
            let mut tags = Vec::new();
            let mut denoms = Vec::new();
            for result in run_task::<Mean>(matches, &mut tags, &mut denoms)? {
                if !header_printed {
                    for tag in tags.iter() {
                        print!("\t{}", tag);
                    }
                    println!();
                    header_printed = true;
                }
                print!("{}\t{}\t{}", result.chrom, result.begin, result.end);
                for (value, denom) in result.output.into_iter().zip(denoms.iter()) {
                    print!("\t{}", value / denom.unwrap_or(1.0))
                }
                println!();
            }
        }
        Some("sum") => {
            if !header_printed {
                print!("#Chr\tBegin\tEnd");
            }
            let mut tags = Vec::new();
            let mut denoms = Vec::new();
            for result in run_task::<d4::task::Sum>(matches, &mut tags, &mut denoms)? {
                if !header_printed {
                    for tag in tags.iter() {
                        print!("\t{}", tag);
                    }
                    println!();
                    header_printed = true;
                }
                print!("{}\t{}\t{}", result.chrom, result.begin, result.end);
                for (value, denom) in result.output.into_iter().zip(denoms.iter()) {
                    print!("\t{}", value as f64 / denom.unwrap_or(1.0))
                }
                println!();
            }
        }
        Some("median") => {
            percentile_stat(matches, 0.5, !header_printed)?;
        }
        Some("hist") => {
            hist_stat(matches)?;
        }
        Some(whatever) if whatever.starts_with("perc_cov") => {
            perc_cov_stat(matches, !header_printed)?;
        }
        Some(whatever) if whatever.starts_with("percentile=") => {
            let prefix_len = "percentile=".len();
            let percentile: f64 = whatever[prefix_len..].parse()?;
            percentile_stat(matches, percentile / 100.0, !header_printed)?;
        }
        _ => panic!("Unsupported stat type: {:?}", matches.value_of("stat")),
    }
    Ok(())
}
