use clap::{load_yaml, App};
use d4::task::{Task, TaskOutput};
use plotters::prelude::*;
use regex::Regex;

fn downsample_data(
    path: &str,
    chr: &str,
    mut range: (u32, u32),
    npoints: usize,
) -> Result<Vec<(u32, f64)>, Box<dyn std::error::Error>> {
    let mut input: d4::D4TrackReader = d4::D4TrackReader::open(path)?;

    let target = input
        .header()
        .chrom_list()
        .iter()
        .find(|this| this.name == chr)
        .unwrap();
    range.0 = range.0.min(target.size as u32);
    range.1 = range.1.max(range.0).min(target.size as u32);

    let base_per_point = (range.1 - range.0 + 1) as usize / npoints;
    let mut extra_point = (range.1 - range.0 + 1) as usize % npoints;
    let mut last_end = range.0;
    let stat_parts: Vec<_> = (0..npoints)
        .map(|_| {
            let new_end = last_end
                + base_per_point as u32
                + if extra_point == 0 {
                    0
                } else {
                    extra_point -= 1;
                    1
                };

            let ret = (chr.to_string(), last_end, new_end);
            last_end = new_end;
            ret
        })
        .collect();
    let tc = d4::task::Mean::create_task(&mut input, &stat_parts)?;
    Ok(tc
        .run()
        .into_iter()
        .map(
            |TaskOutput {
                 begin: l,
                 end: r,
                 output: &b,
                 ..
             }| ((l + r) / 2, b),
        )
        .collect())
}

pub fn entry_point(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml)
        .version(d4::VERSION)
        .get_matches_from(args);

    let input = matches.value_of("input-file").unwrap();
    let output = matches.value_of("output-file").unwrap();

    let resolution = matches.value_of("resolution").map_or((1440, 768), |res| {
        let parts: Vec<_> = res.split('x').collect();
        (parts[0].parse().unwrap(), parts[1].parse().unwrap())
    });

    let region_pattern = Regex::new(r"^(?P<CHR>[^:]+)((:(?P<FROM>\d+)-)?(?P<TO>\d+)?)?$")?;
    let (chr, left, right) = matches
        .value_of("region")
        .map(|region| {
            if let Some(captures) = region_pattern.captures(region) {
                let chr = captures.name("CHR").unwrap().as_str();
                let start: u32 = captures
                    .name("FROM")
                    .map_or(0u32, |x| x.as_str().parse().unwrap_or(0));
                let end: u32 = captures
                    .name("TO")
                    .map_or(!0u32, |x| x.as_str().parse().unwrap_or(!0));
                return (chr.to_string(), start, end);
            }
            panic!("Invalid region specification");
        })
        .unwrap();

    let data = downsample_data(input, &chr, (left, right), 256)?;

    let pos_range = plotters::data::fitting_range(data.iter().map(|x| &x.0));
    let mut data_range = plotters::data::fitting_range(data.iter().map(|x| &x.1));
    data_range.start = 0.0;

    let root = SVGBackend::new(output, resolution).into_drawing_area();

    let mut chart = ChartBuilder::on(&root)
        .caption(
            format!("{}:{}-{}", chr, pos_range.start, pos_range.end),
            ("sans-serif", (3).percent_height()),
        )
        .set_label_area_size(LabelAreaPosition::Left, (10).percent_height())
        .set_label_area_size(LabelAreaPosition::Bottom, (10).percent_height())
        .margin(5)
        .build_ranged(pos_range, data_range)?;

    chart.configure_mesh().disable_mesh().draw()?;

    chart.draw_series({
        let ret = AreaSeries::new(data.into_iter(), 0.0, &RED.mix(0.2));
        ret.border_style(&RED)
    })?;

    Ok(())
}
