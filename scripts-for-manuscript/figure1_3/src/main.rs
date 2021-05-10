use d4::ptab::{DecodeResult, PTablePartitionReader, UncompressedReader};
use d4::stab::{RangeRecord, STablePartitionReader, SimpleKeyValueReader};
use d4::D4FileReader;
use plotters::prelude::*;
use rayon::prelude::*;
use serde_json::{from_str as from_json, to_string as to_json};
use std::collections::BTreeMap;
use std::env::args;

use std::fs::File;
use std::io::{Read, Write};

type D4Reader = D4FileReader<UncompressedReader, SimpleKeyValueReader<RangeRecord>>;
fn get_stats(path: &str) -> (Vec<usize>, Vec<(usize, usize)>) {
    if let Some(result) = File::open(format!("{}.json", path))
        .ok()
        .map(|mut file| {
            let mut buf = String::new();
            file.read_to_string(&mut buf).unwrap();
            from_json(buf.as_ref()).ok()
        })
        .flatten()
    {
        return result;
    }

    let mut input = D4Reader::open(path).unwrap();

    let parts = input.split(Some(10_000_000)).unwrap();

    let hist: Vec<_> = parts
        .into_par_iter()
        .map(|(mut pt, mut st)| {
            let mut result = BTreeMap::new();
            let (chr, left, right) = pt.region();
            if chr
                .chars()
                .all(|c| !((c >= '0' && c <= '9') || c == 'X' || c == 'Y'))
            {
                return result;
            }
            let dec = pt.as_decoder();
            for pos in left..right {
                let value = match dec.decode(pos as usize) {
                    DecodeResult::Definitely(value) => value,
                    DecodeResult::Maybe(value) => st.decode(pos).unwrap_or(value),
                };
                *result.entry(value).or_insert(0usize) += 1;
            }
            result
        })
        .collect();

    let mut merged = vec![
        0;
        hist.iter()
            .map(|h| *h.keys().max().unwrap_or(&0) as usize)
            .max()
            .unwrap()
            + 1
    ];

    for h in hist.iter() {
        for (k, v) in h {
            merged[*k as usize] += *v as usize;
        }
    }

    let parts = input.split(Some(10000_000)).unwrap();

    let es: Vec<_> = parts
        .into_par_iter()
        .map(|(mut pt, mut st)| {
            let (chr, left, right) = pt.region();
            if chr
                .chars()
                .all(|c| !((c >= '0' && c <= '9') || c == 'X' || c == 'Y'))
            {
                return (vec![0; 32], 0);
            }
            let dec = pt.as_decoder();
            let mut region_left = [None; 32];
            let mut region_count = vec![0; 32];
            for pos in left..right {
                let value = match dec.decode(pos as usize) {
                    DecodeResult::Definitely(value) => value,
                    DecodeResult::Maybe(value) => st.decode(pos).unwrap_or(value),
                };
                for d in 0..31 {
                    if value >= (1 << d) {
                        if let Some(val) = region_left[d].clone() {
                            if val != value {
                                region_count[d] += 1;
                            }
                        }
                        region_left[d] = Some(value);
                    } else {
                        if region_left[d].is_some() {
                            region_count[d] += 1;
                        }
                        region_left[d] = None;
                    }
                }
            }
            for d in 0..31 {
                if region_left[d].is_some() {
                    region_count[d] += 1;
                }
            }
            (region_count, right - left)
        })
        .collect();

    let mut encode = vec![(0, 0); 31];

    for d in 0..31 {
        for (rc, size) in es.iter() {
            encode[d].0 += rc[d] * 80;
            encode[d].1 += *size as usize * d as usize;
        }
    }

    let ret = (merged, encode);

    File::create(format!("{}.json", path))
        .unwrap()
        .write_all(to_json(&ret).unwrap().as_bytes())
        .unwrap();

    ret
}

fn main() {
    let results: Vec<_> = args().skip(1).map(|p| get_stats(p.as_ref())).collect();

    let max_k = 7;

    let root = SVGBackend::new("/tmp/result.svg", (1100, 768)).into_drawing_area();
    root.fill(&WHITE).unwrap();
    for (root, data) in root
        .split_evenly((1, results.len()))
        .into_iter()
        .zip(results.into_iter())
    {
        let root = root.margin(10, 10, 20, 20);
        let areas = root.split_evenly((2, 1));
        let x_range = 0usize..(1 + (1usize << max_k));
        let y_range = 0..data.0.iter().max().map(|&x| x).unwrap();
        let mut chart = ChartBuilder::on(&areas[0])
            .set_label_area_size(LabelAreaPosition::Top, 40)
            .set_label_area_size(LabelAreaPosition::Right, 60)
            .set_label_area_size(LabelAreaPosition::Left, 60)
            .build_cartesian_2d(x_range.clone().with_key_points((3..32).map(|x:usize|1<<x).collect()), y_range)
            .unwrap()
            .set_secondary_coord(x_range.clone().with_key_points((3..32).map(|x:usize|1<<x).collect()), 0.0..1.0);
        chart
            .configure_mesh()
            .disable_mesh()
            .y_desc("Count(x1,000,000)")
            .y_label_formatter(&|x| format!("{:.0}", x / 1_000_000))
            .x_labels(5)
            .draw()
            .unwrap();
        chart
            .configure_secondary_axes()
            .y_label_formatter(&|x| format!("{:.0}%", x * 100.0))
            .x_desc("Depth")
            .x_labels(5)
            .x_label_formatter(&|y| format!("{:.0}", y))
            .y_desc("% of Data Points")
            .draw()
            .unwrap();

        let mut elements = vec![0usize; (1<<max_k) + 1];
        for (mut x, &y) in data.0.iter().enumerate() {
            x = x.min(1<<max_k);
            elements[x] += y;
        }

        chart
            .draw_series(
                elements.iter().enumerate().take_while(|&(x, _)| x <= (1<<max_k)).map(|(x0, &y)|{
                    Rectangle::new([(x0, 0), (x0 + 1, y)], BLUE.filled())
                })
            )
            .unwrap()
            .label("# of pos with depth")
            .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 10, y + 5)], BLUE.filled()));

        let mut accu = vec![(0.0, 0); elements.len()];
        accu[0] = (0.0, elements[0]);
        for i in 1..elements.len() {
            accu[i].0 = i as f64 * 1.0;
            accu[i].1 += accu[i - 1].1 + elements[i];
        }

        let sum = accu.last().unwrap().1;

        chart
            .draw_secondary_series(LineSeries::new(
                accu.iter()
                    .filter(|&(i, _)| (*i as usize) < 1 << max_k)
                    .map(|&(i, c)| (i as usize + 1, c as f64 / sum as f64)),
                &RED,
            ))
            .unwrap()
            .label("Culumative % of Pos")
            .legend(|(x, y)| Rectangle::new([(x, y - 5), (x + 10, y + 5)], RED.filled()));
        chart
            .configure_series_labels()
            .background_style(RGBColor(240, 240, 240).filled())
            .draw()
            .unwrap();


        let total_size: Vec<_> = data.1.iter().map(|&(x, y)| (x + y) / 8).collect();

        const GB:f64 = 1024.0 * 1024.0 * 1024.0;
        let mut chart = ChartBuilder::on(&areas[1])
            .x_label_area_size(40)
            .y_label_area_size(60)
            .right_y_label_area_size(60)
            .build_cartesian_2d(
                (0..128usize).clone().with_key_points((3..32).map(|x:usize|1<<x).collect()),
                (0..*total_size.iter().max().unwrap()).with_key_points((0..20).map(|x| (x as f64 * GB) as usize).collect::<Vec<_>>()),
            )
            .unwrap();
        chart
            .configure_mesh()
            .disable_mesh()
            .y_label_formatter(&|x| format!("{:.1}", *x as f64 / 1024.0 / 1024.0 / 1024.0))
            .x_labels(5)
            .y_desc("Size(GiB)")
            .x_desc("Dictionary Size")
            .draw()
            .unwrap();

        chart.draw_series(
                LineSeries::new(data.1.iter().take(max_k + 1).enumerate().map(|(idx, &(s, _))| (1usize << idx, s/8)), &BLUE)
            )
            .unwrap()
            .label("Seconary table size")
            .legend(|(x,y)| Rectangle::new([(x, y- 5), (x + 10, y+ 5)], BLUE.filled()));
        chart.draw_series(
                data.1.iter().enumerate().take(max_k + 1).map(|(idx, &(s, _))| (1usize << idx, s/8)).map(|(x,y)| Circle::new((x,y), 3, BLUE.filled()))
            )
            .unwrap();
        
        chart.draw_series(
                LineSeries::new(data.1.iter().enumerate().take(max_k + 1).map(|(idx, &(_, s))| (1usize << idx, s/8)), &RED)
            )
            .unwrap()
            .label("Primary table size")
            .legend(|(x,y)| Rectangle::new([(x, y- 5), (x + 10, y+ 5)], RED.filled()));
        chart.draw_series(
                data.1.iter().enumerate().take(max_k + 1).map(|(idx, &(_, s))| (1usize << idx, s/8)).map(|(x,y)| Circle::new((x,y), 3, RED.filled()))
            )
            .unwrap();
        
        chart.draw_series(
                LineSeries::new(data.1.iter().take(max_k + 1).enumerate().map(|(idx, &(p, s))| (1usize << idx, (s + p)/8)), &BLACK)
            )
            .unwrap()
            .label("Total size")
            .legend(|(x,y)| Rectangle::new([(x, y- 5), (x + 10, y+ 5)], BLACK.filled()));
        chart.draw_series(
                data.1.iter().enumerate().take(max_k + 1).map(|(idx, &(p, s))| (1usize << idx, (s + p)/8)).map(|(x,y)| Circle::new((x,y), 3, BLACK.filled()))
            )
            .unwrap();
        chart
            .configure_series_labels()
            .background_style(RGBColor(240, 240, 240).mix(0.1).filled())
            .draw()
            .unwrap();
    }
}
