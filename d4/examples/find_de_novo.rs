use d4::{
    task::{Mean, Task, TaskPartition},
    D4MatrixReader, D4TrackReader, MultiTrackReader,
};
use std::{collections::HashMap, marker::PhantomData};

fn compute_chrom_mean_depth(
    reader: &mut D4TrackReader,
) -> Result<HashMap<String, (u32, f64)>, Box<dyn std::error::Error>> {
    let regions: Vec<_> = reader
        .chrom_regions()
        .into_iter()
        .filter_map(|(c, b, e)| {
            if c.starts_with("chr") && c[3..].parse::<u32>().is_ok() {
                return Some((c.to_string(), b, e));
            }
            None
        })
        .collect();
    let result = reader
        .run_tasks::<_, Mean>(&regions)?
        .into_iter()
        .map(|res| (res.chrom.to_string(), (res.end, *res.output)))
        .collect();
    Ok(result)
}

struct TaskPart<'a> {
    part_size: f64,
    depth_sum: [f64; 3],
    mean_depth: [f64; 3],
    _p: PhantomData<&'a i32>,
}
impl<'a, T: Iterator<Item = i32> + ExactSizeIterator> TaskPartition<T> for TaskPart<'a> {
    type ParentType = FindDeNovoTask<'a>;

    type ResultType = bool;

    fn new(left: u32, right: u32, parent: &Self::ParentType) -> Self {
        Self {
            part_size: (right - left) as f64,
            depth_sum: [0.0, 0.0, 0.0],
            mean_depth: parent.mean_depth,
            _p: Default::default(),
        }
    }

    #[inline(always)]
    fn feed_range(&mut self, left: u32, right: u32, value: &mut T) -> bool {
        for (i, d) in value.enumerate() {
            self.depth_sum[i] += d as f64 * (right - left) as f64;
        }
        true
    }

    fn result(&mut self) -> Self::ResultType {
        let mut part_mean = [0.0; 3];
        for i in 0..3 {
            part_mean[i] = self.depth_sum[i] / self.part_size / self.mean_depth[i];
        }
        if (part_mean[1] - 1.0).abs() < 0.2
            && (part_mean[2] - 1.0).abs() < 0.2
            && part_mean[0] < 0.2
        {
            return true;
        }
        false
    }
}

struct FindDeNovoTask<'a> {
    chrom: &'a str,
    begin: u32,
    end: u32,
    mean_depth: [f64; 3],
}

impl<'a, T: Iterator<Item = i32> + ExactSizeIterator> Task<T> for FindDeNovoTask<'a> {
    type Partition = TaskPart<'a>;
    type Output = bool;
    fn region(&self) -> (&str, u32, u32) {
        (self.chrom, self.begin, self.end)
    }
    fn combine(&self, parts: &[bool]) -> Self::Output {
        parts.iter().filter(|x| **x).count() as f64 > parts.len() as f64 * 0.8
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let input_path = std::env::args().nth(1).unwrap();
    let mut tracks: Vec<D4TrackReader> = D4TrackReader::open_tracks(&input_path, |_| true)?;

    let mean_depth: Vec<_> = tracks
        .iter_mut()
        .map(|t| compute_chrom_mean_depth(t).unwrap())
        .collect();
    const RES: u32 = 500;

    let mut tasks = vec![];
    for (chr, (size, _)) in mean_depth[0].iter() {
        for i in 0..(size + RES - 1) / RES {
            let begin = i * RES;
            let end = (begin + RES).min(*size);
            tasks.push(FindDeNovoTask {
                chrom: chr,
                begin,
                end,
                mean_depth: [
                    mean_depth[0][chr].1,
                    mean_depth[1][chr].1,
                    mean_depth[2][chr].1,
                ],
            });
        }
    }

    let mut mr = D4MatrixReader::new(tracks)?;
    for result in mr.run_tasks(tasks)?.into_iter() {
        if *result.output {
            println!("{}\t{}\t{}", result.chrom, result.begin, result.end);
        }
    }

    Ok(())
}
