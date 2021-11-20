use d4::{
    task::{Task, TaskPartition},
    D4MatrixReader, D4TrackReader, MultiTrackReader,
};
use std::env::args;

struct CountTaskPart {
    threshold: i32,
    count: u64,
}
impl<T: Iterator<Item = i32> + ExactSizeIterator> TaskPartition<T> for CountTaskPart {
    type ParentType = CountTask;
    type ResultType = u64;
    fn new(_: u32, _: u32, parent: &Self::ParentType) -> Self {
        Self {
            threshold: parent.3,
            count: 0,
        }
    }
    #[inline(always)]
    fn feed_range(&mut self, left: u32, right: u32, value: &mut T) -> bool {
        if value.all(|x| x > self.threshold) {
            self.count += (right - left) as u64;
        }
        true
    }
    fn result(&mut self) -> Self::ResultType {
        self.count
    }
}
struct CountTask(String, u32, u32, i32);
impl<T: Iterator<Item = i32> + ExactSizeIterator> Task<T> for CountTask {
    type Partition = CountTaskPart;
    type Output = u64;
    fn region(&self) -> (&str, u32, u32) {
        (self.0.as_str(), self.1, self.2)
    }
    fn combine(
        &self,
        parts: &[<Self::Partition as d4::task::TaskPartition<T>>::ResultType],
    ) -> Self::Output {
        parts.iter().sum()
    }
}
fn main() {
    let input_path = args().nth(1).unwrap();
    let tracks: Vec<D4TrackReader> = D4TrackReader::open_tracks(input_path, |_| true).unwrap();
    let mut matrix_reader = D4MatrixReader::new(tracks).unwrap();
    let tasks: Vec<_> = matrix_reader
        .chrom_regions()
        .iter()
        .map(|&(chr, begin, end)| CountTask(chr.to_string(), begin, end, 10))
        .collect();
    let result = matrix_reader.run_tasks(tasks).unwrap();
    println!("{}", result.into_iter().map(|x| x.output).sum::<u64>());
}
