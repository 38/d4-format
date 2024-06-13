use std::cmp::Reverse;
use std::collections::BinaryHeap;

#[allow(clippy::all)]
pub mod alignment;
pub use alignment::*;

pub struct DepthIter<'a, R: AlignmentReader<'a>> {
    iter: AlignmentIter<'a, R>,
    cur_chrom: i32,
    cur_pos: usize,
    heap: BinaryHeap<Reverse<usize>>,
    next_read: Option<(i32, usize, usize)>,
    #[allow(clippy::type_complexity)]
    filter: Option<Box<dyn Fn(&Alignment<'_>) -> bool + 'a>>,
}

impl<'a, R: AlignmentReader<'a>> DepthIter<'a, R> {
    pub fn with_filter<F: Fn(&Alignment<'_>) -> bool + 'a>(reader: R, filter: F) -> Self {
        let (chrom, pos) = reader.start();
        let iter = reader.into_alignment_iter();

        let mut ret = Self {
            iter,
            next_read: None,
            cur_chrom: chrom as i32,
            cur_pos: pos as usize,
            heap: BinaryHeap::new(),
            filter: Some(Box::new(filter)),
        };

        ret.load_next();
        ret
    }

    fn load_next(&mut self) {
        self.next_read = loop {
            if let Some(Ok(read)) = self.iter.next() {
                if self.filter.as_ref().map_or(true, |predict| predict(&read)) {
                    break Some(read);
                }
            } else {
                break None;
            }
        }
        .map(|read| (read.ref_id(), read.ref_begin(), read.ref_end()));
    }
}

impl<'a, R: AlignmentReader<'a>> Iterator for DepthIter<'a, R> {
    type Item = (i32, usize, u32);
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_read.is_none() && self.heap.is_empty() {
            return None;
        }

        let ret = (self.cur_chrom, self.cur_pos, self.heap.len() as u32);

        self.cur_pos += 1;

        while let Some((tid, left, right)) = self.next_read {
            if tid != self.cur_chrom {
                if self.heap.is_empty() {
                    self.cur_chrom = tid;
                    self.cur_pos = 0;
                }
                break;
            }
            if left > self.cur_pos {
                break;
            }
            self.heap.push(Reverse(right));
            self.load_next();
        }

        while self.heap.peek().map_or(false, |x| x.0 < self.cur_pos) {
            self.heap.pop();
        }

        Some(ret)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
