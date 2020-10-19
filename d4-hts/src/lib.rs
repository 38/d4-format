pub mod alignment;

pub use alignment::*;

use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub struct DepthIter<'a, R: AlignmentReader<'a>> {
    iter: AlignmentIter<'a, R>,
    cur_chrom: i32,
    cur_pos: usize,
    heap: BinaryHeap<Reverse<usize>>,
    next_read: Option<(i32, usize, usize)>,
}

impl<'a, R: AlignmentReader<'a>> DepthIter<'a, R> {
    pub fn new(reader: R) -> Self {
        let (chrom, pos) = reader.start();
        let mut iter = reader.to_alignment_iter();
        let next_read = iter.next().map(|x| {
            (
                x.as_ref().unwrap().ref_id(),
                x.as_ref().unwrap().ref_begin(),
                x.as_ref().unwrap().ref_end(),
            )
        });
        Self {
            iter,
            next_read,
            cur_chrom: chrom as i32,
            cur_pos: pos as usize,
            heap: BinaryHeap::new(),
        }
    }
}

impl<'a, R: AlignmentReader<'a>> Iterator for DepthIter<'a, R> {
    type Item = (i32, usize, u32);
    fn next(&mut self) -> Option<Self::Item> {
        if self.next_read.is_none() && self.heap.len() == 0 {
            return None;
        }

        let ret = (self.cur_chrom, self.cur_pos, self.heap.len() as u32);

        self.cur_pos += 1;

        while let Some((tid, left, right)) = self.next_read {
            if tid != self.cur_chrom {
                if self.heap.len() == 0 {
                    self.cur_chrom = tid;
                    self.cur_pos = 0;
                }
                break;
            }
            if left > self.cur_pos {
                break;
            }
            self.heap.push(Reverse(right));
            self.next_read = self.iter.next().map(|x| {
                (
                    x.as_ref().unwrap().ref_id(),
                    x.as_ref().unwrap().ref_begin(),
                    x.as_ref().unwrap().ref_end(),
                )
            });
        }

        while self.heap.peek().map_or(false, |x| x.0 < self.cur_pos) {
            self.heap.pop();
        }

        return Some(ret);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
