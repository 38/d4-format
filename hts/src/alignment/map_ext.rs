use super::{cigar_ext::Cigar, cigar_ext::CigarIter, seq_ext::SequenceIter, Alignment, Nucleotide};

pub struct MapIter<'a> {
    ref_pos: usize,
    cur_cigar: Option<Cigar>,
    seq_it: SequenceIter<'a>,
    cigar_it: CigarIter<'a>,
}

impl<'a> Alignment<'a> {
    pub fn map_iter<'b>(&'b self) -> MapIter<'b> {
        let mut ret = MapIter {
            ref_pos: self.ref_begin(),
            seq_it: self.sequence().into_iter(),
            cigar_it: self.cigar(),
            cur_cigar: None,
        };
        ret.cur_cigar = ret.cigar_it.next();
        return ret;
    }
}

#[derive(Debug)]
pub struct Mapping {
    ref_base: Option<usize>,
    seq_base: Option<Nucleotide>,
}

impl<'a> Iterator for MapIter<'a> {
    type Item = Mapping;
    fn next(&mut self) -> Option<Mapping> {
        let mut ret = None;
        if let Some(ref mut cur_cigar) = self.cur_cigar {
            let mut map = Mapping {
                ref_base: None,
                seq_base: None,
            };

            if cur_cigar.in_reference() {
                map.ref_base = Some(self.ref_pos);
                self.ref_pos += 1;
            }

            if cur_cigar.in_alignment() {
                map.seq_base = self.seq_it.next();
            }

            cur_cigar.len -= 1;
            ret = Some(map);
        }

        if ret.is_some() {
            if self.cur_cigar.as_ref().unwrap().len == 0 {
                self.cur_cigar = self.cigar_it.next();
            }
        }

        ret
    }
}
