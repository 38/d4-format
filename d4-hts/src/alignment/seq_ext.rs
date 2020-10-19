use super::Alignment;
use super::Nucleotide;

use std::ops::Index;

pub struct Sequence<'a> {
    alignment: &'a Alignment<'a>,
}

impl<'a> Sequence<'a> {
    pub fn len(&self) -> usize {
        self.alignment.seq_len() as usize
    }
    fn read_numeric(&self, offset: usize) -> u32 {
        let hts_obj = self.alignment.hts_obj();
        let seq = unsafe {
            hts_obj.data.offset(
                hts_obj.core.n_cigar as isize * 4
                    + (hts_obj.core.l_qname as isize)
                    + offset as isize / 2,
            )
        };
        let numeric: u32 = if offset % 2 == 0 {
            (unsafe { *seq } as u32) >> 4
        } else {
            (unsafe { *seq } as u32) & 0xf
        };
        return numeric;
    }
}

impl<'a> Index<usize> for Sequence<'a> {
    type Output = Nucleotide;
    fn index(&self, offset: usize) -> &Nucleotide {
        self.read_numeric(offset).into()
    }
}

impl<'a> IntoIterator for Sequence<'a> {
    type Item = Nucleotide;
    type IntoIter = SequenceIter<'a>;
    fn into_iter(self) -> SequenceIter<'a> {
        SequenceIter { seq: self, ofs: 0 }
    }
}

pub struct SequenceIter<'a> {
    seq: Sequence<'a>,
    ofs: usize,
}

impl<'a> Iterator for SequenceIter<'a> {
    type Item = Nucleotide;
    fn next(&mut self) -> Option<Nucleotide> {
        if self.ofs < self.seq.len() {
            let ret = self.seq.read_numeric(self.ofs).into();
            self.ofs += 1;
            return Some(ret);
        }
        None
    }
}

impl<'a> Alignment<'a> {
    pub fn sequence(&self) -> Sequence {
        Sequence { alignment: self }
    }
}
