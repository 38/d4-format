use super::htslib::*;
use super::Alignment;

#[derive(Debug)]
pub enum CigarOps {
    Match,
    Insert,
    Delete,
    Skip,
    Soft,
    Hard,
    Pad,
    Equal,
    Diff,
    Back,
}

#[derive(Debug)]
pub struct Cigar {
    pub op: CigarOps,
    pub len: u32,
}

impl std::fmt::Display for Cigar {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}{}",
            self.len,
            match self.op {
                CigarOps::Match => "M",
                CigarOps::Insert => "I",
                CigarOps::Delete => "D",
                CigarOps::Skip => "N",
                CigarOps::Soft => "S",
                CigarOps::Hard => "H",
                CigarOps::Pad => "P",
                CigarOps::Equal => "=",
                CigarOps::Diff => "X",
                CigarOps::Back => "B",
            },
        )
    }
}

impl Cigar {
    fn new(ops: CigarOps, len: u32) -> Cigar {
        Cigar { op: ops, len: len }
    }
    fn from_alignment(al: &Alignment, idx: usize) -> Option<Cigar> {
        if idx >= (al.hts_obj().core.n_cigar as usize) {
            return None;
        }
        let cigar_num: u32 = unsafe {
            *(al.hts_obj()
                .data
                .offset(al.hts_obj().core.l_qname as isize + (idx * 4) as isize)
                as *const u32)
        };

        let cigar_op = cigar_num & BAM_CIGAR_MASK;
        let cigar_len = cigar_num >> BAM_CIGAR_SHIFT;

        match cigar_op {
            BAM_CMATCH => Some(Cigar::new(CigarOps::Match, cigar_len)),
            BAM_CINS => Some(Cigar::new(CigarOps::Insert, cigar_len)),
            BAM_CDEL => Some(Cigar::new(CigarOps::Delete, cigar_len)),
            BAM_CREF_SKIP => Some(Cigar::new(CigarOps::Skip, cigar_len)),
            BAM_CSOFT_CLIP => Some(Cigar::new(CigarOps::Soft, cigar_len)),
            BAM_CHARD_CLIP => Some(Cigar::new(CigarOps::Hard, cigar_len)),
            BAM_CPAD => Some(Cigar::new(CigarOps::Pad, cigar_len)),
            BAM_CEQUAL => Some(Cigar::new(CigarOps::Equal, cigar_len)),
            BAM_CDIFF => Some(Cigar::new(CigarOps::Diff, cigar_len)),
            BAM_CBACK => Some(Cigar::new(CigarOps::Back, cigar_len)),
            _ => None,
        }
    }

    pub fn in_alignment(&self) -> bool {
        match self.op {
            CigarOps::Match
            | CigarOps::Insert
            | CigarOps::Soft
            | CigarOps::Equal
            | CigarOps::Diff => true,
            _ => false,
        }
    }

    pub fn in_reference(&self) -> bool {
        match self.op {
            CigarOps::Match
            | CigarOps::Delete
            | CigarOps::Skip
            | CigarOps::Equal
            | CigarOps::Diff => true,
            _ => false,
        }
    }
}

pub struct CigarIter<'a> {
    alignment: &'a Alignment<'a>,
    offset: usize,
}

impl<'a> Iterator for CigarIter<'a> {
    type Item = Cigar;
    fn next(&mut self) -> Option<Cigar> {
        if let Some(ret) = Cigar::from_alignment(self.alignment, self.offset) {
            self.offset += 1;
            return Some(ret);
        }
        None
    }
}

impl<'a> Alignment<'a> {
    pub fn cigar(&self) -> CigarIter {
        CigarIter {
            alignment: self,
            offset: 0,
        }
    }
}
