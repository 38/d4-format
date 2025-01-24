use super::Alignment;
impl<'a> Alignment<'a> {
    pub fn flag(&self) -> u16 {
        self.hts_obj().core.flag
    }

    pub fn seq_len(&self) -> usize {
        self.hts_obj().core.l_qseq as usize
    }

    pub fn map_qual(&self) -> u8 {
        self.hts_obj().core.qual
    }

    pub fn ref_begin(&self) -> usize {
        self.hts_obj().core.pos as usize
    }

    pub fn ref_len(&self) -> usize {
        self.cigar()
            .filter_map(|x| {
                if x.in_reference() {
                    Some(x.len as usize)
                } else {
                    None
                }
            })
            .sum()
    }

    pub fn ref_end(&self) -> usize {
        // d4 uses 0-based closed interval end coordinate.
        self.ref_begin() + self.ref_len() - 1
    }

    pub fn ref_id(&self) -> i32 {
        self.hts_obj().core.tid
    }
}
