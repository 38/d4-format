use super::record::Record;
use flate2::read::DeflateDecoder;
use std::cell::RefCell;
use std::io::{Read, Result};

/// The secondary table is represented by a list of record blocks
/// Each record blocks carries multiple records.
pub(crate) enum RecordBlock<'a, R: Record> {
    /// The record block is backed by a memory region
    Block(&'a [R]),
    /// The record block is backed by a memory region but compressed
    /// We gradually decompress the data block when it's need
    CompressedBlock {
        raw: &'a [u8],
        decompressed: RefCell<Vec<R>>,
        unused: usize,
        start: u32,
        limit: u32,
        block_count: usize,
    },
    /// The record block that carried by a owned vector of records
    OwnedBlock(Vec<R>),
    /// A block that has only one record. This is useful when a record
    /// is across the frame boundary, so that we need to assemble the record
    /// in memory
    Record(R),
}
impl<'a, R: Record> RecordBlock<'a, R> {
    #[allow(dead_code)]
    pub fn to_owned(&self) -> RecordBlock<'static, R> {
        let data = self.as_ref();
        RecordBlock::OwnedBlock(data.to_vec())
    }
    #[inline(never)]
    fn decompress(&self, mut count: isize) -> Result<()> {
        if let Self::CompressedBlock {
            raw,
            decompressed,
            unused,
            block_count,
            ..
        } = self
        {
            let mut decompressed = decompressed.borrow_mut();
            if (count < 0 && *block_count != decompressed.len())
                || (count >= 0 && decompressed.len() < unused + count as usize)
            {
                // to avoid decompress the unused blocks again and again, if we see an unused block is here
                // we always decompress the entire block
                if *unused > 0 {
                    count = -1;
                } else if count > 0 && decompressed.len() < unused + count as usize {
                    // To avoid decompression many times, we double the decompressed region
                    // each time.
                    count = (count as usize)
                        .max(decompressed.len() * 2)
                        .min(*block_count) as isize;
                }
                let mut decoder = DeflateDecoder::new(*raw);
                let bytes_to_read = (if count > 0 {
                    count as usize + unused
                } else {
                    *block_count
                }) * R::SIZE;
                let mut buffer = vec![0; bytes_to_read];
                let mut size = 0;
                while size < bytes_to_read {
                    let this_size = decoder.read(&mut buffer[size..])?;
                    size += this_size;
                    if this_size == 0 {
                        break;
                    }
                }
                let records = unsafe {
                    std::slice::from_raw_parts(
                        &buffer[0] as *const u8 as *const R,
                        size / std::mem::size_of::<R>(),
                    )
                };
                *decompressed = records.to_owned();
            }
        }
        Ok(())
    }
}
impl<'a, R: Record> AsRef<[R]> for RecordBlock<'a, R> {
    fn as_ref(&self) -> &[R] {
        self.decompress(-1).unwrap();
        match self {
            Self::Block(blk) => blk,
            Self::Record(rec) => std::slice::from_ref(rec),
            Self::CompressedBlock {
                decompressed,
                unused,
                ..
            } => unsafe {
                // The only way that decompressed data gets modified is
                // call self.decompress. However, when we call as_ref,
                // at this point, self.decompress(-1) has been called already
                // which indicates we have fully decompressed the data block
                // at this point. Thus, nothing can be changed after this
                // reference be returned.
                // Thus the data inside the decompressed RefCell is logically
                // immutable since this point.
                // So it won't volidate the borrow rule even we drop the RefCell
                // ticket.
                std::mem::transmute(&decompressed.borrow()[*unused..])
            },
            Self::OwnedBlock(blk) => blk.as_ref(),
        }
    }
}
impl<'a, R: Record> RecordBlock<'a, R> {
    pub fn is_single_record(&self) -> bool {
        matches!(self, RecordBlock::Record(_))
    }
    pub fn binary_search_by_key<KeyFunc: Fn(&R) -> K, K: Ord>(
        &self,
        key: K,
        kf: KeyFunc,
    ) -> std::result::Result<usize, usize> {
        self.decompress(1).unwrap();
        let fk = kf(&self.get(0));
        if key <= fk {
            return if key == fk { Ok(0) } else { Err(0) };
        }
        let (mut l, mut r) = (0, self.count());
        while r - l > 1 {
            let m = (l + r) / 2;
            self.decompress(m as isize + 1).unwrap();
            let mk = kf(&self.get(m));
            match &key {
                key if key < &mk => {
                    r = m;
                }
                key if key == &mk => {
                    return Ok(m);
                }
                _ => {
                    l = m;
                }
            }
        }
        if r < self.count() {
            let item = self.get(r);
            if kf(&item) == key {
                Ok(r)
            } else {
                Err(r)
            }
        } else {
            Err(r)
        }
    }
    pub fn split_by_size(
        mut self,
        count: usize,
        first_part: &mut Option<Self>,
        second_part: &mut Option<Self>,
    ) {
        if count == 0 || self.count() <= count {
            *first_part = Some(self);
            *second_part = None;
            return;
        }

        self.decompress(count as isize + 1).unwrap();

        match self {
            Self::Block(what) => {
                if self.count() < count {
                    *first_part = Some(self);
                    return;
                }
                *first_part = Some(Self::Block(unsafe { std::mem::transmute(&what[..count]) }));
                *second_part = Some(Self::Block(unsafe { std::mem::transmute(&what[count..]) }));
            }
            Self::Record(_) => *first_part = Some(self),
            Self::OwnedBlock(ref what) => {
                if what.len() < count {
                    *first_part = Some(self);
                    return;
                }
                *first_part = Some(Self::OwnedBlock(what[..count].to_owned()));
                *second_part = Some(Self::OwnedBlock(what[count..].to_owned()));
            }
            Self::CompressedBlock {
                ref decompressed,
                ref mut unused,
                ref block_count,
                ref mut start,
                ref limit,
                ..
            } => {
                if decompressed.borrow().len() <= count + *unused {
                    *first_part = Some(self);
                    return;
                }

                *first_part = Some(Self::OwnedBlock(
                    decompressed.borrow()[*unused..*unused + count].to_owned(),
                ));
                *unused += count;

                *second_part = if unused == block_count {
                    None
                } else {
                    if decompressed.borrow().len() < *unused {
                        *start = decompressed.borrow()[*unused].effective_range().0;
                    } else {
                        *start = *limit;
                    }
                    Some(self)
                }
            }
        }
    }
    pub fn range(&self) -> (u32, u32) {
        match self {
            Self::Block(what) => (
                what.first().unwrap().effective_range().0,
                what.last().unwrap().effective_range().1,
            ),
            Self::OwnedBlock(blk) => (
                blk.first().unwrap().effective_range().0,
                blk.last().unwrap().effective_range().1,
            ),
            Self::Record(rec) => rec.effective_range(),
            Self::CompressedBlock { limit, start, .. } => (*start, *limit),
        }
    }
    pub fn count(&self) -> usize {
        match self {
            Self::Block(what) => what.len(),
            Self::Record(_) => 1,
            Self::CompressedBlock {
                block_count,
                unused,
                ..
            } => *block_count - *unused,
            Self::OwnedBlock(what) => what.len(),
        }
    }
    pub fn get(&self, idx: usize) -> R {
        match self {
            Self::Block(what) => what[idx],
            Self::Record(what) => *what,
            Self::CompressedBlock {
                decompressed,
                unused,
                ..
            } => {
                self.decompress(idx as isize + 1).unwrap();
                decompressed.borrow()[idx + unused]
            }
            Self::OwnedBlock(what) => what[idx],
        }
    }
}
