use super::error::AlignmentError;
use super::htslib::*;
use super::BamFile;

pub struct Alignment<'a> {
    data_obj: *mut bam1_t,
    file: &'a BamFile,
}

impl<'a> std::fmt::Debug for Alignment<'a> {
    fn fmt(&self, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(fmt, "<Alignment>")
    }
}

impl<'a> Alignment<'a> {
    pub(super) fn new(data_obj: *mut bam1_t, file: &'a BamFile) -> Self {
        Self { data_obj, file }
    }

    pub(super) fn hts_obj(&self) -> &bam1_t {
        unsafe { &*self.data_obj }
    }
}

impl<'a> Drop for Alignment<'a> {
    fn drop(&mut self) {
        self.file.free_inner_obj(self.data_obj);
    }
}

pub trait AlignmentReader<'a> {
    fn start(&self) -> (u32, u32);
    fn get_file(&self) -> &'a BamFile;
    fn next(&self, buf: *mut bam1_t) -> Result<Option<Alignment<'a>>, AlignmentError>;

    fn into_alignment_iter(self) -> AlignmentIter<'a, Self>
    where
        Self: Sized,
    {
        AlignmentIter {
            reader: self,
            p: std::marker::PhantomData,
        }
    }
}

pub struct AlignmentIter<'a, R: AlignmentReader<'a>> {
    reader: R,
    p: std::marker::PhantomData<&'a i32>,
}

impl<'a, R: AlignmentReader<'a>> Iterator for AlignmentIter<'a, R> {
    type Item = Result<Alignment<'a>, AlignmentError>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Ok(buf) = self.reader.get_file().alloc_inner_obj() {
            if buf.is_null() {
                return Some(Err((-1).into()));
            }

            let rc = self.reader.next(buf);

            if let Ok(Some(ref _t)) = rc {
            } else {
                self.reader.get_file().free_inner_obj(buf);
            }

            return match rc {
                Ok(Some(otherwise)) => Some(Ok(otherwise)),
                Ok(None) => None,
                Err(AlignmentError::HtsError(-1)) => None,
                Err(whatever) => Some(Err(whatever)),
            };
        }

        Some(Err((-1).into()))
    }
}
