/// The trait that describes a single record in the Key-Value sparse table
pub trait Record: Sized + Copy + Send + 'static {
    /// An ID used by the metadata for this record format
    const FORMAT_NAME: &'static str;
    /// The size of this record type, by default, it should be as large as the memory image
    const SIZE: usize = std::mem::size_of::<Self>();
    /// Get the range this record effectively encoded
    fn effective_range(&self) -> (u32, u32);
    /// Return a new record with the new left-hand-side
    fn limit_left(&self, new_left: u32) -> Option<Self>;
    /// Return a new record with a new right-hand-side
    fn limit_right(&self, new_right: u32) -> Option<Self>;
    /// Check if this position is in the effective range of this record
    #[inline(always)]
    fn in_range(&self, pos: u32) -> bool {
        let (a, b) = self.effective_range();
        a <= pos && pos < b
    }
    /// Get the value that record is encoding
    fn value(&self) -> i32;

    /// encode a new value to this record, if this can be done by modifying the existing record,
    /// it returns None. If this is impossible, it create a new record for the new value
    fn encode(this: Option<&mut Self>, pos: u32, value: i32) -> Option<Self>;

    /// Serialize the record into bytes
    #[inline(always)]
    fn as_bytes(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(std::mem::transmute(self), Self::SIZE) }
    }

    /// Check if this record is a valid one
    fn is_valid(&self) -> bool;
}

#[repr(packed)]
#[derive(Clone, Copy)]
pub struct RangeRecord {
    left: u32,
    size_enc: u16,
    value: i32,
}

impl Record for RangeRecord {
    const FORMAT_NAME: &'static str = "range";

    #[inline(always)]
    fn effective_range(&self) -> (u32, u32) {
        (
            self.left.to_le() - 1,
            self.left.to_le() + self.size_enc.to_le() as u32,
        )
    }
    #[inline(always)]
    fn limit_left(&self, new_left: u32) -> Option<Self> {
        if new_left >= self.effective_range().1 {
            None
        } else {
            Some(Self {
                left: (new_left + 1).max(self.left),
                size_enc: (self.effective_range().1 - new_left - 1) as u16,
                value: self.value,
            })
        }
    }
    #[inline(always)]
    fn limit_right(&self, new_right: u32) -> Option<Self> {
        if new_right <= self.effective_range().0 {
            None
        } else {
            Some(Self {
                left: self.left,
                size_enc: (new_right - self.effective_range().0 - 1) as u16,
                value: self.value,
            })
        }
    }
    #[inline(always)]
    fn value(&self) -> i32 {
        self.value.to_le()
    }

    #[inline(always)]
    fn encode(this: Option<&mut Self>, pos: u32, value: i32) -> Option<Self> {
        if let Some(this) = this {
            if this.value == value && this.left + this.size_enc as u32 == pos && this.size_enc != !0
            {
                this.size_enc += 1;
                return None;
            }
        }
        Some(Self {
            left: pos + 1,
            size_enc: 0,
            value,
        })
    }

    fn is_valid(&self) -> bool {
        self.left > 0
    }
}
