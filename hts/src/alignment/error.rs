use std::error::Error;
use std::fmt::{Display, Formatter, Result};

#[derive(Debug)]
pub enum AlignmentError {
    HtsError(i32),
    BadPosition,
}

impl Display for AlignmentError {
    fn fmt(&self, formatter: &mut Formatter) -> Result {
        match self {
            AlignmentError::HtsError(code) => write!(formatter, "HtsError({})", code),
            AlignmentError::BadPosition => write!(formatter, "BadPosition"),
        }
    }
}

impl Error for AlignmentError {}

impl From<i32> for AlignmentError {
    fn from(code: i32) -> AlignmentError {
        AlignmentError::HtsError(code)
    }
}
