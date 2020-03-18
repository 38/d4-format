use std::str::FromStr;

#[derive(Debug)]
pub enum Nucleotide {
    A,
    T,
    C,
    G,
    N,
}

impl FromStr for Nucleotide {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, ()> {
        Ok(match s {
            "A" | "a" => Nucleotide::A,
            "T" | "t" => Nucleotide::T,
            "C" | "c" => Nucleotide::C,
            "G" | "g" => Nucleotide::G,
            "N" => Nucleotide::N,
            _ => {
                return Err(());
            }
        })
    }
}

impl From<u32> for Nucleotide {
    fn from(what: u32) -> Self {
        match what {
            1 => Nucleotide::A,
            2 => Nucleotide::C,
            4 => Nucleotide::G,
            8 => Nucleotide::T,
            _ => Nucleotide::N,
        }
    }
}

static A: Nucleotide = Nucleotide::A;
static C: Nucleotide = Nucleotide::C;
static G: Nucleotide = Nucleotide::G;
static T: Nucleotide = Nucleotide::T;
static N: Nucleotide = Nucleotide::N;

impl From<u32> for &'static Nucleotide {
    fn from(what: u32) -> Self {
        match what {
            1 => &A,
            2 => &C,
            4 => &G,
            8 => &T,
            _ => &N,
        }
    }
}
