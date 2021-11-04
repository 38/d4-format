use std::io::{Read, Result, Seek};

use d4_framefile::{Directory, Stream};

use crate::{
    index::RecordFrameAddress,
    stab::{
        CompressionMethod, Record, RecordBlockParsingState, SparseArraryMetadata,
        SECONDARY_TABLE_METADATA_NAME,
    },
    Chrom,
};

#[derive(Clone)]
enum DataStreamAddr {
    Full(String),
    Partial(RecordFrameAddress),
}

pub(super) struct SecondaryTableRef<R: Read + Seek> {
    pub chrom_id: usize,
    pub begin: u32,
    pub end: u32,
    pub comp: CompressionMethod,
    addr: DataStreamAddr,
    pub root: Directory<R>,
}

impl<R: Read + Seek> SecondaryTableRef<R> {
    pub fn new_partial(
        st_root: Directory<R>,
        chrom_id: usize,
        begin: u32,
        end: u32,
        addr: RecordFrameAddress,
        compression: CompressionMethod,
    ) -> SecondaryTableRef<R> {
        SecondaryTableRef {
            chrom_id,
            begin,
            end,
            comp: compression,
            addr: DataStreamAddr::Partial(addr),
            root: st_root,
        }
    }
    pub fn create_stream_index(
        sec_tab_root: Directory<R>,
        chrom_list: &[Chrom],
    ) -> Result<Vec<SecondaryTableRef<R>>> {
        let metadata: SparseArraryMetadata = {
            let mut stream = sec_tab_root.open_stream(SECONDARY_TABLE_METADATA_NAME)?;
            let mut buf = String::new();
            stream.read_to_string(&mut buf)?;
            let stripped = buf.trim_end_matches('\0');
            serde_json::from_str(stripped)?
        };
        let mut ret = vec![];
        for stream in metadata.streams() {
            if let Some((chrom_id, _)) = chrom_list
                .iter()
                .enumerate()
                .find(|(_, c)| c.name == stream.chr)
            {
                let stream_name = stream.id.as_str();
                let stream_ref = SecondaryTableRef {
                    chrom_id,
                    begin: stream.range.0,
                    end: stream.range.1,
                    addr: DataStreamAddr::Full(stream_name.to_string()),
                    comp: metadata.compression(),
                    root: sec_tab_root.clone(),
                };
                ret.push(stream_ref);
            }
        }
        Ok(ret)
    }
    pub fn open_stream(&self) -> Result<Stream<R>> {
        match &self.addr {
            DataStreamAddr::Full(name) => self.root.open_stream(name),
            DataStreamAddr::Partial(addr) => addr.open_stream(&self.root),
        }
    }
    pub fn get_frame_parsing_state<Rec: Record>(&self) -> RecordBlockParsingState<Rec> {
        match &self.addr {
            DataStreamAddr::Partial(addr) => RecordBlockParsingState::new(self.comp)
                .set_is_first_frame(addr.first_frame)
                .set_skip_bytes(addr.record_offset),
            _ => RecordBlockParsingState::new(self.comp),
        }
    }
}

impl<R: Read + Seek> Clone for SecondaryTableRef<R> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            addr: self.addr.clone(),
            ..*self
        }
    }
}
