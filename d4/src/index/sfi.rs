use std::{
    io::{Error, ErrorKind, Read, Result, Seek},
    {fs::File, io::Write},
};

use d4_framefile::{Directory, Stream};

use crate::{
    stab::{
        RangeRecord, Record, RecordBlockParsingState, SparseArraryMetadata,
        SECONDARY_TABLE_METADATA_NAME, SECONDARY_TABLE_NAME,
    },
    Header,
};

#[derive(Clone, Copy)]
#[repr(packed)]
struct FrameIndexEntry {
    chrom_id: u32,
    start_pos: u32,
    end_pos: u32,
    offset: u64,
    frame_size: u64,
    record_offset: u8,
    first_frame: bool,
}

#[derive(Clone, Copy)]
pub struct RecordFrameAddress {
    pub frame_offset: u64,
    pub frame_size: usize,
    pub record_offset: usize,
    pub first_frame: bool,
}

impl RecordFrameAddress {
    pub fn open_stream<T: Seek + Read>(&self, stab_root: &Directory<T>) -> Result<Stream<T>> {
        stab_root.open_stream_by_offset(self.frame_offset, self.frame_size)
    }
    fn from_frame_index_entry(entry: &FrameIndexEntry) -> Self {
        Self {
            frame_offset: entry.offset,
            frame_size: entry.frame_size as usize,
            record_offset: entry.record_offset as usize,
            first_frame: entry.first_frame,
        }
    }
}

impl FrameIndexEntry {
    fn ensure_byte_odering(&mut self) {
        self.chrom_id = self.chrom_id.to_le();
        self.start_pos = self.start_pos.to_le();
        self.end_pos = self.end_pos.to_le();
        self.offset = self.offset.to_le();
        self.frame_size = self.frame_size.to_le();
        self.record_offset = self.record_offset.to_le();
    }
}

pub struct SecondaryFrameIndex {
    header: Header,
    items: Vec<FrameIndexEntry>,
}
impl SecondaryFrameIndex {
    pub const STREAM_NAME: &'static str = "secondary_frame_index";

    pub fn print_secondary_table_index<W: Write>(&self, mut writer: W) -> Result<()> {
        let chrom_list = self.header.chrom_list();
        for item in self.items.iter() {
            let chr_name = chrom_list[item.chrom_id as usize].name.as_str();
            writeln!(
                writer,
                "{is_head}{offset:8x}({size:4x})+{rec_ofs}\t{chr_name}:{begin}-{end}",
                chr_name = chr_name,
                begin = { item.start_pos },
                end = { item.end_pos },
                offset = { item.offset },
                size = { item.frame_size },
                rec_ofs = item.record_offset,
                is_head = if item.first_frame { "H" } else { "T" },
            )?;
        }
        Ok(())
    }

    pub fn find_partial_seconary_table(
        &self,
        chr: &str,
        from: u32,
    ) -> Result<Option<RecordFrameAddress>> {
        if let Some((chr_id, _)) = self
            .header
            .chrom_list()
            .iter()
            .enumerate()
            .find(|(_, c)| c.name == chr)
        {
            let ret = match self
                .items
                .binary_search_by_key(&(chr_id as u32, from), |item| {
                    (item.chrom_id, item.start_pos)
                }) {
                Ok(idx) => RecordFrameAddress::from_frame_index_entry(&self.items[idx]),
                Err(idx) if !self.items.is_empty() => {
                    let prev_idx = if idx > 0 { idx - 1 } else { 0 };
                    if self.items[prev_idx].chrom_id == chr_id as u32 {
                        RecordFrameAddress::from_frame_index_entry(&self.items[prev_idx])
                    } else {
                        return Ok(None);
                    }
                }
                _ => {
                    return Ok(None);
                }
            };
            Ok(Some(ret))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn from_reader<R: Read>(mut reader: R, header: Header) -> Result<Self> {
        let mut size_buf = [0; std::mem::size_of::<u64>()];
        reader.read_exact(&mut size_buf)?;
        let size = u64::from_le_bytes(size_buf) as usize;
        let mut ret = SecondaryFrameIndex {
            items: Vec::with_capacity(size),
            header,
        };

        let mut buffer = vec![0; std::mem::size_of::<FrameIndexEntry>() * size];
        reader.read_exact(&mut buffer)?;
        let items = unsafe {
            std::slice::from_raw_parts_mut(buffer.as_mut_ptr() as *mut FrameIndexEntry, size)
        };
        items.iter_mut().for_each(|item| item.ensure_byte_odering());
        ret.items.extend_from_slice(items);

        Ok(ret)
    }
    pub(crate) fn write<W: Write>(&self, mut out: W) -> Result<()> {
        out.write_all(&(self.items.len() as u64).to_le_bytes())?;
        for item in self.items.iter() {
            let mut item = *item;
            item.ensure_byte_odering();
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    &item as *const _ as *const u8,
                    std::mem::size_of::<FrameIndexEntry>(),
                )
            };
            out.write_all(bytes)?;
        }
        Ok(())
    }
    pub(crate) fn get_blob_size(&self) -> usize {
        std::mem::size_of::<FrameIndexEntry>() * self.items.len() + std::mem::size_of::<usize>()
    }
    pub(crate) fn from_data_track(track_root: &Directory<File>) -> Result<Self> {
        let header = Header::read(track_root.open_stream(Header::HEADER_STREAM_NAME)?)?;
        let stab_root = track_root.map_directory(SECONDARY_TABLE_NAME)?;
        let stab_metadata = {
            let stream = stab_root
                .open_stream(SECONDARY_TABLE_METADATA_NAME)
                .unwrap();
            let mut stream_content = Vec::new();
            stream.copy_content(&mut stream_content);
            let raw_metadata = String::from_utf8_lossy(&stream_content);
            serde_json::from_str::<SparseArraryMetadata>(raw_metadata.trim_end_matches('\0'))
                .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?
        };

        let mut items = Vec::<FrameIndexEntry>::new();
        let chrom_list = header.chrom_list();

        for stream in stab_metadata.streams() {
            let chrom_id = if let Some((idx, _)) = chrom_list
                .iter()
                .enumerate()
                .find(|(_, c)| c.name == stream.chr)
            {
                idx as u32
            } else {
                return Err(Error::new(ErrorKind::Other, "No such chrom"));
            };
            let data_stream = stab_root.open_stream(&stream.id).unwrap();
            let mut frame = data_stream.get_primary_frame();
            let mut state =
                RecordBlockParsingState::<RangeRecord>::new(stab_metadata.compression());
            let mut parsing_buf = vec![];
            let mut first_frame = true;
            loop {
                let offset = unsafe { frame.offset_from(stab_root.get_base_addr()) };
                assert!(offset >= 0);
                let offset = offset as u64;

                parsing_buf.clear();
                let rec_offset = state.first_record_offset();
                state.parse_frame(frame.as_ref(), &mut parsing_buf);
                let frame_size = std::mem::size_of_val(frame);

                let has_next_frame = if let Some(next_frame) = frame.next_frame() {
                    frame = next_frame;
                    true
                } else {
                    false
                };

                for (id, block) in parsing_buf.iter().enumerate() {
                    let mut recs = block.as_ref();
                    if !has_next_frame {
                        while let Some(last) = recs.last() {
                            if !last.is_valid() {
                                recs = &recs[..recs.len() - 1];
                            } else {
                                break;
                            }
                        }
                    }
                    if !recs.is_empty() {
                        if id == 0 && block.is_single_record() {
                            if let Some(last_entry) = items.last_mut() {
                                last_entry.end_pos = recs[0].effective_range().1;
                            }
                        } else {
                            items.push(FrameIndexEntry {
                                chrom_id,
                                start_pos: recs[0].effective_range().0,
                                end_pos: recs[recs.len() - 1].effective_range().1,
                                offset,
                                frame_size: frame_size as u64,
                                record_offset: rec_offset as u8,
                                first_frame,
                            });
                        }
                    } else {
                        break;
                    }
                }

                if !has_next_frame {
                    break;
                }
                first_frame = false;
            }
        }

        Ok(Self { items, header })
    }
}
