mod data;

use d4_framefile::{Blob, Directory};
pub use data::{DataSummary, Sum};

use std::{
    collections::HashMap,
    fmt::Debug,
    fs::File,
    io::{Read, Result, Seek},
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use crate::{ssio::D4TrackReader as StreamD4Reader, Chrom, D4TrackReader};

#[derive(PartialEq)]
pub enum DataIndexType {
    Sum,
}

#[repr(C)]
struct DataIndexHeader {
    granularity: u32,
    index_type: DataIndexType,
}

#[repr(C)]
pub struct DataIndex<T: DataSummary> {
    header: DataIndexHeader,
    pre_computed_data: [T],
}

pub struct DataIndexRef<T: DataSummary> {
    raw_data: Vec<u8>,
    offset_table: HashMap<String, (usize, usize)>,
    phantom: PhantomData<T>,
}

pub struct DataIndexQueryResult<'a, T: DataSummary> {
    chrom: &'a str,
    query_begin: u32,
    query_end: u32,
    actual_begin: u32,
    actual_end: u32,
    actual_result: T,
}

impl<'a, T: DataSummary> DataIndexQueryResult<'a, T> {
    fn get_incomplete_regions(&self) -> [(u32, u32); 2] {
        [
            (self.query_begin, self.actual_begin),
            (self.actual_end, self.query_end),
        ]
    }
    fn do_per_base_query<R: Read + Seek>(
        reader: &mut StreamD4Reader<R>,
        chr: &str,
        begin: u32,
        end: u32,
    ) -> Result<T> {
        if begin >= end {
            return Ok(T::identity());
        }
        let view = reader.get_view(chr, begin, end)?;
        Ok(T::from_data_iter(view.filter_map(|x| x.ok())))
    }
    pub fn query_size(&self) -> u32 {
        self.query_end - self.query_begin
    }
    pub fn get_result<R: Read + Seek>(&self, reader: &mut StreamD4Reader<R>) -> Result<T> {
        let [left_missing, right_missing] = self.get_incomplete_regions();
        let left_result =
            Self::do_per_base_query(reader, self.chrom, left_missing.0, left_missing.1)?;
        let right_result =
            Self::do_per_base_query(reader, self.chrom, right_missing.0, right_missing.1)?;
        Ok(left_result
            .combine(&self.actual_result)
            .combine(&right_result))
    }
}

impl<T: DataSummary> DataIndexRef<T> {
    pub fn print_index(&self)
    where
        T: Debug,
    {
        let granularity = self.header.granularity;
        let mut chroms: Vec<_> = self.offset_table.iter().collect();
        chroms.sort_unstable_by_key(|(_, (start, _))| *start);
        for (chr, (begin_idx, chrom_size)) in chroms {
            let mut begin = 0;
            for item in &self.pre_computed_data[*begin_idx..] {
                let end = (begin + granularity).min(*chrom_size as u32);
                println!("{}\t{}\t{}\t{:.5?}", chr, begin, end, item);
                begin += granularity;
                if begin as usize > *chrom_size {
                    break;
                }
            }
        }
    }
    pub fn query(
        &self,
        chr: &str,
        mut begin: u32,
        mut end: u32,
    ) -> Option<DataIndexQueryResult<'_, T>> {
        let (base_offset, chrom_size) = *self.offset_table.get(chr)?;
        begin = begin.min(chrom_size as u32);
        end = end.min(chrom_size as u32);
        let grand = self.header.granularity;
        let actual_begin = if begin % grand == 0 {
            begin
        } else {
            begin + grand - begin % grand
        };
        let actual_end: u32 = if end % grand == 0 {
            end
        } else {
            end - end % grand
        };
        let actual_begin_idx = (actual_begin / grand) as usize + base_offset;
        let actual_end_idx = (actual_end / grand) as usize + base_offset;
        let mut ret = T::identity();
        for idx in actual_begin_idx..actual_end_idx {
            ret = ret.combine(&self.pre_computed_data[idx]);
        }
        Some(DataIndexQueryResult {
            chrom: self.offset_table.get_key_value(chr)?.0.as_str(),
            query_begin: begin,
            query_end: end,
            actual_begin,
            actual_end,
            actual_result: ret,
        })
    }
}

impl<T: DataSummary> Deref for DataIndexRef<T> {
    type Target = DataIndex<T>;
    fn deref(&self) -> &Self::Target {
        DataIndex::from_raw(&self.raw_data)
    }
}
impl<T: DataSummary> DerefMut for DataIndexRef<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        DataIndex::from_raw_mut(&mut self.raw_data)
    }
}

impl<T: DataSummary> DataIndex<T> {
    fn from_raw_mut(raw_slice: &mut [u8]) -> &mut Self {
        let len =
            (raw_slice.len() - std::mem::size_of::<DataIndexHeader>()) / std::mem::size_of::<T>();
        let fat_ptr = std::ptr::slice_from_raw_parts_mut(raw_slice.as_mut_ptr(), len);
        unsafe { &mut *(fat_ptr as *mut Self) }
    }
    fn from_raw(raw_slice: &[u8]) -> &Self {
        let len =
            (raw_slice.len() - std::mem::size_of::<DataIndexHeader>()) / std::mem::size_of::<T>();
        let fat_ptr = std::ptr::slice_from_raw_parts(raw_slice.as_ptr(), len);
        unsafe { &*(fat_ptr as *const Self) }
    }
    pub(crate) fn from_blob<R: Read + Seek>(
        blob: &mut Blob<R>,
        chrom: &[Chrom],
    ) -> Result<DataIndexRef<T>> {
        let mut data_buffer = vec![0; blob.size()];
        blob.get_reader().read_exact(&mut data_buffer)?;
        let mut ret = DataIndexRef {
            raw_data: data_buffer,
            offset_table: HashMap::new(),
            phantom: PhantomData,
        };
        let mut offset = 0;
        for Chrom { name, size } in chrom {
            let chrom_index_size =
                (*size + ret.header.granularity as usize - 1) / ret.header.granularity as usize;
            ret.offset_table.insert(name.to_string(), (offset, *size));
            offset += chrom_index_size;
        }
        Ok(ret)
    }

    pub(crate) fn build<'a>(
        track_root: &'a mut Directory<File>,
        index_root: &'a mut Directory<File>,
        granularity: u32,
    ) -> Result<()> {
        let mut reader = D4TrackReader::create_reader_for_root(track_root.clone())?;
        let index_result = T::run_summary_task(&mut reader, granularity)?;
        let size_of_blob =
            index_result.len() * std::mem::size_of::<T>() + std::mem::size_of::<DataIndexHeader>();
        let mut blob = index_root.create_blob(T::INDEX_NAME, size_of_blob)?;
        let mut mapped_blob = blob.mmap_mut()?;
        let index = Self::from_raw_mut(mapped_blob.as_mut());
        index.header.granularity = granularity.to_le();
        index.header.index_type = T::INDEX_TYPE_CODE;

        for (ofs, item) in index_result.into_iter().enumerate() {
            index.pre_computed_data[ofs] = item.output.clone();
        }

        Ok(())
    }
}
