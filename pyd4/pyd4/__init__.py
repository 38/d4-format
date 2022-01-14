"""
The Python Binding for the D4 file format.
"""

from .pyd4 import D4File as D4FileImpl, D4Iter, D4Builder as D4BuilderImpl, D4Writer as D4WriterImpl, D4Merger as D4MergerImpl

import numpy
import ctypes
import subprocess
import tempfile
import atexit
import os
import math
from pathlib import Path

def bam_to_d4(bam_file, output = None, compression = False, encode_dict = "auto", read_flags = 0xffff, reference_genome = None):
    """
    Create a coverage profile from a given BAM/CRAM file and return the opened D4 file.
    This function provide a fast way to profile per-base coverage and load it as numpy array. 
    The following code will compute the depth for hg002 and load the per-base converage as 
    numpy array. (Typically this takes < 2 min)

        chr1_coverage = bam_to_d4("hg002.bam").load_to_np("1")
    
    If the output parameter is given, the D4 file will be placed to the path 'output' described.
    """
    if output == None:
        fp = tempfile.NamedTemporaryFile(delete = False, suffix = ".d4")
        output = fp.name
        def remove_temp_file():
            os.remove(output)
        atexit.register(remove_temp_file)
    cmd_line = ["d4tools", "create", bam_file, output]
    if compression:
        cmd_line.append("-z")
    if encode_dict != "auto":
        cmd_line.append("--dict-range=%s"%encode_dict)
    else:
        cmd_line.append("--dict-auto")
    if read_flags != 0xffff:
        cmd_line.append("--bam-flag=%s"%read_flags)
    if reference_genome != None:
        cmd_line.append("--ref=" + reference_genome)
    subprocess.run(cmd_line)
    return D4File(output)

def enumerate_values(inf, chrom, begin, end):
    """
    A helper function that can enumerate all the values in given range.
    For example print values in 1:0-10000

        for pos,val in pyd4.enumerate_values(input, "1", 0, 10000):
            print(pos, val)
    
    """
    if inf.__class__ == list:
        def gen():
            iters = [x.value_iter(chrom, begin, end) for x in inf]
            for pos in range(begin, end):
                yield (chrom, pos, [f.__next__() for f in iters])
        return gen()
    return map(lambda p: (chrom, p[0], p[1]), zip(range(begin, end), inf.value_iter(chrom, begin, end)))

def open_all_tracks(fp):
    """
    Open all the tracks that are living in given file
    """
    f = D4File(fp)
    return [f.open_track(track_label) for track_label in f.list_tracks()]

class D4Merger(D4MergerImpl):
    """
    The helper class to make multi-track D4 files.
    This class enables merge multiple single track D4 file into one multi-track file
    """
    def __del__(self):
        self.merge()
    def add_track(self, path):
        tag = str(Path(path).stem)
        self.add_tagged_track(tag, path)
        return self
    def add_tagged_track(self, tag, path):
        super().add_tagged_track(tag, path)
        return self

class D4Matrix:
    """
    Higher level abstraction for a multitrack D4 file
    """
    def __init__(self, tracks, track_names = None):
        self.tracks = tracks
        self.track_names = track_names
    def enumerate_values(self, chrom, begin, end):
        """
        Enumerate values in the given range
        """
        return enumerate_values(self.tracks, chrom, begin, end)
    def __getitem__(self, key):
        data = [track[key] for track in self.tracks]
        return numpy.stack(data)
    def resample(self, regions, method = "mean", bin_size = 1000, allow_bin_size_adjustment = True):
        data = [track.resample(regions, method, bin_size, allow_bin_size_adjustment) for track in self.tracks]
        ret = []
        for idx in range(len(data[0])):
            region_data = [track_data[idx] for track_data in data]
            ret.append(numpy.stack(region_data))
        return ret
class D4Writer:
    def __init__(self, writer_obj):
        self._inner = writer_obj
    def __del__(self):
        if self._inner:
            self._inner.close()
    def close(self):
        """
        Manually close the D4 writer. Unless the D4 writer is closed, the output file
        may be incompleted and unable to read correctly. 

        This will be automatically called when the writer gets deleted. 
        You can also call this function explicitly so that the file will be complete right
        after this invocation.
        """
        if self._inner != None:
            self._inner.close()
        self._inner = None
    def write_np_array(self, chr, pos, data):
        """
            Write a numpy array to a D4 file
            The data will be stored from the locus chr:pos specified
        """
        if self._inner == None:
            raise RuntimeError("Unable to write a closed D4 file")
        if len(data.shape) != 1:
            raise RuntimeError("Invalid input shape")
        if data.dtype != "int32":
            data = data.astype("int32")
        data_ptr = data.ctypes.data_as(ctypes.POINTER(ctypes.c_int32))
        data_addr = ctypes.cast(data_ptr, ctypes.c_void_p).value
        self._inner.write(chr, pos, data_addr, data.shape[0])
class D4Builder(D4BuilderImpl):
    """
    The helper class to build a new D4 file
    """
    def __init__(self, output_path):
        self.output_path = output_path
        self.index = ""
    def enable_secondary_table_compression(self, level = 5):
        """
        Enable the secondary table compression for the d4 file to created
        """
        self.set_compression(level)
        return self
    def set_dict_bits(self, n):
        """
        Set how many bits we want to use for the primary table.
        This function will encode the value range starting from 0.
        """
        self.dict_range(0, 1<<n)
        return self
    def add_sequence(self, chr, size):
        """
        Add a new sequence/chromosome to the given file
        """
        self.add_seq(chr, size)
        return self
    def add_chrom(self, chr, size):
        """
        Add a new sequence/chromosomes to given file
        """
        self.add_sequence(chr, size)
        return self
    def add_chroms(self, chroms):
        """
            Add a list of chromosomes
        """
        for (chr, size) in chroms:
            self.add_chrom(chr, size)
        return self
    def for_bit_array(self):
        """
        Make the D4 file optimized for a boolean array
        """
        self.set_compression(-1)
        self.dict_range(0, 2)
        return self
    def for_sparse_data(self):
        """
        Make the D4 file optimized for sparse data
        """
        self.set_compression(5)
        self.dict_range(0, 1)
        return self
    def dup_seqs(self, input):
        super().dup_seqs(input)
        return self
    def get_writer(self):
        """
        Get the writer object
        """
        return D4Writer(self.into_writer(self.output_path, self.index))
    def generate_index(self, flavor = "sum"):
        """
        Make the output file indexed
        """
        self.index = flavor
        return self

class Histogram:
    """
    Represents a hisgoram. 
    """
    def __init__(self, raw):
        values, below, above = raw
        self.below = below
        self.above = above
        values.sort()
        self.first_value = values[0][0]
        self.prefix_sum = [self.below]
        current_value = self.first_value
        for v,c in values:
            while current_value < v - 1:
                current_value += 1
                self.prefix_sum.append(self.prefix_sum[-1])
            current_value += 1
            self.prefix_sum.append(self.prefix_sum[-1] + c)
    def value_count(self, value):
        """
        Count the number of value
        """  
        if value < self.first_value or self.first_value + len(self.prefix_sum) - 1 < value:
            return 0
        idx = int(value - self.first_value + 1)
        return self.prefix_sum[idx] - self.prefix_sum[idx - 1]
    def total_count(self):
        """
        Total number of data points
        """
        return self.prefix_sum[-1] + self.above
    def value_percentage(self, value):
       """
       Percentage of the value
       """
       return self.value_count(value) / float(self.total_count())
    def percentile_below(self, value):
        """
        Count the number of value
        """  
        if value < self.first_value or self.first_value + len(self.prefix_sum) - 1 < value:
            return 0
        idx = int(value - self.first_value + 1)
        return self.prefix_sum[idx] / self.total_count()
    def mean(self):
        """
        Get the mean depth from this histogram
        """
        current_value = self.first_value
        current_sum = self.prefix_sum[0]
        total = 0
        for value in self.prefix_sum[1:]:
            current_count = value - current_sum
            total += current_count * current_value
            current_value += 1
            current_sum = value
        return total / self.total_count()
    def percentile(self, nth):
        """
        Get the n-th percentile value from the histogram
        """
        total_count = self.total_count()
        value = self.first_value
        for count in self.prefix_sum[1:]: 
            print(count * 100 / total_count, value)
            if count * 100.0 / total_count > nth:
                return value
            value += 1
        return 0
    def median(self):
        """
        Compute the median value of the histogram
        """
        return self.percentile(50)
    def std(self):
        """
        Compute the standard deviation of this histogram
        """
        current_value = self.first_value
        current_sum = self.prefix_sum[0]
        sum = 0
        square_sum = 0
        for value in self.prefix_sum[1:]:
            current_count = value - current_sum
            sum += current_count * current_value
            square_sum += current_count * current_value * current_value
            current_value += 1
            current_sum = value
        ex = sum / self.total_count()
        esx = square_sum / self.total_count()
        return math.sqrt(esx - ex * ex)
class D4File(D4FileImpl):
    """
        The python wrapper for a D4 file reader. 

        'mean', 'median', 'percentile' method supports various 'regions' parameter:
        
        # Single chromosome, this will return a single value
        self.mean("chr1") 
        # List of chromosomes, this will return a list of values
        self.mean(["chr1", "chr2"])
        # Region specification as "chr:begin-end" or "chr:begin-"
        self.mean("chr1:0-10000")
        # List of region specification
        self.mean(["chr1:1000-", "chr2:0-1000"])
        # A tuple of (chr, begin, end) or (chr, begin)
        self.mean(("chr1", 0, 10000))
        # A list of tuple
        self.mean([("chr1", 0, 10000)])

    """
    def create_on_same_genome(self, output, seqs = None):
        """
            Create a new D4 file which would use the same reference genome.

            Use 'seqs' parameter to selectively choose which chromosome to select
        """
        ret = D4Builder(output)
        if seqs != None:
            this_seqs = dict(self.chroms())
            for seq in seqs:
                if seq in this_seqs:
                    ret.add_seq(seq, this_seqs[seq])
        ret.dup_dict(self)
        return ret
    def percentile(self, regions, nth):
        """
        Return the percentile value in the given regions.
        """
        def collect_region(name, begin, end): 
            return (name, begin, end)
        region_spec = self._for_each_region(regions, collect_region)
        histo_result = super().histogram(region_spec, 0, 1000)
        ret = []
        for result, (chr, begin, end) in zip(histo_result, region_spec):
            ret.append(self._percentile_impl(result, begin, end, nth))
        return ret
    def _percentile_impl(self, result, chrom, begin = 0, end = None, nth = 50):
        if end == None:
            chroms = dict(self.chroms())
            end = chroms[chrom]
        hist, below, above = super().histogram([(chrom, begin, end)], 0, 65536)[0]
        total = end - begin
        if nth < below * 100.0 / total or \
            100.0 - above * 100.0 / total < nth:
            data = self[(chrom, begin, end)]
            low,high = data.min(),data.max() + 1
            while high - low > 1:
                mid = (high + low) // 2
                p = (data < mid).sum() * 100.0 / total
                if p < nth:
                    low = mid
                else: 
                    high = mid
            return low
        acc = below
        for value,count in hist:
            if (acc + count) * 100.0 / total > nth:
                return value
            acc += count
    def enumerate_values(self, chrom, begin, end):
        """
        Enuemrate all the values in given range
        """
        return enumerate_values(self.tracks, chrom, begin, end)
    def open_all_tracks(self):
        """
        Open all the tracks that are living in this file
        """
        tracks = self.list_tracks()
        return D4Matrix(
            [D4File(self.get_track_specifier(track_label)) for track_label in tracks],
            track_names = tracks
        )
    def chrom_names(self):
        """
        Return a list of chromosome names
        """
        return list(map(lambda x: x[0], self.chroms()))
    def histogram(self, regions, min=0, max=1024):
        """
        Returns the value histogram for given regions
        """
        is_list = type(regions) == list
        regions = self._for_each_region(regions, lambda name, begin, end: (name, begin, end), False)
        ret = super().histogram(regions, min, max)
        if not is_list:
            return Histogram(ret[0])
        return list(map(Histogram, ret))
    def median(self, regions):
        """
        return the median value for the given regions
        """
        return self.percentile(regions, nth = 50)
    def mean(self, regions):
        """
        Compute the mean depth of the given region. 
        """ 
        is_list = type(regions) == list
        regions = self._for_each_region(regions, lambda name, begin, end: (name, begin, end), False)
        ret = super().mean(regions)
        if not is_list:
            return ret[0]
        return ret
    def _parse_region(self, key):
        chroms = dict(self.chroms())
        splitted = key.split(":",1)
        chr = splitted[0]
        if len(splitted) == 1:
            return (chr, 0, chroms[chr])
        begin, end = splitted[1].split("-")
        if begin == "":
            begin = "0"
        if end == "":
            return (chr, int(begin), chroms[chr])
        else:
            return (chr, int(begin), int(end))
    def __getitem__(self, key):
        if type(key) == str:
            key = self._parse_region(key)
        if type(key) == tuple:
            return self.load_to_np(key)
        else:
            raise ValueError("Unspported region specification")
    def _for_each_region(self, regions, func, unpack_single_value = True):
        ret = []
        chroms = dict(self.chroms())
        single_value = False
        if type(regions) != list:
            regions = [regions]
            single_value = True
        for region in regions:
            if type(region) == tuple:
                if len(region) == 2:
                    begin = 0
                    end = region[1]
                    name = region[0]
                else:
                    name = region[0]
                    begin = region[1]
                    end = region[2]
            else:
                name, begin, end = self._parse_region(region)
            begin = max(0, begin)
            end = min(end, chroms[name])
            ret.append(func(name, begin, end))
        if unpack_single_value:
            return ret if not single_value else ret[0]
        return ret
    def resample(self, regions, method = "mean", bin_size = 1000, allow_bin_size_adjustment = True):
        """
        Re-sample the given region and return the value as an numpy array
        """
        unpack = not type(regions) == list
        if self.is_remote_file() and allow_bin_size_adjustment:
            if bin_size < 65536:
                bin_size = 65536
            else:
                bin_size -= bin_size % 65536
        def split_region(chr, begin, end):
            ret = []
            while begin < end:
                bin_end = min(begin + bin_size, end)
                ret.append((chr, begin, bin_end))
                begin = bin_end
            return ret
        splitted = self._for_each_region(regions, split_region, False)
        size = []
        tasks = []
        idx = 0
        for part in splitted:
            size.append(len(part))
            tasks += part
            idx += 1
        if method == "mean":
            values = self.mean(tasks)
        elif method == "median":
            values = self.median(tasks)
        else:
            raise TypeError("Unsupported resample method")
        ret = [numpy.zeros(shape = (size[i])) for i in range(0, idx)]
        idx = 0
        ofs = 0 
        for val in values:
            if ofs >= size[idx]:
                ofs = 0
                idx += 1
            ret[idx][ofs] = val
            ofs += 1
        return (ret[0] if unpack and len(ret) == 1 else ret, bin_size)
    def load_to_np(self, regions):
        """
        Load regions as numpy array. It's similar to the __getitem__ operator.
        """
        def load_to_np_impl(name, begin, end):
            buf = numpy.zeros(shape=(end - begin,), dtype = numpy.int32)
            buf_ptr = buf.ctypes.data_as(ctypes.POINTER(ctypes.c_uint32))
            buf_addr = ctypes.cast(buf_ptr, ctypes.c_void_p).value
            self.load_values_to_buffer(name, begin, end, buf_addr)
            return buf
        return self._for_each_region(regions, load_to_np_impl)
__all__ = [ 'D4File', 'D4Iter', 'D4Matrix', 'D4Builder']