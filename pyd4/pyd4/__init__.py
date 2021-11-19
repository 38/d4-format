"""
The Python Binding for the D4 file format
"""

from .pyd4 import D4File as D4FileImpl, D4Iter, D4Builder as D4BuilderImpl, D4Writer as D4WriterImpl

import numpy
import ctypes
import subprocess
import tempfile
import atexit
import os
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

class D4Matrix:
    """
    Higher level abstraction for a multitrack D4 file
    """
    def __init__(self, tracks):
        self.tracks = tracks
    def enumerate_values(self, chrom, begin, end):
        """
        Enumerate values in the given range
        """
        return enumerate_values(self.tracks, chrom, begin, end)
class D4Writer:
    def __init__(self, writer_obj):
        self._inner = writer_obj
    def __del__(self):
        self._inner.close()
    def write_np_array(self, chr, pos, data):
        """
            Write a numpy array to a D4 file
            The data will be stored from the locus chr:pos specified
        """
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
    def enable_secondary_table_compression(self, level = 5):
        """
            Enable the secondary table compression for the d4 file to created
        """
        self.set_compression(level)
        return self
    def set_dict_bits(self, n):
        self.dict_range(0, 1<<n)
        return self
    def add_sequence(self, chr, size):
        """
            Add a new sequence to the given file
        """
        self.add_seq(chr, size)
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
    def get_writer(self):
        """
            Get the writer object
        """
        return D4Writer(self.into_writer(self.output_path))
class D4File(D4FileImpl):
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
    def enumerate_values(self, chrom, begin, end):
        """
        Enuemrate all the values in given range
        """
        return enumerate_values(self.tracks, chrom, begin, end)
    def open_all_tracks(self):
        """
        Open all the tracks that are living in this file
        """
        return D4Matrix([self.open_track(track_label) for track_label in self.list_tracks()])
    def load_to_np(self, regions):
        """
        Load regions as numpy array. 

        If the region is a list, the function will return a list of np array.

        If the region is a string, the function will load the entire chromosome

        If the region is a tuple of (chr, begin, end), The function will load data in range chr:begin-end
        """
        ret = []
        chroms = dict(self.chroms())
        single_value = False
        if type(regions) != list:
            regions = [str(regions)]
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
                name = str(region)
                begin = 0
                end = chroms[name]
            begin = max(0, begin)
            end = min(end, chroms[name])
            buf = numpy.zeros(shape=(end - begin,), dtype = numpy.int32)
            buf_ptr = buf.ctypes.data_as(ctypes.POINTER(ctypes.c_uint32))
            buf_addr = ctypes.cast(buf_ptr, ctypes.c_void_p).value
            self.load_values_to_buffer(name, begin, end, buf_addr)
            ret.append(buf)
        return ret if not single_value else ret[0]
__all__ = [ 'D4File', 'D4Iter', 'D4Matrix', 'D4Bulder']