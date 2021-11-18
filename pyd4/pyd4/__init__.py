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
    if inf.__class__ == list:
        def gen():
            iters = [x.value_iter(chrom, begin, end) for x in inf]
            for pos in range(begin, end):
                yield (chrom, pos, [f.__next__() for f in iters])
        return gen()
    return map(lambda p: (chrom, p[0], p[1]), zip(range(begin, end), inf.value_iter(chrom, begin, end)))

def open_all_tracks(fp):
    f = D4File(fp)
    return [f.open_track(track_label) for track_label in f.list_tracks()]

class D4Matrix:
    def __init__(self, tracks):
        self.tracks = tracks
    def enumerate_values(self, chrom, begin, end):
        return enumerate_values(self.tracks, chrom, begin, end)
class D4Writer:
    def __init__(self, writer_obj):
        self._inner = writer_obj
    def __del__(self):
        self._inner.close()
    def write_np_array(self, chr, pos, data):
        if len(data.shape) != 1:
            raise RuntimeError("Invalid input shape")
        if data.dtype != "int32":
            data = data.astype("int32")
        data_ptr = data.ctypes.data_as(ctypes.POINTER(ctypes.c_int32))
        data_addr = ctypes.cast(data_ptr, ctypes.c_void_p).value
        self._inner.write(chr, pos, data_addr, data.shape[0])
class D4Builder(D4BuilderImpl):
    def __init__(self, output_path):
        self.output_path = output_path
    """
    Enable the secondary table compression for the d4 file to created
    """
    def enable_secondary_table_compression(self, level = 5):
        self.set_compression(level)
        return self
    def add_sequence(self, chr, size):
        self.add_seq(chr, size)
        return self
    """
    Make the D4 file optimized for a boolean array
    """
    def for_bit_array(self):
        self.set_compression(-1)
        self.dict_range(0, 2)
        return self
    """
    Make the D4 file optimized for sparse data
    """
    def for_sparse_data(self):
        self.set_compression(5)
        self.dict_range(0, 1)
        return self
    def get_writer(self):
        return D4Writer(self.into_writer(self.output_path))
class D4File(D4FileImpl):
    def create_on_same_genome(self, output, seqs = None):
        ret = D4Builder(output)
        if seqs != None:
            this_seqs = dict(self.chroms())
            for seq in seqs:
                if seq in this_seqs:
                    ret.add_seq(seq, this_seqs[seq])
        ret.dup_dict(self)
        return ret
    def open_all_tracks(self):
        return D4Matrix([self.open_track(track_label) for track_label in self.list_tracks()])
    def load_to_np(self, regions):
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
__all__ = [ 'D4File', 'D4Iter', 'D4Matrix' ]