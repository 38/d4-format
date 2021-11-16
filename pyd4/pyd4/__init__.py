"""
The Python Binding for the D4 file format
"""

from .pyd4 import D4File as D4FileImpl, D4Iter

import numpy

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

class D4File(D4FileImpl):
    def enumerate_values(self, chrom, begin, end):
        enumerate_values(self, chrom, begin, end)
    def open_all_tracks(self):
        return D4Matrix([self.open_track(track_label) for track_label in self.list_tracks()])
    def load_to_np(self, regions):
        ret = numpy.array([], dtype = "int16")
        for (name, begin, end) in regions:
            ret = numpy.append(ret, numpy.fromiter(self.value_iter(name, begin, end), dtype = "int16"))
        return ret
__all__ = [ 'D4File', 'D4Iter', 'D4Matrix' ]
