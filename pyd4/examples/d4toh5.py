#!env python3
import pyd4
import h5py
import numpy
import sys

file = pyd4.D4File(sys.argv[1])
out = h5py.File(sys.argv[2], 'w')

for name, size in file.chroms():
    data = numpy.fromiter(file.value_iter(name, 0, size), dtype="int16")
    out.create_dataset(name, data = data)

out.close()
