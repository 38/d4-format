#!env python3
import pyd4
import h5py
import numpy
import sys

file = pyd4.D4File(sys.argv[1])
out = h5py.File(sys.argv[2], 'w')

for name, _ in file.chroms():
    data = file[name]
    out.create_dataset(name, data = data)

out.close()
