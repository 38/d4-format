import h5py
import sys

file = h5py.File(sys.argv[1], "r")

for chr in file.keys():
    data = file[chr][:]
    print(chr, data.mean())

