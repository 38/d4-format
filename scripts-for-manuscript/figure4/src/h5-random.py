import h5py
import sys

file = h5py.File(sys.argv[1], "r")

#for chr in file.keys():
#    data = file[chr][:]
#    print(chr, data.mean())
for line in open(sys.argv[2], "r"):
    line = line.split("\t");
    c,b,e=line[0], int(line[1]), int(line[2])
    print(c,b,e, file[c][b:e].mean())

