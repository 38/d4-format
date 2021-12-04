#!env python3
import pyd4
import sys
bin_size = 10000
file = pyd4.D4File(sys.argv[1])

for name, _ in file.chroms():
    print(file[name].resample(bin_size = bin_size))