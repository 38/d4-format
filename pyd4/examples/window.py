#!env python3
import pyd4
import sys
bin_size = 10000
file = pyd4.D4File(sys.argv[1])

regions = []

for name, size in file.chroms():
    begin = 0
    while begin < size:
        end = min(begin + bin_size, size)
        regions.append((name, begin, end))
        begin = end

for (mean, r) in zip(file.mean(regions), regions):
    print(r[0], r[1], r[2], mean)
