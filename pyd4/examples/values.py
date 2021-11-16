#!env python3
import pyd4
import sys

file = pyd4.D4File(sys.argv[1])
chrom = sys.argv[2]
begin = int(sys.argv[3])
end = int(sys.argv[4])

for (chrom, pos, value) in pyd4.enumerate_values(file, chrom, begin, end):
    print(chrom, pos, value)

