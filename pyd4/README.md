# pyd4 - Python Binding for D4 File Format

This python module allows python read D4 file.

## Installation

Install through pip is recommended

```bash
pip install pyd4
```

or you can also run the `setup.py` to install:

```bash
./setup.py install
```


## Usage by Example

```python
from pyd4 import D4File

# Open a D4 File
file = D4File("test.d4")

# Print the chrom list
print(file.chroms())

# Get the mean cov for region chr1:10000000-20000000
print(file.mean([("chr1", 10000000, 20000000)]))

# Get the depth distribution hisgoram for chr1:10000000-20000000. 
# The max bucket is 1000 and the min bucket is 0
print(file.histogram([("chr1", 10000000, 20000000)], 0, 1000))

# Get a iterator over values
for i in file.value_iter("chr1", 0, 10000):
	print(i)

```

## Use PyD4 with NumPy

PyD4 can be use with NumPy effeciently. It can load depth profile as a numpy array. For example

```python
from pyd4 import D4File

# Open a D4 File
file = D4File("test.d4")

# Load chr1 as np array (this will take < than 1s)
per_base_depth = file.load_to_np("1")


print(per_base_depth.mean())
```

## Use PyD4 as a Bam Coverage Profiler

It's possible that we use PyD4 to get per-base coverage of a BAM within < 2min!

```python
import pyd4

# Create D4 file from a BAM input
d4_file = pyd4.bam_to_d4("input.bam")

chr1_per_base = d4_file.load_to_np("1")

# Print number of locus that is > 30
print((chr1_per_base > 30).sum())
```
