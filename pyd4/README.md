# pyd4 - Python Binding for D4 File Format

This python module allows python read and write D4 file. It provides API for Python user effeicnet way to handle genomic quantatitive data, provides effecient routines to summarize, load and dump data from and to D4 format.

Also it provides a very effecient way to profile the covarage from a BAM file and load it to numpy array (typically the entire process only takes less than 2 minutes).

## Installation

*Note: PyD4 doesn't support Python2 or earlier, please use Python3 or later.*

Install through pip is recommended

```bash
pip install pyd4
```

or you can also install from source with the `setup.py`:

```bash
git clone https://github.com/38/d4-format.git
cd d4-format/pyd4
./setup.py install
```


## Quick start by Example

Here's some basic example to use the package.

```python
from pyd4 import D4File

# Open a D4 File
file = D4File("test.d4")

# Print the chrom list
print(file.chroms())

# Get the mean cov for region chr1:10000000-20000000
print(file.mean("chr1:10000000-20000000"))

# Get a iterator over values
for i in file.value_iter("chr1", 0, 10000):
	print(i)

# Load the values to numpy 
data = file["chr1:0-10000"]
```

## Use PyD4 with NumPy

PyD4 can be use with NumPy effeciently. It can load data from a D4 file as a numpy array for further analysis. For example

```python
from pyd4 import D4File

# Open a D4 File
file = D4File("test.d4")

# Load chr1 as np array (this will take < than 1s)
per_base_depth = file.load_to_np("1")

# Then we can count the number of locus that is greater than 30 with numpy API
print((per_base_depth > 30).sum())
```

Alternatively, you can also use the index operator for that

```python
per_base_depth_2 = file["2"]
```

It's possible to load a region from chromosome instead of the entire chromosome.
```python
per_base_from_1m = file["3:1000000-"]
per_base_first_1m = file["3:-1000000"]
per_base_12345_to_22345 = file["3:12345-22345"]
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

## Dump NumPy array as D4File

```python
import pyd4

input_file = pyd4.D4File("input.d4")

chr1_data = input_file["1"]

chr1_flags = chr1_data > 64

# create_on_same_genome will create a new D4 file that copies the same genome size from input_file and the list ["1"] tells the API only copy the chromosome 1
# for_bit_array tells PyD4 that this output should be optimized for a boolean array
# and finally we call get_writer to get the writer
output_file = input_file.create_on_same_genome("output.d4", ["1"]).for_bit_array().get_writer()

# Then we can dump the numpy array to the D4 file
# The first parameter specifies the chromosome we want to write
# The second parameter specifies the locus in the genome to write the first value of the np array
# The last parameter is the actual np array
output_file.write_np_array("1", 0, chr1_flags)
```

## Fast Summarize 

One of the key advantage of D4 is it provide a highly effecient way to summarize the data on multi-core CPUs. D4Py also provides the API that exposes those feature to Python users. Although most of the summarize task can be done with load_to_np API and numpy routines, but numpy doesn't support multicore CPU effeciently. Thus the summarize API is a faster way to summarize data.

To get the mean depth of chromosome 1
```python
import pyd4

input_file = pyd4.D4File("input.d4")

# Slower way (single threaded) to compute the mean depth with numpy
np_array = input_file["1"]
print(np_array.mean())

# Faster way (parallel) to compute the same summary
print(input_file.mean("1"))
```

D4 also provides a high effecient way to perform batch summarize (For example down sample chromosome one per 1000 base pair window).

```python
import pyd4

input_file = pyd4.D4File("input.d4")

down_sampled_chr1 = input_file.resample("1", bin_size = 1000)
print(down_sampled_chr1)
```