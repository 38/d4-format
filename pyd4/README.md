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
