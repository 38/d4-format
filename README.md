# `d4` - Dense Depth Data Dump

## Synopsis

The Dense Depth Data Dump (D4) format and tool suite provide an alternative to BigWig for fast analysis and compact storage of quantitative genomics datasets (e.g., RNA-seq, ChIP-seq, WGS depths, etc.). It supports random access, multiple tracks (e.g., RNA-seq, ChiP-seq, etc. from the same sample), HTTP range requests, and statistics on arbitrary genome intervals. The D4tools software is built on a [Rust crate](https://docs.rs/d4/). We provide both a [C-API](https://github.com/38/d4-format/tree/master/d4binding/include) and a [Python API](https://github.com/38/d4-format/tree/master/pyd4/) with an [Jupyter notebook](https://github.com/38/d4-format/blob/master/pyd4/examples/D4%20Tutorial%20in%20Python.ipynb) providing examples of how to to read, query, and create single-track and multi-track D4 files.

Usage examples are provided below. Also, check out the [slide deck](https://docs.google.com/presentation/d/1vqs6mnfiVryfMAxqDyJrZsX6HI39NbwWqvB7DUCLTgw) that describes the motivation, performance and toolkits for D4

## Motivation

Modern DNA sequencing is used as a readout for diverse assays, with the count of aligned sequences, or "read depth", serving as the quantitative signal for many underlying cellular phenomena. Despite wide use and thousands of datasets, existing formats used for the storage and analysis of read depths are limited with respect to both size and speed. For example, it is faster to recalculate sequencing depth from an alignment file than it is to analyze the text output from that calculation. We sought to improve on existing formats such as BigWig and compressed BED files by creating the Dense Depth Data Dump (D4) format and tool suite. The D4 format is adaptive in that it profiles a random sample of aligned sequence depth from the input BAM or CRAM file to determine an optimal encoding that  minimizes file size, while also enabling fast data access. We show that D4 uses less disk space for both RNA-Seq and whole-genome sequencing and offers 3 to 440 fold speed improvements over existing formats for random access, aggregation and summarization for scalable downstream analyses that would be otherwise intractable.

## Manuscript

To learn more, please read the publication: [https://www.nature.com/articles/s43588-021-00085-0](https://www.nature.com/articles/s43588-021-00085-0). Note We ran the experiments described in the manuscript on a server with following hardward and software  

- Processor: Intel(R) Xeon(R) Gold 6230 CPU @ 2.10GHz
- RAM: 376GB
- OS: CentOS 7.6.180 w/ Linux Kernel 3.0.10
- Rust Version: 1.47.0-nightly

## Basic Usage by Examples (each should take seconds)

### Create a D4 file

The `d4tools create` subcommand is used to convert BAM,CRAM,BigWig and BedGraph file to D4 file.

```text
USAGE:
    create [FLAGS] [OPTIONS] <input-file> [output-file]

FLAGS:
    -z, --deflate      Enable the deflate compression
    -A, --dict-auto    Automatically determine the dictionary type by random sampling
        --dump-dict    Do not profile the BAM file, only dump the dictionary
    -h, --help         Prints help information
    -S, --sparse       Sparse mode, this is same as '-zR0-1', which enable secondary table compression and disable
                       primary table
    -V, --version      Prints version information

OPTIONS:
        --deflate-level <level>          Configure the deflate algorithm, default 5
    -d, --dict-file <dict_spec_file>     Provide a file that defines the values of the dictionary
    -R, --dict-range <dict_spec>         Dictionary specification, use "a-b" to specify the dictionary is encoding
                                         values from A to B(exclusively)
    -f, --filter <regex>                 A regex that matches the genome name should present in the output file
    -g, --genome <genome_file>           The genome description file (Used by BED inputs)
    -q, --mapping-qual <mapping-qual>    The minimal mapping quality (Only valid with CRAM/BAM inputs)
    -r, --ref <fai_file_path>            Reference genome file (Used by CRAM inputs)
    -t, --threads <num_of_threads>       Specify the number of threads D4 can use for encoding

ARGS:
    <input-file>     Path to the input file
    <output-file>    Path to the output file
```

- From CRAM/BAM file

```bash
  d4tools create -Azr hg19.fa.gz.fai hg002.cram hg002.d4
```

- From BigWig file

```bash
  d4tools create -z input.bw output.d4
```

- From a BedGraph file (extension must be ".bedgraph")

```bash
  d4tools create -z -g hg19.genome input.bedgraph output.d4
```

### View a D4 File

```text
USAGE:
    view [FLAGS] <input-file> [chr:start-end]...

FLAGS:
    -h, --help           Prints help information
    -g, --show-genome    Show the genome file instead of the file content
    -V, --version        Prints version information

ARGS:
    <input-file>          Path to the input file
    <chr:start-end>...    Regions to be viewed
```

- Convert a d4 file to a bedgraph file

```text
$ d4tools view hg002.d4 | head -n 10
chr1    0       9998    0
chr1    9998    9999    6
chr1    9999    10000   9
chr1    10000   10001   37
chr1    10001   10002   59
chr1    10002   10003   78
chr1    10003   10004   100
chr1    10004   10005   116
chr1    10005   10006   130
chr1    10006   10007   135
```

- Print given regions

```text
$ d4tools view hg002.d4 1:1234560-1234580 X:1234560-1234580
1       1234559 1234562 28
1       1234562 1234565 29
1       1234565 1234566 30
1       1234566 1234572 31
1       1234572 1234573 29
1       1234573 1234576 28
1       1234576 1234578 27
1       1234578 1234579 26
X       1234559 1234562 26
X       1234562 1234563 25
X       1234563 1234565 26
X       1234565 1234574 25
X       1234574 1234575 26
X       1234575 1234576 25
X       1234576 1234578 26
X       1234578 1234579 25
```

- Print the genome layout

```text
$ d4tools view -g hg002.d4 | head -n 10
1       249250621
2       243199373
3       198022430
4       191154276
5       180915260
6       171115067
7       159138663
8       146364022
9       141213431
10      135534747
```

### Run stat on a D4 file

```text
USAGE:
    stat [OPTIONS] <input_d4_file>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -r, --region <bed_file_path>      A bed file that describes the region we want to run the stat
    -s, --stat <stat_type>            The type of statistics we want to perform, by default average. You can specify
                                      statistic methods: perc_cov, mean, median, hist, percentile=X% (If this is not speficied
                                      d4tools will use mean by default)
    -t, --threads <num_of_threads>    Number of threads

ARGS:
    <input_d4_file>
```

- Mean cov for each Chrom

```text
$ d4tools stat hg002.d4
chr1    0       249250621       27.075065016588262
chr10   0       135534747       31.59483947684648
chr11   0       135006516       25.970025943044114
chr11_gl000202_random   0       40103   14.47213425429519
chr12   0       133851895       25.80992053194316
chr13   0       115169878       24.18613685602758
chr14   0       107349540       24.25194093053403
chr15   0       102531392       23.04176524785697
chr16   0       90354753        28.106620932271266
chr17   0       81195210        25.58382477242192
...
```

- Median cov for each Chrom

```text
$ d4tools stat -s median hg002.d4 | head -n 10
1       0       249250621       25
10      0       135534747       26
11      0       135006516       26
12      0       133851895       26
13      0       115169878       26
14      0       107349540       25
15      0       102531392       24
16      0       90354753        24
17      0       81195210        25
18      0       78077248        26
```

- Top 5% for the given region defined in a bed file

```text
$ d4tools stat -s percentile=95 -r region.bed hg002.d4
1       2000000 3000000 33
2       0       150000000       38
```

- Percent of bases at or above coverage levels (perc_cov)
```text
$ d4tools stat -H -s perc_cov=1,2 -r data/input_10nt.multiple_ranges.bed data/input_10nt.d4 
#Chr    Start   End     1x      2x
chr     0       2       0.000   0.000
chr     0       8       0.625   0.375
chr     0       10      0.600   0.300
chr     1       6       0.600   0.400
chr     3       9       1.000   0.500
chr     4       5       1.000   1.000
chr     5       10      0.800   0.400
```

### Reading D4 File Served by static HTTP Server

D4 now supports showing and run statistics for D4 files that is served on a HTTP server without downloading the file to local.
For printing the file content, simple use the following command:

```
$ d4tools show https://d4-format-testing.s3.us-west-1.amazonaws.com/hg002.d4 | head -n 10
1       0       9998    0
1       9998    9999    6
1       9999    10000   10
1       10000   10001   38
1       10001   10002   55
1       10002   10003   72
1       10003   10004   93
1       10004   10005   110
1       10005   10006   126
1       10006   10007   131
```

To run statistics on a D4 file on network, we required the D4 file contains the data index to avoid full file accessing.

- (On the server side) Prepare the D4 file that need to be accessed on web

```bash
d4tools index build --sum hg002.d4
```

- (On the client side) Run mean depth statistics on this file

```
$ d4tools stat https://d4-format-testing.s3.us-west-1.amazonaws.com/hg002.d4
1       0       249250621       23.848327146193952
2       0       243199373       25.02162749408075
3       0       198022430       23.086504175309837
4       0       191154276       23.18471121200553
5       0       180915260       23.2536419094774
6       0       171115067       24.515156108374722
7       0       159138663       24.398102314080646
8       0       146364022       26.425789139628865
9       0       141213431       19.780247114029827
10      0       135534747       25.475887087464

....
```

## Build

### Prerequisites

To build `d4`, Rust toolchain is required. To install Rust toolchain,
please run the following command and follow the prompt to complete the
Rust installation.

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

`gcc` or `clang` is required to build `htslib` embeded with the `d4` library.
For details, please check the htslib repository.

### Build Steps

Normally, the build step is quite easy. Just

```bash
# For Debug Build
cargo build
# For Release Build
cargo build --release
```

And it will produce the `d4tools` binary which you can find at either
`target/debug/d4tools` or `target/release/d4tools` depending on which build mode
you choose.

#### Troubleshooting

- Compiling error: asking for -fPIC or -fPIE option

For some environment, the Rust toolchain will ask compile the `-fPIC` or `-fPIE` to build the `d4tools` binary.
In this case, you should be able to use the following workaround:

```bash
# To build a debug build :
cd d4tools && cargo rustc --bin d4tools -- -C relocation-model=static

# To build a release build :
cd d4tools && cargo rustc --bin d4tools --release -- -C relocation-model=static
```

### Installation (< 2 minutes)

- Install bioconda 

Assuming you have bioconda environment installed and configured, you can simply install d4tools and d4binding from bioconda repository

```bash
conda install d4tools
```

- Install from crates.io: Assuming you have Rust compiler toolchain, you can install it from crate.io as well.

```
cargo install d4tools
```

- Install from source code: The following steps allows you to install d4tools from source code. You can choose to install the d4tools binary by running

```bash
cargo install --path .
```

### Using D4 in C/C++

D4 provides a C binding that allows the D4 library used in C and C++.
Here's the steps to build D4 binding.

1. Install or build the binding library

- The easist way to install d4binding library is using bioconda. 

```
conda install d4binding
```
Then the header file will be installed under `<conda-dir>/include`. And `libd4binding.so` or `libd4binding.dylib` will be installed under `<conda-dir>/lib`.

- Alternatively, you can choose install from the source code as well:

```bash
# Build the D4 binding library, for debug build, remove "--release" argument
cargo build --package=d4binding --release
```

After running this command, you should be able to find the library "target/release/libd4binding.so".

2. Use D4 in C

Here's a small example that prints all chromosome name and size defined in a D4 file.

```c
#include <stdio.h>
#include <d4.h>

int main(int argc, char** argv) 
{
    d4_file_t* fp = d4_open("input.d4", "r");

    d4_file_metadata_t mt = {};
    d4_file_load_metadata(fp, &mt);

    int i;
    for(i = 0; i < mt.chrom_count; i ++)
        printf("# %s %d\n", mt.chrom_name[i], mt.chrom_size[i]);
    
    d4_close(fp);
    return 0;
}
```

3. Compile C++ code against D4 binding library

```
gcc print-chrom-info.c -o print-chrom-info -I d4binding/include -L target/release -ld4binding  
```

For more examples, see `d4binding/examples/`

### Sample Data

- WGS  [https://home.chpc.utah.edu/~u0875014/hg002.cram](https://home.chpc.utah.edu/~u0875014/hg002.cram)
- RNASeq [https://www.encodeproject.org/files/ENCFF164HRL/](https://www.encodeproject.org/files/ENCFF164HRL/)
