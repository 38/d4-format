# `d4` - Dense Depth Data Dump

## Motivation

Modern DNA sequencing is used as a readout for diverse assays, with the count of aligned sequences, or "read depth", serving as the quantitative signal for many underlying cellular phenomena. Despite wide use and thousands of datasets, existing formats used for the storage and analysis of read depths are limited with respect to both size and speed. For example, it is faster to recalculate sequencing depth from an alignment file than it is to analyze the text output from that calculation. We sought to improve on existing formats such as BigWig and compressed BED files by creating the Dense Depth Data Dump (D4) format and tool suite. The D4 format is adaptive in that it profiles a random sample of aligned sequence depth from the input BAM or CRAM file to determine an optimal encoding that  minimizes file size, while also enabling fast data access. We show that D4 uses less disk space for both RNA-Seq and whole-genome sequencing and offers 3 to 440 fold speed improvements\* over existing formats for random access, aggregation and summarization for scalable downstream analyses that would be otherwise intractable.

We runs the experiment on a server with following hardward and software  
 - Processor: AMD EPYC 7702P 64-Core Processor
 - RAM: 503GB
 - OS: CentOS 7.6.180 w/ Linux Kernel 3.0.10
 - Rust Version: 1.47.0-nightly

## Basic Usage by Examples (each should take seconds)

### Create a D4 file

The `d4utils create` subcommand is used to convert BAM,CRAM,BigWig and BedGraph file to D4 file.

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
  d4utils create -Azr hg19.fa.gz.fai hg002.cram hg002.d4
```

- From BigWig file

```bash
  d4utils create -z input.bw output.d4
```

- From a BedGraph file

```bash
  d4utils create -z -g hg19.genome input.bedgraph output.d4
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
$ d4utils view hg002.d4 | head -n 10
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
$ d4utils view hg002.d4 1:1234560-1234580 X:1234560-1234580
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
$ d4utils view -g hg002.d4 | head -n 10
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
                                      statistic methods: mean, median, hist, percentile=X% (If this is not speficied
                                      d4utils will use mean by default)
    -t, --threads <num_of_threads>    Number of threads

ARGS:
    <input_d4_file>
```


- Mean cov for each Chrom

```text
$ d4utils stat hg002.d4
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
$ d4utils stat -s median hg002.d4 | head -n 10
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
$ d4utils stat -s percentile=95 -r region.bed hg002.d4
1       2000000 3000000 33
2       0       150000000       38
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

And it will produce the `d4utils` binary which you can find at either
`target/debug/d4utils` or `target/release/d4utils` depends on which build mode
you choose.

### Installation (< 2 minutes)

You can choose to install the d4utils binary by running

```bash
cargo install --path .
```

Or you can choose install from crates.io:

```bash
cargo install d4utils
```
