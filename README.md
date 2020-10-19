# `d4` - Dense Depth Data Dump

## Motivation

Modern DNA sequencing is used as a readout for diverse assays, with the count of aligned sequences, or "read depth", serving as the quantitative signal for many underlying cellular phenomena. Despite wide use and thousands of datasets, existing formats used for the storage and analysis of read depths are limited with respect to both size and speed. For example, it is faster to recalculate sequencing depth from an alignment file than it is to analyze the text output from that calculation. We sought to improve on existing formats such as BigWig and compressed BED files by creating the Dense Depth Data Dump (D4) format and tool suite. The D4 format is adaptive in that it profiles a random sample of aligned sequence depth from the input BAM or CRAM file to determine an optimal encoding that  minimizes file size, while also enabling fast data access. We show that D4 uses less disk space for both RNA-Seq and whole-genome sequencing and offers 3 to 440 fold speed improvements over existing formats for random access, aggregation and summarization for scalable downstream analyses that would be otherwise intractable.

## Basic Usage by Examples

### Create a D4 file

- From CRAM/BAM file

```bash
  d4utils create -Azr ../data/hg19.fa.gz.fai ../data/hg002.cram /tmp/hg002.d4
```

- From BigWig file

```bash
  d4utils create -z input.bw output.d4
```

- From a BedGraph file

```bash
  d4utils create -z -g hg19.genome input.bedgraph output.d4
```

### Print the content of D4 file

```text
$ d4utils view ../data/hg002.d4 | head -n 10
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

### Run stat on a D4 file

```text
$ d4utils stat ../data/hg002.d4
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

### Installation

You can choose to install the d4utils bindary by running

```bash
cargo install --path .
```

Or you can choose install from crates.io:

```bash
cargo install d4utils
```
