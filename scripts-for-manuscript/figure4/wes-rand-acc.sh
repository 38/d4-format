#!/usr/bin/zsh
source config.sh

#export REF_PATH=/scratch/ucgd/lustre-work/u6000771/bcbio/genomes/Hsapiens/g1k_v37_decoy/seq/g1k_v37_decoy.fa 
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${PWD}/src/libBigWig

PYTHON=${HOME}/.linuxbrew/bin/python3

timed_run wes-rand-acc hdf5  ${PYTHON} src/h5-random.py output/RNAseq.h5 data/random-regions.bed > /dev/null
timed_run wes-rand-acc d4-single d4utils stat -t1 -r data/random-regions.bed output/RNAseq_deflated.d4 > /dev/null
timed_run wes-rand-acc d4-single-unc d4utils stat -t1 -r data/random-regions.bed output/RNAseq.d4 > /dev/null
timed_run wes-rand-acc d4-unc d4utils stat -t64 -r data/random-regions.bed output/RNAseq.d4 > /dev/null
timed_run wes-rand-acc d4-compressed d4utils stat -t64 -r data/random-regions.bed output/RNAseq_deflated.d4 > /dev/null
timed_run wes-rand-acc mosdepth src/bgzf-mean-random.sh output/RNAseq.per-base.bed.gz data/random-regions.bed > /dev/null
timed_run wes-rand-acc bigwig src/bw-mean-random/bw-mean output/RNAseq.bw data/random-regions.bed > /dev/null
