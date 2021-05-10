#!/usr/bin/zsh
source config.sh

#export REF_PATH=/scratch/ucgd/lustre-work/u6000771/bcbio/genomes/Hsapiens/g1k_v37_decoy/seq/g1k_v37_decoy.fa 
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${PWD}/src/libBigWig

PYTHON=${HOME}/.linuxbrew/bin/python3

#timed_run wgs-seq-acc hdf5  ${PYTHON} src/h5-mean.py output/hg002.h5
timed_run wgs-seq-acc d4-single d4utils stat -t1 output/hg002.d4
timed_run wgs-seq-acc d4 d4utils stat -t64 output/hg002.d4
timed_run wgs-seq-acc d4-deflate d4utils stat -t64 output/hg002-deflate.d4
timed_run wgs-seq-acc mosdepth src/bgzf-mean/mean output/hg002.per-base.bed.gz
timed_run wgs-seq-acc bigwig src/bw-mean/bw-mean output/hg002.bw
