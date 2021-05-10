#!/usr/bin/zsh
source config.sh

#export REF_PATH=/scratch/ucgd/lustre-work/u6000771/bcbio/genomes/Hsapiens/g1k_v37_decoy/seq/g1k_v37_decoy.fa 
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${PWD}/src/libBigWig

PYTHON=${HOME}/.linuxbrew/bin/python3
timed_run wes-seq-acc hdf5  ${PYTHON} src/h5-mean.py output/RNAseq.h5
timed_run wes-seq-acc d4-single-unc d4utils stat -t1 output/RNAseq.d4
timed_run wes-seq-acc d4-unc d4utils stat -t64 output/RNAseq.d4
timed_run wes-seq-acc d4-single d4utils stat -t1 output/RNAseq_deflated.d4
timed_run wes-seq-acc d4-compressed d4utils stat -t64 output/RNAseq_deflated.d4
timed_run wes-seq-acc mosdepth src/bgzf-mean/mean output/RNAseq.per-base.bed.gz
timed_run wes-seq-acc bigwig src/bw-mean/bw-mean output/RNAseq.bw
