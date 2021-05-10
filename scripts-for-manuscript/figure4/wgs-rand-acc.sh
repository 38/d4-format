#!/usr/bin/zsh
source config.sh

#export REF_PATH=data/hg19.fa
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${PWD}/src/libBigWig

PYTHON=${HOME}/.linuxbrew/bin/python3

#timed_run wgs-rand-acc hdf5  ${PYTHON} src/h5-random.py output/hg002.h5 data/hg002-random-regions.bed > /dev/null
timed_run wgs-rand-acc d4 d4utils stat -t64 -r data/hg002-random-regions.bed output/hg002.d4 > /dev/null
timed_run wgs-rand-acc d4-deflate d4utils stat -t64 -r data/hg002-random-regions.bed output/hg002-deflate.d4 > /dev/null
timed_run wgs-rand-acc mosdepth src/bgzf-mean-random.sh output/hg002.per-base.bed.gz data/hg002-random-regions.bed > /dev/null
timed_run wgs-rand-acc bigwig src/bw-mean-random/bw-mean output/hg002.bw data/hg002-random-regions.bed > /dev/null
