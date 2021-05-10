#!/usr/bin/zsh

# The configuration of the D4 evaluation scripts

HTSLIB_PREFIX=bin
D4BIN_PREFIX=bin

DATA_PREFIX=data

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${PWD}/${HTSLIB_PREFIX}
export PATH=${PWD}/${D4BIN_PREFIX}:${PATH}
export REF_PATH=${PWD}/data/hs37d5.fa

function timed_run() {
	local EXPERIMENT=$1
	local TAG=$2
	shift
	shift
	mkdir -p performance/${EXPERIMENT}
	/usr/bin/time -f "Wall=%e\tCPU=%P\tMemory=%M" --append -o performance/${EXPERIMENT}/${TAG}.txt \
		$@
}
