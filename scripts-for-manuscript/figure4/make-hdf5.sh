#!/usr/bin/zsh
set -x

export PYTHONPATH=${PYTHONPATH}:$(dirname $(readlink -f $0))/bin/
export PYTHON=${HOME}/.linuxbrew/bin/python3
function convert() {
	${PYTHON} bin/d4toh5.py $1 $(dirname $1)/$(basename $1 .d4).h5
}

convert output/hg002.d4
convert output/RNAseq.d4
