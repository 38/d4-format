#!/usr/bin/zsh
source config.sh

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${PWD}/src/libBigWig
set -x

NTHREADS=1

NCPUS=$(grep 'processor' /proc/cpuinfo  | wc -l)
LAST=no

while [ ${LAST} != "yes" ]
do
	if [ ${NTHREADS} -ge ${NCPUS} ]
	then
		NTHREADS=${NCPUS}
		LAST=yes
	fi
	timed_run create-scale ${NTHREADS} d4utils create -t ${NTHREADS} data/hg002.cram output/hg002.scale-test.d4  > /dev/null
	NTHREADS=$((${NTHREADS} * 2))
done
