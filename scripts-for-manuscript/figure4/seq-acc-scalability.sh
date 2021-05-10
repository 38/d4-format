#!/usr/bin/zsh
source config.sh

#export REF_PATH=/scratch/ucgd/lustre-work/u6000771/bcbio/genomes/Hsapiens/g1k_v37_decoy/seq/g1k_v37_decoy.fa 
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${PWD}/src/libBigWig
set -x

NTHREADS=1
dd if=output/hg002.d4 of=/dev/null bs=1M &> /dev/null

NCPUS=$(grep 'processor' /proc/cpuinfo  | wc -l)
LAST=no

while [ ${LAST} != "yes" ]
do
	if [ ${NTHREADS} -ge ${NCPUS} ]
	then
		NTHREADS=${NCPUS}
		LAST=yes
	fi
	timed_run seq-acc-scale ${NTHREADS} d4utils stat -t ${NTHREADS} output/hg002.d4 > /dev/null
	NTHREADS=$((${NTHREADS} * 2))
done
