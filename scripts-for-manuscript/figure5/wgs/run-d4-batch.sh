#!/bin/zsh
set -x
NWAYS=8
NPROC=$(cat /proc/cpuinfo  | grep processor | wc -l)

function run_partially() {
	local IDX
	ls cram/*.cram | sed -e's/d4//g' -e 's/[^0-9]//g'| while read IDX 
	do
		if [[ $((${IDX} % $1)) = $2 ]]
		then
			local INPUT=cram/HG${IDX}.final.cram
			local OUTPUT=d4-${SIZE}/HG${IDX}.final.d4
			if [ ! -e ${OUTPUT} ]
			then
				d4utils create -f "^chr[0-9XY]*$" -r ref/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai ${DICT} -zt $((${NPROC} * 4 / ${NWAYS})) ${INPUT} ${OUTPUT}
				touch ${OUTPUT}.completed
			fi
		fi
	done
}

if [[ "$1" != "auto" ]]
then
	SIZE=${1}bit
	DICT="-R0-$((1<<${1}))"
else
	SIZE="auto-sampled"
	DICT="-A"
fi

if [[ "$2" = "" ]]
then
	START=0
else
	START=$2
fi

if [[ "$3" = "" ]]
then
	END=${NWAYS}
else
	END=$3
fi

for ((i=${START};i<${END};i++))
do
	run_partially ${NWAYS} $i&
done

wait
