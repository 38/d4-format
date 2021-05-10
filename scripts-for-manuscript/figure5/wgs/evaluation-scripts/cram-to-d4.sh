#!/usr/bin/zsh
function run-d4() {
	CRAM=$1
	OUTPUT=$2
	/usr/bin/time -f "Wall=%e\tCPU=%P\tMemory=%M" -o d4_prof/$(basename $CRAM).txt \
		d4utils create -t 64 -zA -r GRCh38_full_analysis_set_plus_decoy_hla.fa.fai -f "^chr[0-9XY]*$" ${CRAM} ${OUTPUT}
	touch ${OUTPUT}.done
}

mkdir -p d4/
mkdir -p d4_prof/
TASK_COUNT=0

for CRAM in cram/*.cram
do
	OUTPUT=d4/$(basename $CRAM .cram).d4

	if [ ! -e ${OUTPUT} ]; then
		run-d4 ${CRAM} ${OUTPUT}&
		TASK_COUNT=$((${TASK_COUNT} + 1))
	else
		echo "Skipping ${CRAM}"
	fi

	if [[ ${TASK_COUNT} -gt 16 ]]
	then
		wait
		TASK_COUNT=0
	fi
done
