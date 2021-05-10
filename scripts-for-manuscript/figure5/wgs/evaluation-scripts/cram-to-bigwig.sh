#!/usr/bin/zsh
export REF_PATH=/uufs/chpc.utah.edu/common/HIPAA/u6000771/Data/GRCh38_full_analysis_set_plus_decoy_hla.fa.fai

mkdir -p bigwig_prof/
mkdir -p bigwig/

function cram-to-bigwig() {
	set -x
	echo $@
	CRAM=$1
	TEMP_PREFIX=$3
	mkdir -p ${TEMP_PREFIX}
	/usr/bin/time -f "Wall=%e\tCPU=%P\tMemory=%M" -o bigwig_prof/$(basename $CRAM).mosdepth.txt \
		./mosdepth --by GRCh38.bed ${TEMP_PREFIX}/tmp ${CRAM}
	/usr/bin/time -f "Wall=%e\tCPU=%P\tMemory=%M" -o bigwig_prof/$(basename $CRAM).gzip.txt \
		gzip -d < ${TEMP_PREFIX}/tmp.per-base.bed.gz | grep -E '^chr[0-9XY]*[ \t]' > ${TEMP_PREFIX}/tmp.per-base.bed
	/usr/bin/time -f "Wall=%e\tCPU=%P\tMemory=%M" -o bigwig_prof/$(basename $CRAM).bedGraphToBigWig.txt \
		./bedGraphToBigWig ${TEMP_PREFIX}/tmp.per-base.bed ./GRCh38.genome ${TEMP_PREFIX}/out.bigwig
	mv ${TEMP_PREFIX}/out.bigwig $2
	rm -rf ${TEMP_PREFIX}
}

TASK_COUNT=0

for CRAM in cram/*.cram
do
	OUTPUT=bigwig/$(basename $CRAM .cram).bw
	if [ ! -e ${OUTPUT} ]; then
		cram-to-bigwig ${CRAM} ${OUTPUT} ./tmp/tmp_${TASK_COUNT} &
		TASK_COUNT=$((${TASK_COUNT} + 1))
	else
		echo "Skipping ${CRAM}"
	fi
	if [[ ${TASK_COUNT} -gt 50 ]]
	then
		wait
		TASK_COUNT=0
	fi
done

