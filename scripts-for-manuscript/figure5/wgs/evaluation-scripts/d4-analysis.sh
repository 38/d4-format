#!/usr/bin/zsh
export LD_LIBRARY_PATH=~/c2f-project/d4/hts/htslib
export NTHREADS=40

#pushd find-high-cov-region
#cargo build --release
#popd

BIN=find-high-cov-region/target/release/find-high-cov-region

mkdir -p fhrc-perf 
mkdir -p fhrc-result

for CRAM in d4/*.d4
do
	echo ${CRAM}
	dd if=${CRAM} of=/dev/null
	/usr/bin/time -f "Wall=%e\tCPU=%P\tMemory=%M" -o ./fhrc-perf/$(basename ${CRAM} .d4).txt \
		 ${BIN} ${CRAM} > fhrc-result/$(basename ${CRAM} .d4).bed
	cat ./fhrc-perf/$(basename ${CRAM} .d4).txt
done
