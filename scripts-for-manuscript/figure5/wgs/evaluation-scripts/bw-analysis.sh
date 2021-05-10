#!/usr/bin/zsh
export LD_LIBRARY_PATH=${PWD}/bw-fhcr/libBigWig/
pushd bw-fhcr/libBigWig
make
cd ..
make
popd

mkdir -p fhrc-bw-perf 
mkdir -p fhrc-bw-result

for CRAM in ~/data/SV_1000_WGS/bigwig/*.bw
do
	echo ${CRAM}
	if [ ! -e fhrc-bw-result/$(basename ${CRAM} .bw).bed ]
	then
		/usr/bin/time -f "Wall=%e\tCPU=%P\tMemory=%M" -o ./fhrc-bw-perf/$(basename ${CRAM} .bw).txt \
			bw-fhcr/bw-fhcr ${CRAM} > fhrc-bw-result/$(basename ${CRAM} .bw).bed
	fi
done
