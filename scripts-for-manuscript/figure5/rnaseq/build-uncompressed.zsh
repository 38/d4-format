#!/bin/zsh
export LD_LIBRARY_PATH=~/c2f-project/d4/hts/htslib
function run_d4() {
	mkdir -p uncompressed 
	if [ ! -e uncompressed/$(basename $1).d4 ]
	then
		~/c2f-project/d4/target/release/d4utils create -A $1 uncompressed/$(basename $1).d4
	fi
}

count=0

for file in d4/*.d4
do
	file=bam/$(basename ${file} .d4)
	run_d4 ${file} &

	count=$((${count} + 1))

	if [ ${count} = 16 ]
	then
		count=0
		wait
	fi

done
