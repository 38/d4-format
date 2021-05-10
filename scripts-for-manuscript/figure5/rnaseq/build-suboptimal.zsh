#!/bin/zsh
export LD_LIBRARY_PATH=~/c2f-project/d4/hts/htslib
function run_d4() {
	mkdir -p sub_opt_d4
	if [ ! -e sub_opt_d4/$(basename $1).d4 ]
	then
		~/c2f-project/d4/target/release/d4utils create -z $1 sub_opt_d4/$(basename $1).d4
	fi
}

for file in d4/*.d4
do
	file=bam/$(basename ${file} .d4)
	run_d4 ${file}
done
