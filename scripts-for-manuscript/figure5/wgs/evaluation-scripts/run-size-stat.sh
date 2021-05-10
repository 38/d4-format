#!/bin/zsh

function get_sizes() {
	ls -l $1/*.$3 | sed 's/[^ \t]*\(\(HG\|NA\)[0-9]*\)\.final\..*$/\1/g' | awk '{print "'${2}'\t"$NF"\t"$5}'
}

function get_bam_size() {
	ls -l cram/*.cram | sed 's/[^ \t]*\(\(HG\|NA\)[0-9]*\)\.final\.cram$/\1/g' | awk '{print "'${2}'\t"$NF"\t"$5}'
}

function get_mean_depth() {
	set -x
	local PENDING=()
	ls $1/*.d4
	ls $1/*.d4 | while read input
	do
		if [ ! -e $input.meancov.txt ]
		then
			id=$(basename ${input} .final.d4)
			d4utils stat ${input} | awk '{size += $3; depth += $4 * $3}END{print "'$2'\t'${id}'\t"depth/size}' > $input.meancov.txt &
			PENDING+=(${input}.meancov.txt)
		else
			cat $input.meancov.txt
		fi
		if [ ${#PENDING[@]} -gt 4 ]
		then
			wait
			for input in ${PENDING}
			do
				cat ${input}
			done
			PENDING=()
		fi
	done
}

function get_dict_size() {
	ls $1/*.d4 | while read input 
	do
		id=$(basename ${input} .final.d4)
		d4utils framedump $input | awk '$1 == ".ptab" { print "'Dict-Size'\t'$id'\t"$4/386033731}'
	done
}

function get_time() {
	pushd $1
	rg . | sed -e 's/:/\t/g' -e 's/\.final\.txt//g' -e 's/Wall=\([0-9\.]*\).*/\1/g' -e 's/^/'$1'\t/g'
	popd
}

awk '
	{
		columns[$1]=1;
		rows[$2]=1;
		data[$2","$1] = $3;
	}
	END {
		header = "Sample"
		for(key in columns) header = header"\t"key;
		print header
		for(row in rows) {
			buffer = row;
			for(key in columns) {
				buffer = buffer"\t"((data[row","key] + 0));
			}
			print buffer
		}
	}
' \
	<(get_bam_size cram CRAM) \
	<(get_sizes d4 D4 d4) \
	<(get_sizes bigwig BigWig bw) \
	<(get_dict_size d4) \
	<(get_time fhrc-perf) \
	<(get_time fhrc-bw-perf)
