#!/bin/zsh

function get_sizes() {
	ls -l $1/*.d4 | sed 's/[^ \t]*\(\(HG\|NA\)[0-9]*\)\.final\.d4$/\1/g' | awk '{print "'${2}'\t"$NF"\t"$5}'
}

function get_bam_size() {
	ls -l cram/*.cram | sed 's/[^ \t]*\(\(HG\|NA\)[0-9]*\)\.final\.cram$/\1/g' | awk '{print "'${2}'\t"$NF"\t"$5}'
}

function get_mean_depth() {
	ls $1/*.d4 | while read file
	do
		id=$(basename ${file} .final.d4)
		d4utils stat ${file} | awk '{size += $3; depth += $4 * $3}END{print "'mean-depth'\t'${id}'\t"depth/size}'
	done
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
				buffer = buffer"\t"((data[row","key] + 0)/1024.0/1024.0/1024.0);
			}
			print buffer
		}
	}
' \
	<(get_bam_size cram CRAM) \
	<(get_sizes d4-4bit K=4) \
	<(get_sizes d4-5bit K=5) \
	<(get_sizes d4-6bit K=6) \
	<(get_sizes d4-7bit K=7) \
	<(get_sizes d4-auto-sampled Sampled) \
	<(get_mean_depth d4)
