#!/bin/zsh
function get_bam_size() {
	samtools idxstats ${1} | awk '{sum += $3} END {print sum }'
}

function get_bam_sizes() {
	ls -Ll bam/ENC*.sorted.bam | sed 's/[^ \t]*bam\/\(ENC[0-9A-Z]*\.sorted\.bam\)$/\1/g' | awk '{print $NF"\t"$5}'
}

function get_data_sizes() {
	ls -l $1/*.$2 | awk '{print $NF"\t"$5}' | sed -e 's/\.'$2'//g' -e 's/^[^\/]*\///g'
}

function get_perofrmance() {
	rg . $1/*.txt | awk '{print $1}' | sed -e 's/\.txt//g' -e 's/^[^\/]*\///g' -e 's/:Wall=/\t/g'
}

awk '
	{
		stats[$1] = stats[$1]"\t"$2 
	}
	END {
		print "Sample\tBam Size\tD4(K=0)\tD4(K=6)\tD4 Unc\tBigWig\tFHCR-D4\tFHCR-D4-UNC\tFHCR-BW"
		for(key in stats) print key""stats[key];
	}
' \
	<(get_bam_sizes) \
	<(get_data_sizes d4 d4) \
	<(get_data_sizes sub_opt_d4 d4) \
	<(get_data_sizes uncompressed d4) \
	<(get_data_sizes bigwig bw) \
	<(get_perofrmance performance) \
	<(get_perofrmance performance-unc) \
	<(get_perofrmance bw-performance)
