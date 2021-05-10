#!/usr/bin/zsh
export PATH=${PATH}:${HOME}/.cargo/bin/
PREFIX=${HOME}/data/SV_1000_WGS
function combine_timing_data() {
	rg Wall ${PREFIX}/d4_prof/*.cram.txt ${PREFIX}/bigwig_prof/*.txt | awk -F'[:_/= \t]' '{
		if(length($(NF-6)) > 22) {
			type = substr($(NF-6), 20, length($(NF-6)) - 23);
		} else {
			type = "d4tools"
		}
		pipeline = $(NF-8);
		sample=substr($(NF-6),0,7);
		walltime = $(NF-4)
		data[sample"."pipeline"."type] = walltime;
		names[sample] = 1;
	}
	END {
		OFS="\t"
		for(name in names) {
			print name, 0+data[name".d4.d4tools"], 0+data[name".bigwig.mosdepth"], 0+data[name".bigwig.gzip"], 0+data[name".bigwig.bedGraphToBigWig"]
		}
	}
	'
}
awk '
BEGIN {
	OFS="\t"
}
NR==FNR{ data[$1] = $0;} 
NR!=FNR {
	gsub(/^[a-z]*\//,"", $NF);
	sample=substr($NF, 0, 7);
	if(data[sample]) 
		data[sample]=data[sample] OFS $5
}
END {
	print "Sample","D4_Create","Mosdepth","Gzip","bedGraphToBigWig","CRAM","D4","BW"
	for(key in data) if(data[key]) print data[key]
}
' <(combine_timing_data) <(ls -l ${PREFIX}/cram/*.cram)  <(ls -l ${PREFIX}/d4/) <(ls -l ${PREFIX}/bigwig/)
