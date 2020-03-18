DATA_DIR=./sample_data

export HTSLIB=dynamic
export LD_LIBRARY_PATH=`pwd`/hts/htslib:${LD_LIBRARY_PATH}
export DYLD_LIBRARY_PATH=`pwd`/hts/htslib:${DYLD_LIBRARY_PATH}

mkdir -p sample_data

function check_file() {
	if [ ! -e ${DATA_DIR}/$1 ]
	then
		curl "http://home.chpc.utah.edu/~u0875014/$1" > ${DATA_DIR}/$1
	fi
}

check_file hg002.cram
check_file hg002.cram.crai
check_file hg19.fa.gz
check_file hg19.fa.gz.fai
check_file hg19.fa.gz.gzi

if [ ! -e ${DATA_DIR}/hg002.d4 ]
then
	cargo run --release -- create -r ${DATA_DIR}/hg19.fa.gz ${DATA_DIR}/hg002.cram ${DATA_DIR}/hg002.d4
fi

if [ ! -e ${DATA_DIR}/callset.bed ]
then
	curl ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/AshkenazimTrio/analysis/NIST_SVs_Integration_v0.6/HG002_SVs_Tier1_v0.6.vcf.gz \
		| zgrep 'SVTYPE=DEL' \
		| awk '$7 == "PASS" {print}' \
		| sed -n 's/^\(.*SVLEN=-\([0-9]*\);.*\)$/\2\t\1/gp' \
		| awk '$1 > 200 {  print $2"\t"$3"\t"($3+$1); }' \
	> ${DATA_DIR}/callset.bed
fi

cargo run --release -- stat -r ${DATA_DIR}/callset.bed ${DATA_DIR}/hg002.d4

