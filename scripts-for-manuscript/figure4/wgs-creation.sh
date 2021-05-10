#!/usr/bin/zsh
source config.sh

#export REF_PATH=/scratch/ucgd/lustre-work/u6000771/bcbio/genomes/Hsapiens/g1k_v37_decoy/seq/g1k_v37_decoy.fa 
#export REF_PATH=/scratch/ucgd/lustre-work/u6000771/Data/hg19/hg19.fa

timed_run wgs-creation d4-single d4utils create -t1 -zr ${REF_PATH}.fai ${DATA_PREFIX}/hg002.cram output/hg002.d4
timed_run wgs-creation d4 d4utils create -t64 -r ${REF_PATH}.fai ${DATA_PREFIX}/hg002.cram output/hg002.d4
timed_run wgs-creation d4-deflate d4utils create -t64 -r ${REF_PATH}.fai -z ${DATA_PREFIX}/hg002.cram output/hg002-deflate.d4
timed_run wgs-creation mosdepth mosdepth output/hg002 ${DATA_PREFIX}/hg002.cram
timed_run wgs-creation bigwig mosdepth-bw ${DATA_PREFIX}/hg002.cram output/hg002.bw
