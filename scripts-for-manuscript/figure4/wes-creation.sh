#!/usr/bin/zsh
source config.sh

#export REF_PATH=/scratch/ucgd/lustre-work/u6000771/bcbio/genomes/Hsapiens/g1k_v37_decoy/seq/g1k_v37_decoy.fa 

timed_run wes-creation d4-single-deflate d4utils create -t1 -zR0-1 ${DATA_PREFIX}/RNAseq.bam output/RNAseq_deflated.d4
#exit 0
timed_run wes-creation d4-single d4utils create -t1 -R0-1 ${DATA_PREFIX}/RNAseq.bam output/RNAseq.d4
timed_run wes-creation d4 d4utils create -t64 -R0-1 ${DATA_PREFIX}/RNAseq.bam output/RNAseq.d4
timed_run wes-creation d4-defalte d4utils create -t64 -zR0-1 ${DATA_PREFIX}/RNAseq.bam output/RNAseq_deflated.d4
timed_run wes-creation mosdepth mosdepth output/RNAseq ${DATA_PREFIX}/RNAseq.bam
timed_run wes-creation bigwig mosdepth-bw ${DATA_PREFIX}/RNAseq.bam output/RNAseq.bw
