#!/usr/bin/zsh
TEMPFILE=.tmp.$(basename $1 .d4).bedgraph

d4utils view $1 > ${TEMPFILE}
d4utils stat $1 | awk '{print $1"\t"$3}' > ${TEMPFILE}.size
./bedGraphToBigWig  ${TEMPFILE} ${TEMPFILE}.size bigwig/$(basename $1 .d4).bw
rm -f ${TEMPFILE}*
