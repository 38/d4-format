#!/bin/zsh
set -x
LINES=$(wc -l bams.txt | awk '{print $1}')

for ((i=0;i<${LINES};i+=30)) 
do
	./download.sh bams.txt $i $(($i + 30 > ${LINES} ? ${LINES} : $i + 30)) &
done

wait
