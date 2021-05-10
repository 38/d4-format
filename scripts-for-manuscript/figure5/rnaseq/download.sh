#!/bin/zsh
mkdir -p bam
head -n $3 $1 | tail -n $(($3-$2)) | while read i
do
	echo ${i} ${PWD}
	wget $i --continue -q --directory-prefix=bam/
	touch bam/$(basename ${i}).completed
	echo Done ${i}
done
