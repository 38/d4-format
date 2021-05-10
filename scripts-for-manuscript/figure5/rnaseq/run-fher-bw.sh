#!/bin/zsh


for i in bigwig/*.bw
do
	dd if=${i} of=/dev/null bs=1M
		/usr/bin/time -f "Wall=%e\tCPU=%P\tMemory=%M" --append -o bw-performance/$(basename $i .bw).txt \
			 bw-fhcr/bw-fhcr $i> /dev/null
done
