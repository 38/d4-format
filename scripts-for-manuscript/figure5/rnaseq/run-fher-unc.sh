#!/bin/zsh

mkdir -p performance-unc

for i in uncompressed/*.d4
do
	dd if=${i} of=/dev/null bs=1M
		/usr/bin/time -f "Wall=%e\tCPU=%P\tMemory=%M" --append -o performance-unc/$(basename $i .d4).txt \
			find-high-expressed-region/target/release/find-high-expressed-region $i> /dev/null
done
