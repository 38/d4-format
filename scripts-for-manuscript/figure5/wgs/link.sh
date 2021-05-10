#!/bin/zsh
find ftp-trace.ncbi.nih.gov -name "*.cram" -or -name "*.crai" | while read source 
do
	dest=cram/$(basename ${source});
	if [ ! -e ${dest} ]
	then
		echo "Hard link to ${source}"
		ln ${source} ${dest}
	fi
done
