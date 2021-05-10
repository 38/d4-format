#!/bin/zsh

count=0;
 find d4 -name '*.d4' | while read file 
 do
	 ./d4-to-bigwig.sh ${file} &

	 count=$((${count} + 1))

	 if [[ ${count} -gt 32 ]]
	 then
		 wait
		 count=0
	 fi
 done
