#!/bin/zsh
filtered=output/$(basename $1).$(basename $2).filtered.bed
tabix -R $2 $1 > ${filtered}
exec bedtools coverage -b ${filtered} -a $2
