pushd performance
 rg . | sed -e 's/\/\|:Wall=/\t/g' | cut -f 1,2,3 | awk '{s[$1"\t"$2]=s[$1"\t"$2]"\t"$3}END{for(key in s) print key"\t"s[key]}' | sed  's/.txt//g' | sort
 popd

