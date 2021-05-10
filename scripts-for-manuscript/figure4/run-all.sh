function run() {
	echo $@
	for ((i=0;i<8;i++))
	do
		$@
	done
}
#run	./wgs-creation.sh  
#run	./wes-creation.sh  
run	./wgs-rand-acc.sh  
#run	./wes-rand-acc.sh  
run	./wgs-seq-acc.sh
#run	./wes-seq-acc.sh  
