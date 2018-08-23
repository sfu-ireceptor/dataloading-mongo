#!/bin/sh

for i in `seq 9`
do
	#echo "Starting iteration $i.." 
	mongo -quiet --host 192.168.108.213 db1 ~/dataloading-mongo/jerome/test_performance2.js > "run$i-single.txt"
done



