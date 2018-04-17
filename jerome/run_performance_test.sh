#!/bin/sh

for i in `seq 5`
do
	#echo "Starting iteration $i.." 
	mongo -quiet --host 192.168.108.213 db1 ~/dataloading-mongo/jerome/test_performance3.js
done

