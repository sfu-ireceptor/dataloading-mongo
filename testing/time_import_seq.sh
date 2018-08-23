#!/bin/sh

cd /home/ubuntu/scratch
today=`date +%Y_%m_%d.%H%M%S`
filename="$today"'.txt'

for i in `seq 1000`
do
	echo "Starting iteration $i.." 
	python /home/ubuntu/dataloading-mongo/yang_script/sequenceLoading_imgt_20180125.py db1 sequences samples /home/ubuntu/data_to_load/seq/ >> ~/"$filename"
	SIZE=`ssh 192.168.108.210 'du -skh /mnt/mongodbdata | cut -f1'`
	echo "Done, DB folder size: $SIZE"
done
