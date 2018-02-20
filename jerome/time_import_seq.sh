#!/bin/sh

cd /home/ubuntu/scratch

for i in `seq 100`
do
	python /home/ubuntu/dataloading-mongo/yang_script/sequenceLoading_imgt_20180125.py db1 sequences samples /home/ubuntu/data_to_load/seq/
	du -skh /mnt/mongodbdata
done
