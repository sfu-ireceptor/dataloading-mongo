#!/bin/sh

cd /home/ubuntu/test/scratch

for i in `seq 100`
do
	python /home/ubuntu/test/dataloading-mongo/yang_script/sequenceLoading_imgt_20180125.py db1 sequences samples /home/ubuntu/test/data_to_load/seq/
	du -skh /data/db
done
