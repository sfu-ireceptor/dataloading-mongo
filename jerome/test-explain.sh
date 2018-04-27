#!/bin/bash

count=5
if [ $# -eq 1 ]
then
    count=$1
fi

js_file="test_performance_explain.js"
host_name=$(hostname)
db_name="db1"

current_time=$(date "+%Y.%m.%d-%H.%M.%S")
echo "Test performed at: $current_time"
mongo $db_name cache-dump.js > $host_name-$db_name-cache-$current_time.txt

for i in `seq 1 $count`;
do
    echo "performing test iteration $i"
    mongo $db_name $js_file > run$i-$host_name-$db_name-$current_time.txt
done

