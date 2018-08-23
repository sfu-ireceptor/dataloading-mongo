#!/bin/bash

count=5
if [ $# -eq 3 ]
then
    count=$1
    db_name=$2
    db_port=$3
else
    echo "usage: $0 count db port"
    exit
fi

perf_js_file="test_performance_explain.js"
cache_js_file="cache_dump.js"
host_name=$(hostname)

current_time=$(date "+%Y.%m.%d-%H.%M.%S")
echo "Test performed at: $current_time"
mongo $db_name --port $db_port $cache_js_file > cache-$host_name-$db_port-$db_name-$current_time.txt

for i in `seq 1 $count`;
do
    echo "Performing test iteration $i"
    time mongo $db_name --port $db_port $perf_js_file > run$i-$host_name-$db_port-$db_name-$current_time.txt
done
end_time=$(date "+%Y.%m.%d-%H.%M.%S")
echo "Test finished at: $end_time"

