#!/bin/bash

# Default number of iterations is 5
count=5

# Get the command line arguements, the number of iterations, the DB name,
# and the DB port to use.
if [ $# -eq 3 ]
then
    count=$1
    db_name=$2
    db_port=$3
else
    echo "usage: $0 count db port"
    exit
fi

# The Javascript files to use for the performance test.
perf_js_file="test_performance_explain.js"
cache_js_file="cache_dump.js"
# The host we are running on
host_name=$(hostname)

# Get the current time for the start of the overall performance test.
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
echo "Test performed at: $current_time"

# Dump the query plan cache. This is important to know as if the performance
# is not as good as expected this can help diagnose the problem.
mongo $db_name --port $db_port $cache_js_file > cache-$host_name-$db_port-$db_name-$current_time.txt

# Perform the benchmark test the number of times requested.
for i in `seq 1 $count`;
do
    # Run the performance test once. Output file is named such that it is
    # possible to track down where (and when) a performance file came from
    echo "Performing test iteration $i"
    time mongo $db_name --port $db_port $perf_js_file > run$i-$host_name-$db_port-$db_name-$current_time.txt
done

# Print out the time when the test run finished.
end_time=$(date "+%Y.%m.%d-%H.%M.%S")
echo "Test finished at: $end_time"

