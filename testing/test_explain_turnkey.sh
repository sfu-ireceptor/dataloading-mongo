#!/bin/bash

count=5
db_name="ireceptor" # default database name for Turnkey
db_port=27017 # the commands used below didn't really specify a port since mongod will default to 27017; but it is included here for completeness

if [ $# -eq 3 ]
then
    count=$1
    db_name=$2
    db_port=$3
else
    echo "usage: $0 count db port"
    exit
fi

TEST="dbtest"
OUT="out"
perf_js_file="test_performance_explain.js"
cache_js_file="cache_dump.js"
host_name=$(hostname)

current_time=$(date "+%Y.%m.%d-%H.%M.%S")
current_date=$(date "+%Y-%b-%d")
RECENT=$OUT/$current_date

echo "Test performed at: $current_time"

mkdir -p $RECENT

sudo docker exec -it irdn-mongo mongo --authenticationDatabase admin $db_name -u $MONGODB_SERVICE_USER -p $MONGODB_SERVICE_SECRET $TEST/$cache_js_file > $RECENT/cache-$host_name-$db_port-$db_name-$current_time.txt

for i in `seq 1 $count`;
do
    echo "Performing test iteration $i"
    sudo docker exec -it irdn-mongo mongo --authenticationDatabase admin $db_name -u $MONGODB_SERVICE_USER -p $MONGODB_SERVICE_SECRET $TEST/$perf_js_file > $RECENT/run$i-$host_name-$db_port-$db_name-$current_time.txt

done
end_time=$(date "+%Y.%m.%d-%H.%M.%S")
echo "Test finished at: $end_time"
