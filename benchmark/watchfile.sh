#!/bin/bash

if [ $# -eq 1 ]
then
    filename=$1
else
    echo "usage: $0 file_to_watch"
    exit
fi

step_size=30
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
echo "Current time: $current_time"
echo "Watching file: $filename"

for number in {0..3600..30}
do
    echo -n "$number "
    wc -l $filename
    sleep $step_size
done
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
echo "Finish time: $current_time"
exit 0
