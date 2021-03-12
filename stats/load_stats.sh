#!/bin/sh

SCRIPT_DIR=`dirname "$0"`

# check number of arguments
NB_ARGS=3
if [ $# -lt $NB_ARGS ];
then
    echo "$0: wrong number of arguments ($# instead of at least $NB_ARGS)"
    echo "usage: $0 database collection json_file"
    exit 1
fi

mongoimport -d $1 -c $2 --file $3
