#!/bin/sh

SCRIPT_DIR=`dirname "$0"`

# check number of arguments
if [ $# -ne 2 -a $# -ne 3 ];
then
    echo "$0: wrong number of arguments"
    echo "usage: $0 study_id outdir [--skipload]"
    exit 1
fi

# Set up the parameters required.
skipload=0
study_id=$1
outdir=$2

# Handle the --skipload parameter.
if [ $# -eq 3 ];
then
    if [ $3 == "--skipload" ];
    then
        skipload=1
    else
        echo "ERROR: Invalid command line parameter $3, expecting --skipload"
        exit 1
    fi
fi


TIME=`date +%Y-%m-%d_%H-%M-%S`
echo "Starting stats generation at: " ${TIME}

# Run the code to generate the stats file for the study of interest, generate
# the files in the output directory. This is essentially a file per repertoire
# in the study with all of the stats for that study. This file can be directly
# loaded into Mongo.
php $SCRIPT_DIR/stats_files_create.php ${study_id} ${outdir}

# Check for success, don't want to try and load data that wasn't generated
# succesfully.
if [ $? -ne 0 ]
then
  echo "ERROR: Could not generate stats correctly, exiting."
  exit 1
fi

# Check if we are supposed to skip the load step, if not, load
if [ $skipload -eq 0 ]
then
    # Load the JSON files generated in the previous step.
    php $SCRIPT_DIR/stats_files_load.php ${outdir}/*.json
    if [ $? -ne 0 ]
    then
      echo "ERROR: Could not load stats into repository correctly, exiting."
      exit 1
    fi
else
    echo "Warning: Skipload used, no data was loaded."
fi

# We are done.
TIME=`date +%Y-%m-%d_%H-%M-%S`
echo "Finished stats generation at: " ${TIME}

