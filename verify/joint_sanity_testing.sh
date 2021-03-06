#!/bin/bash

# REPERTOIRE SANITY CHECK ADC API
# AUTHOR: LAURA GUTIERREZ FUNDERBURK
# SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
# CREATED ON: July 2020
# LAST MODIFIED ON: July 22, 2020
# This script generates facet queries (JSON files) and performs repertoire sanity check


# Get the directory where the script is. That is where the python code will be.
SCRIPT_DIR=`dirname "$0"`

# ---------------------------------------------------------------------
TIME=`date +%Y-%m-%d_%H-%M-%S`
echo "Starting run at: " ${TIME}
echo "Generate facet queries"
echo ""

# $1 base_url       String containing URL to API server (e.g. https://airr-api2.ireceptor.org)
# $2 entry_point    Options: string 'rearragement' or string 'repertoire'
# $3 path_to_json   Enter full path to JSON directory where facet JSON query files will be stored
# $4 no_filters     Enter full path to JSON query nofilters
# $5 study_id       Enter study_id

python3 $SCRIPT_DIR/generate_facet_json.py -v "$1" "$2" "$3" "$4" "$5"
if [ $? -ne 0 ]
then
  echo "ERROR: Could not generate queries correctly."
  exit 1
fi

echo ""
echo "Begin sanity check"
echo ""

# $6 mapping_file    Indicate the full path to where the mapping file is found
# $1 base_url        String containing URL to API server (e.g. https://airr-api2.ireceptor.org)
# $2 entry_point     Options: string 'rearragement' or string 'repertoire'
# $4 no_filters      Enter full path to JSON query nofilters
# $7 master_md       Full path to master metadata
# $5 study_id        Study ID (study_id) associated to this study
# $3 path_to_json    Enter full path to JSON directory where facet JSON query files will be stored
# $8 annotation_dir  Enter full path to where annotation files associated with study_id
# $9 details_dir     Enter full path where you'd like to store content feedback in CSV format

python3 $SCRIPT_DIR/AIRR-repertoire-checks.py "$6" "$1" "$2" "$4" "$7" "$5" "$3" "$8" "$9" "CC-FC"
TIME=`date +%Y-%m-%d_%H-%M-%S`
echo "Ending run at: " ${TIME}
