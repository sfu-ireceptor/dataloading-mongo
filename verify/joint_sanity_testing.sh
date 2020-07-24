#!/bin/bash

# REPERTOIRE SANITY CHECK ADC API
# AUTHOR: LAURA GUTIERREZ FUNDERBURK
# SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
# CREATED ON: July 2020
# LAST MODIFIED ON: July 22, 2020
# This script generates facet queries (JSON files) and performs repertoire sanity check

TIME1=`date +%Y-%m-%d_%H-%M-%S`

echo "Current working directory: `pwd`"
echo "Starting run at: " ${TIME1}


# ---------------------------------------------------------------------

echo "Begin Script"
echo $PWD
echo " "

echo "Generate facet queries"
echo ""

# $0 base_url       String containing URL to API server (e.g. https://airr-api2.ireceptor.org)
# $1 entry_point    Options: string 'rearragement' or string 'repertoire'
# $2 path_to_json   Enter full path to JSON directory where facet JSON query files will be stored
# $3 no_filters     Enter full path to JSON query nofilters
# $4 study_id       Enter study_id

python3 /app/verify/generate_facet_json.py "$0" "$1" "$2" "$3" "$4"

echo "Begin sanity check"
echo ""

# $5 mapping_file    Indicate the full path to where the mapping file is found
# $0 base_url        String containing URL to API server (e.g. https://airr-api2.ireceptor.org)
# $1 entry_point     Options: string 'rearragement' or string 'repertoire'
# $3 no_filters      Enter full path to JSON query nofilters
# $6 master_md       Full path to master metadata
# $4 study_id        Study ID (study_id) associated to this study
# $2 path_to_json    Enter full path to JSON directory where facet JSON query files will be stored
# $7 annotation_dir  Enter full path to where annotation files associated with study_id
# $8 details_dir     Enter full path where you'd like to store content feedback in CSV format


python3 /app/verify/AIRR-repertoire-checks.py "$5" "$0" "$1" "$3" "$6" "$4" "$2" "$7" "$8"  

echo "End Script" 

