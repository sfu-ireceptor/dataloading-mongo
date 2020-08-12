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

# $1 base_url       String containing URL to API server (e.g. https://airr-api2.ireceptor.org)
# $2 entry_point    Options: string 'rearragement' or string 'repertoire'
# $3 path_to_json   Enter full path to JSON directory where facet JSON query files will be stored
# $4 no_filters     Enter full path to JSON query nofilters
# $5 study_id       Enter study_id

echo "base_url" "$1" 
echo "entry_point" "$2" 
echo "path_to_json" "$3" 
echo "no_filters" "$4" 
echo "study_id" "$5"
python3 /app/verify/generate_facet_json.py "$1" "$2" "$3" "$4" "$5"

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

echo "mapping_file" "$6" 
echo "Base url" "$1" 
echo "entry point" "$2" 
echo "No filters" "$4" 
echo "master md" "$7" 
echo "Study id" "$5" 
echo "Path to json" "$3" 
echo "annotation dir" "$8" 
echo "details dir" "$9" 
echo "Sanity check level" "CC-FC"


python3 /app/verify/AIRR-repertoire-checks.py "$6" "$1" "$2" "$4" "$7" "$5" "$3" "$8" "$9" "CC-FC"

echo "End Script" 
