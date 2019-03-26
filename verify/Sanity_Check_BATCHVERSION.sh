#!/bin/bash
#SBATCH --time=00:40:00
#SBATCH --mem=3600M
#SBATCH --array=1-18%1
#SBATCH --account=rpp-breden-ab
#SBATCH --output=/home/lgutierr/projects/rpp-breden-ab/ireceptor/SanityChecking/Scripts/2019-03-18_Automated_SanityCheck_%J.out

##Script Author: Laura Gutierrez Funderburk
##Supervised by: Dr. Felix Breden, Dr. Jamie Scott, Dr. Brian Corrie
##Created on: December 20 2018
##Last modified on: March 18 2019

echo "Current working directory: `pwd`"
echo "Starting run at: `date`"
# ---------------------------------------------------------------------
echo ""
echo "Job Array ID / Job ID: $SLURM_ARRAY_JOB_ID / $SLURM_JOB_ID"
echo "This is job $SLURM_ARRAY_TASK_ID out of $SLURM_ARRAY_TASK_COUNT jobs."
echo ""

echo "Begin Script"


#######################
##### Directories #####
#######################

###############################
## OUTPUT DIRS AND ARRAY DATA##
###############################

# Call array data file
array_data=/home/lgutierr/projects/rpp-breden-ab/ireceptor/SanityChecking/Scripts/

# Scripts director
script=/home/lgutierr/projects/rpp-breden-ab/ireceptor/SanityChecking/Scripts/

# Sequence Data Dir
MD_dir=/home/lgutierr/projects/rpp-breden-ab/ireceptor/SanityChecking/Scripts/MasterMD/
MD_file=master_metadata_2019-03-05.xlsx

#######################
# Array File Entries ##
#######################

cd ${array_data}

Study_ID=`awk -F, '{print $1}' study_id_API_ann | head -$SLURM_ARRAY_TASK_ID | tail -1` 
API_REPO=`awk -F, '{print $2}' study_id_API_ann | head -$SLURM_ARRAY_TASK_ID | tail -1` 
ANNO_DIR=`awk -F, '{print $3}' study_id_API_ann | head -$SLURM_ARRAY_TASK_ID | tail -1` 


#######################
# AVerify Input Para ##
#######################

echo "CHECK INPUT IS CORRECT"
echo "Metadata version: " ${MD_dir}${MD_file}
echo "Study ID: "${Study_ID}
echo "Entry point API response file: "${API_REPO}
echo "Annotation directory: "${ANNO_DIR}

#######################
### Sanity Checking ###
#######################

cd ${script}

echo "Begin Python Sanity Check "

#module load python/3.7.0

#virtualenv ~/ENV

source ~/ENV/bin/activate

python sanitychecking.py ${MD_dir}${MD_file} ${API_REPO} ${Study_ID} ${ANNO_DIR} "LHF"

deactivate

echo "End Python Sanity Check"

# rm ${APIFile_name}
