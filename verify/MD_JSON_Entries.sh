#!/bin/bash
#SBATCH --time=00:40:00
#SBATCH --mem=3600M
#SBATCH --array=1-1%1
#SBATCH --account=rpp-breden-ab
#SBATCH --output=/home/lgutierr/projects/rpp-breden-ab/ireceptor/curation/cancer_data_and_papers/PRJNA381394_Vergani_Korunsky_2017/uploads/2019-01-25/2019-02-28_PRJNA486323_SanityCheck_%J.out

##Script Author: Laura Gutierrez Funderburk
##Supervised by: Dr. Felix Breden, Dr. Jamie Scott, Dr. Brian Corrie
##Created on: December 15 2018
##Last modified on: February 26 2018

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
# StudyID
Study_title=PRJNA381394

# API Response 
APIFile_name=i3Response.json

## INPUT DIR AND PARAMETERS
entry_point='http://turnkey-test1.ireceptor.org/v2/samples'

# FETCH API WITH CURL COMMAND
curl -k -d "ir_rearrangement_number=523" http://turnkey-test1.ireceptor.org/v2/samples > ${APIFile_name}

# Sequence Data Dir
MD_dir=/home/lgutierr/projects/rpp-breden-ab/ireceptor/curation/cancer_data_and_papers/PRJNA381394_Vergani_Korunsky_2017/uploads/2019-01-25/
MD_file=PRJNA381394_Vergani_2019-1-2.csv

echo "CHECK INPUT IS CORRECT"
echo ${MD_dir}${MD_file}
echo "Entry point: "${entry_point}
###############################
## OUTPUT DIRS AND ARRAY DATA##
###############################

# Scripts director
script=/home/lgutierr/projects/rpp-breden-ab/ireceptor/SanityChecking/Scripts/

# Call array data file
#array_data=/home/lgutierr/projects/rpp-breden-ab/ireceptor/SanityChecking/Turnkey/Test1/Scripts/

# Annotation directory
annotation_dir=/home/lgutierr/projects/rpp-breden-ab/ireceptor/curation/cancer_data_and_papers/PRJNA381394_Vergani_Korunsky_2017/annotation_files/imgt-2018-01-05/

#######################
# Array File Entries ##
#######################

cd ${array_data}

#Study_title=`awk -F_ '{print $1}' study_id_ver | head -$SLURM_ARRAY_TASK_ID | tail -1` 

echo ${Study_title}

#######################
### Sanity Checking ###
#######################

cd ${script}

echo "Begin Python Sanity Check "

#module load python/3.7.0

#virtualenv ~/ENV

source ~/ENV/bin/activate

python sanitychecking.py ${MD_dir}${MD_file} ${APIFile_name} ${Study_title} ${annotation_dir} "LH"

deactivate

echo "End Python Sanity Check"
