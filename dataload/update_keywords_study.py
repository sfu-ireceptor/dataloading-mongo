#!/usr/bin/env python

import pandas as pd
import pymongo
import json
import sys
import datetime


#script to update keywords_study field to use ADC v 1.4 terms
# contains_ig -> contains_ig
# contains_tcr -> contains_tr
# if repertoire has rearrangements, add contains_schema_rearrangement
# contains_single_cell -> contains_paired_chain (inferred)

# Replace mongo db name
db_host = sys.argv[1]
db_name = sys.argv[2]

mng_client = pymongo.MongoClient(db_host, 27017)
mng_db = mng_client[db_name]
#  Replace mongo db collection names
sample_collection_name = sys.argv[3]
db_cm = mng_db[sample_collection_name]

# set the database fields we'll need: keywords_study to update, 
#  field that determines if study is single cell to find paired chains and
#  field that determines
keywords_study_field = sys.argv[4]
single_cell_field = sys.argv[5]
sequence_count_field = sys.argv[6]
update_date_field = sys.argv[7]


keyword_translation = { "contains_ig": "contains_ig", 
    "contains_tcr": "contains_tr", 
    "contains_tr": "contains_tr",
    "contains_paired_chain": "contains_paired_chain",
    "contains_single_cell": "contains_paired_chain",
    "contains_schema_rearrangement": "contains_schema_rearrangement",
    "contains_schema_clone": "contains_schema_clone",
    "contains_schema_cell": "contains_schema_cell",
    "contains_schema_receptor": "contains_schema_receptor" }

def updateDocument(doc, targetCollections):
    id = doc["_id"]
    sequence_count = doc[sequence_count_field]
    has_single_cell = doc[single_cell_field]

    updated_keywords_study = list()

    has_update = False
    old_keywords_study = list()
    try:
        old_keywords_study_value = doc[keywords_study_field]
        # some older repositories have keywords_study as a string, we want it as array
        if type(old_keywords_study_value) == str:
            old_keywords_study.append(old_keywords_study_value)    
            has_update = True         
        else:
            old_keywords_study.extend(old_keywords_study_value)
    except: 
        print ("For sample _id ", id, " could not find study_keywords.", flush=True)

    for keyword in old_keywords_study:
        if keyword in keyword_translation:
            updated_keywords_study.append(keyword_translation[keyword])
            if (keyword_translation[keyword] != keyword):
                has_update = True
 
    if sequence_count > 0 and "contains_schema_rearrangement" not in updated_keywords_study: 
        updated_keywords_study.append("contains_schema_rearrangement")
        has_update = True

    if has_single_cell and "contains_paired_chain" not in updated_keywords_study:
        updated_keywords_study.append("contains_paired_chain")
        has_update = True

    if (has_update):
        new_update_date = datetime.datetime.now().isoformat()
        print ("Updating ", id, " with ", updated_keywords_study, flush=True)
        db_cm.update({"_id": id},{"$set":{keywords_study_field: updated_keywords_study, 
            update_date_field: new_update_date}})

record_count = 0
record_list = db_cm.find();
print ("Starting the proces at ",datetime.datetime.now().isoformat(), flush=True )
for r in record_list:
    record_count +=1
    updateDocument(r,db_cm)
    if (record_count % 100 == 0):
        print ("Processing record ", record_count, flush=True)
print ("Ended the process at ", datetime.datetime.now().isoformat(), flush=True)
