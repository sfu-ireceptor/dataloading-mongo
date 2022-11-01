#!/usr/bin/env python

import pandas as pd
import pymongo
import json
import sys
import datetime
import re 
import sys

#script to update collection_timepoint_relative field to use ADC v 1.4 terms
# the original value should be stored in ir_v1-3_collection_time_point_relative
# the value (e.g. "3" in "3 days") to collection_time_point_relative
# the unit (e.g. "days" in "3 days") to two fields
#  - collection_time_point_relative_unit_id = 'UO:0000033'
#  - collection_time_point_relative_unit_label = 'days'

# Replace mongo db name
db_host = sys.argv[1]
db_name = sys.argv[2]

mng_client = pymongo.MongoClient(db_host, 27017)
mng_db = mng_client[db_name]
#  Replace mongo db collection names
sample_collection_name = sys.argv[3]
db_cm = mng_db[sample_collection_name]

# set the database fields we'll need, updated date field and collection_time_point_relative old field
#  note that update may overwrite this field, hence we save the old value
old_collection_time_point_field = sys.argv[4]
update_date_field = sys.argv[5]

# database fields we'll be updating to - value and unit, and we want to 
#  preserve the legacy field
collection_time_point_field = "collection_time_point_relative"
collection_time_point_unit_field = "collection_time_point_relative_unit"
collection_time_point_unit_id_field = "collection_time_point_relative_unit_id"
collection_time_point_legacy_field = "ir_v1-3_collection_time_point_relative"

# create the regular expression - we can only handle template amounts of 
#  format like "3 days" or "Month 3" or "1year"

pattern2 = re.compile("([0-9]+[\.]?[0-9]*)\s*([a-zA-Z]+)$", re.IGNORECASE)
pattern1 = re.compile("([a-zA-Z]+)\s*([0-9]+[\.]?[0-9]*)$", re.IGNORECASE)
# optional noupdate argument if user wants to test what an update would do
#   without affecting the database
# used values are "check", "verbose" and "check-verbose"
try: 
    noupdate = sys.argv[6]
    if (noupdate not in ["check", "verbose", "check-verbose"]):
        print ("Valid parameters are check, verbose and check-verbose.")
        exit(0)
except IndexError:
    noupdate = False

# set default arguments for 'check' and 'verbose' switches, and
#  set a global variable that will show if update is possible or not
#  so script can inform the user
verbose = False
update = True
possible_update = False
had_warnings = False

if (noupdate == 'check'):
    update = False

if (noupdate == 'verbose'):
    verbose = True

if (noupdate == 'check-verbose'):
    update = False
    verbose = True

# create dictionaries to translate times to Units of Measurement ontology
#  https://bioportal.bioontology.org/ontologies/UO/?p=summary 
unit_translation = { 
    "s": "second",
    "second": "second",
    "seconds": "second",
    "m": "minute",
    "minute": "minute",
    "minutes": "minute",
    "h": "hour",
    "hour": "hour",
    "hours": "hour",
    "d": "day",
    "days": "day", 
    "day": "day",
    "w": "week",
    "week": "week",
    "weeks": "week",
    "m": "month",
    "month": "month",
    "months": "month",
    "y": "year",
    "year": "year",
    "years": "year"
    }

id_translation = {
    "second" : "UO:0000010",
    "minute": "UO:0000031",
    "hour": "UO:0000032",
    "day": "UO:0000033",
    "week": "UO:0000034",
    "month": "UO:0000035",
    "year": "UO:0000036"
    }

# update the single document if possible and update flag is set
#  
def updateDocument(doc, targetCollections):
    id = doc["_id"]
    old_collection_timepoint = doc[old_collection_time_point_field]
    new_update_date = datetime.datetime.now().isoformat()
    global possible_update
    global had_warnings
    has_update = False

    #if timepoint is not set, auto-set the value, unit and unit id to null
    if (old_collection_timepoint is None):
        had_warnings == True
        possible_update == True
        if (verbose == True):
            print ("For sample _id ", id, "collection time point relative does not exist")
        if (update == True):
            db_cm.update({"_id": id},{"$set":{collection_time_point_legacy_field: old_collection_timepoint, 
                collection_time_point_field: None, collection_time_point_unit_field: None,
                collection_time_point_unit_id_field: None,
                update_date_field: new_update_date}})
        return()
    #if the unit or unit id fields exist, the update script was run already, so skip
    if (collection_time_point_unit_field in doc or 
        collection_time_point_unit_id_field in doc):
        had_warnings = True
        if (verbose == True):
            print ("For sample _id ", id, "it looks like the script was run already", 
                flush=True)
        return()
    old_collection_timepoint = old_collection_timepoint.strip()
    try:
        match = pattern1.match(old_collection_timepoint)
        amount = float(match.group(2))
        units = match.group(1)    
    except:
        try: 
            match = pattern2.match(old_collection_timepoint)
            amount = float(match.group(1))
            units = match.group(2) 
        except: 
            if (verbose):
                print ("For sample _id", id, 
                    "could not find the collection time point relative I could process", 
                    old_collection_timepoint, flush=True)
            had_warnings = True
            possible_update == True
            if (update == True):
                db_cm.update({"_id": id},{"$set":{collection_time_point_legacy_field: old_collection_timepoint, 
                    collection_time_point_field: None, collection_time_point_unit_field: None,
                    collection_time_point_unit_id_field: None,
                    update_date_field: new_update_date}})
            return()

    try:
        new_units = unit_translation[units.lower()]
        new_id = id_translation[new_units]
        has_update = True
    except:
        if (verbose): 
            print ("For sample _id", id, 
                "could not translate the collection timepoint unit", 
                units, "to an ontology", flush=True)
        had_warnings = True
        possible_update == True
        if (update == True):
            db_cm.update({"_id": id},{"$set":{collection_time_point_legacy_field: old_collection_timepoint, 
                collection_time_point_field: None, collection_time_point_unit_field: None,
                collection_time_point_unit_id_field: None,
                update_date_field: new_update_date}})
        return
         
    if (has_update):
        possible_update = True
        new_update_date = datetime.datetime.now().isoformat()
        if (verbose):
            print ("Updating", id, "from", old_collection_timepoint, "to", amount, new_id, new_units, flush=True)
        if (update):
            db_cm.update({"_id": id},{"$set":{collection_time_point_legacy_field: old_collection_timepoint, 
                collection_time_point_field: amount,
                collection_time_point_unit_field: new_units,
                collection_time_point_unit_id_field: new_id,
                update_date_field: new_update_date}})

record_count = 0
record_list = db_cm.find();
print ("Starting the proces at ",datetime.datetime.now().isoformat(), flush=True )
for r in record_list:
    record_count +=1
    updateDocument(r,db_cm)
    if (record_count % 100 == 0):
        if (verbose):
            print ("Processing record ", record_count, flush=True)
print ("Ended the process at ", datetime.datetime.now().isoformat(), flush=True)
#print out any warnings in verbose mode
if (had_warnings == True):
    print ("There were issues with some of the attempted updates", flush=True)
    if (verbose == True):
        print ("Please consult the program output for warnings")
    else: 
        print ("Consider running the script with verbose or check-verbose option")

#if we were in check mode and there was a possible update, return 1, otherwise 0
#else, return 1 if something went wrong, and 0 if not
if (update == False):
    if (possible_update == True):
        print (1, flush=True)
    else:
        print (0, flush=True)
else: 
    if (had_warnings == True):
        print (1, flush=True)
    else:
        print (0, flush=True)

