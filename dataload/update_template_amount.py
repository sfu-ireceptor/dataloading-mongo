#!/usr/bin/env python

import pandas as pd
import pymongo
import json
import sys
import datetime
import re 

#script to update template_amount field to use ADC v 1.4 terms
 
# Replace mongo db name
db_host = sys.argv[1]
db_name = sys.argv[2]

mng_client = pymongo.MongoClient(db_host, 27017)
mng_db = mng_client[db_name]
#  Replace mongo db collection names
sample_collection_name = sys.argv[3]
db_cm = mng_db[sample_collection_name]

# set the database fields we'll need, updated date field and template amount
old_template_amount_field = sys.argv[4]
update_date_field = sys.argv[5]

# database fields we'll be updating to - value and unit, and we want to 
#  preserve the legacy field
template_amount_unit_field = "template_amount_unit"
template_amount_value_field = "template_amount"
template_amount_unit_id_field = "template_amount_unit_id"
template_amount_legacy_field = "ir-v1-3-template_amount"

# create the regular expression - we can only handle template amounts of 
#  format like "40 mg" or "20nanograms"

pattern = re.compile("([0-9]+\.?[0-9]+)[\s]*([a-zA-z]+)")

# optional noupdate argument if user wants to test what an update would do
#   without affecting the database
# used values are "check", "verbose" and "check-verbose"
try: 
    noupdate = sys.argv[6]
    if (noupdate not in ["check", "verbose", "check-verbose"]):
        print ("Valid parameters are check, verbose and check-verbose")
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

unit_translation = { "ng": "nanogram", 
    "ngs": "nanogram",
    "nanogram": "nanogram",
    "nanograms": "nanogram", 
    "ug": "microgram",
    "ugs": "microgram",
    "microgram": "microgram",
    "micrograms": "microgram",
    "mg": "miligram", 
    "mgs": "miligram",
    "miligram": "miligram",
    "miligrams": "miligram",
    "g": "gram",
    "gs": "gram",
    "gram": "gram",
    "grams": "gram"
    }

unit_id_translation = { "nanogram": "UO:0000024",
    "microgram": "UO_0000023",
    "milligram": "UO:0000022",
    "gram" : "UO_0000021"
    }
def updateDocument(doc, targetCollections):
    id = doc["_id"]
    template_amount = doc[old_template_amount_field]
    global possible_update
    global had_warnings
    has_update = False

    try:
        match = pattern.match(template_amount)
        amount = match.group(1)
        units = match.group(2)    
    except: 
        had_warnings = True
        if (verbose == True):
            print ("For sample _id", id, "could not find the template amount I could process", 
                template_amount, flush=True)
        return()

    try:
        new_units = unit_translation[units]
        unit_id = unit_id_translation[new_units]
        has_update = True
    except:
        had_warnings = True 
        if (verbose == True):
            print ("For sample _id", id, "could not find the template amount unit I could process", 
                template_amount, flush=True)
        return()

    if (has_update == True):
        possible_update = True
        new_update_date = datetime.datetime.now().isoformat()
        if (verbose == True):
            print ("Updating", id, "from", template_amount,"to",amount, unit_id, new_units, flush=True)
        if (update == True):
            db_cm.update({"_id": id},{"$set":{template_amount_legacy_field: template_amount, 
                template_amount_unit_field: new_units, template_amount_value_field: amount,
                template_amount_unit_id_field: unit_id,
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

