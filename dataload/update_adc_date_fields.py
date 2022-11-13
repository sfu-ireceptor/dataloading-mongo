#!/usr/bin/env python

import pymongo
import json
import sys
import datetime

#script that will copy ir_created_at date to adc_publish_date, and, if it can, 
#  update the ir_updated_at and adc_updated_at

# Replace mongo db name
db_host = sys.argv[1]
db_name = sys.argv[2]

mng_client = pymongo.MongoClient(db_host, 27017)
mng_db = mng_client[db_name]
#  Replace mongo db collection name
collection_name = sys.argv[3]
db_cm = mng_db[collection_name]

ir_insert_date_field = "ir_created_at"
ir_update_date_field = "ir_updated_at"

adc_insert_date_field = "adc_publish_date"
adc_update_date_field = "adc_update_date"

try: 
    noupdate = sys.argv[4]
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

def updateDocument(doc, targetCollections):
    id = doc["_id"]
    global possible_update
    global had_warnings
    has_update = False
    
    # the "added to database" field may or may not exist
    #  if it does not, make it null
    try:
        ir_insert_date = doc[ir_insert_date_field]
        adc_insert_date = ir_insert_date
        has_update = True
    except:
        had_warnings = True
        ir_insert_date = None
        adc_insert_date = None
        if (verbose == True):
            print ("Document", id,"does not have the insert field ", ir_insert_date_field, flush=True)

    try:
        ir_update_date = doc[ir_update_date_field]
        adc_update_date = ir_update_date
    except:
        had_warnings = True
        ir_update_date = None
        adc_update_date = None
        if (verbose == True):
            print ("Document", id,"does not have the update field ", ir_update_date_field, flush=True)

    #we want to change update date only if the insert date has been updated
    if (has_update == True):
        ir_update_date = datetime.datetime.now().isoformat()
        adc_update_date = datetime.datetime.now().isoformat()
        possible_update = True
        if (verbose == True):
            print ("Setting new adc_publish_date for _id", id, "to", adc_insert_date, 
                flush=True)
        if (update == True):
            db_cm.update({"_id": id},{"$set":{adc_insert_date_field: adc_insert_date}})

    if (ir_update_date is not None):
        if (verbose == True):
            print ("Setting new adc_update_date for _id", id, "to", adc_update_date, 
                flush=True)
        if (update == True):    
            db_cm.update({"_id": id},{"$set":{adc_update_date_field: adc_update_date, 
               ir_update_date_field : ir_update_date}})

record_count = 0
record_list = db_cm.find();
if (verbose):
    print ("Starting the proces at ",datetime.datetime.now().isoformat(), flush=True )
for r in record_list:
    record_count +=1
    updateDocument(r,db_cm)
    if (record_count % 100 == 0 and verbose == True):
        print ("Processing record ", record_count, flush=True)
if (verbose):
    print ("Ended the process at ", datetime.datetime.now().isoformat(), flush=True)
#print out any warnings in verbose mode
if (had_warnings == True and verbose==True):
    print ("There were issues with some of the attempted updates", flush=True)
    print ("Please consult the program output for warnings")
 
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