#!/usr/bin/env python

import pandas as pd
import pymongo
import json
import sys
import datetime

#script to update ir_created_at and ir_updated_at dates from
#  "%a %b %d %Y %H:%M:%S %Z" (e.g. "Wed Nov 11 2020 00:04:05 UTC")
#  to ISO 8601 date format
#  "%Y-%M-%dT%H:%M:%SZ%z" (e.g "2022-11-11T00:04:05Z")

# Replace mongo db name
db_host = sys.argv[1]
db_name = sys.argv[2]

mng_client = pymongo.MongoClient(db_host, 27017)
mng_db = mng_client[db_name]
#  Replace mongo db collection name
collection_name = sys.argv[3]
db_cm = mng_db[collection_name]


insert_date_field = sys.argv[4]
update_date_field = sys.argv[5]
db_date_format = sys.argv[6]

# optional noupdate argument if user wants to test what an update would do
#   without affecting the database
# used values are "check", "verbose" and "check-verbose"
try: 
    noupdate = sys.argv[7]
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

# list of names that are used for rearrangement collection
rearrangement_names = ["sequence", "sequences", "rearrangement", "rearrangements"]

#function to check if a document insert or update date (if it exists) matches 
#  the provided date format, if not, report error
def check_document(doc):
    id = doc["_id"]

    global possible_update
    global had_warnings
    # the "added to database" field may or may not exist
    #  if it does not, make it null
    try:
        old_insert_date = doc[insert_date_field]
    except:
        old_insert_date = None

    try:
        old_update_date = doc[update_date_field]
    except:
        old_update_date = None

    if (old_insert_date is not None):
        try:
            old_insert_datetime = datetime.datetime.strptime(old_insert_date, db_date_format)
            new_insert_date = old_insert_datetime.isoformat()
            possible_update = True
            if (verbose == True):
                print ("Document ", id, "insert date", old_insert_date, "will change to", new_insert_date, 
                    flush=True)
        except:
            had_warnings = True
            if (verbose == True):
                print ("Document", id,"insert date",old_insert_date ,"does not match the format provided",
                    flush=True)
    else:
        had_warnings = True
        if (verbose == True):
            print ("Document", id,"does not have the insert field", insert_date_field, flush=True)

    new_update_date = datetime.datetime.now().isoformat()
    if (old_update_date is not None):
        try:
            old_update_datetime = datetime.datetime.strptime(old_update_date, db_date_format) 
            possible_update = True
            if (verbose == True):   
                print ("Document", id, "update date", old_update_date, "will change to ", new_update_date, 
                    flush=True)
        except:
            had_warnings = True
            if (verbose == True):            
                print ("Document ", id, "update date", old_update_date, "does not match the format provided" ,
                    flush=True)
    else:
        had_warnings = True
        if (verbose == True):
            print ("Document ", id,"does not have the update field", update_date_field, flush=True)

def updateDocument(doc, targetCollections):
    id = doc["_id"]
    global possible_update
    global had_warnings

    #we only want to do an update if either date needs its format changed
    has_update = False

    # the "added to database" field may or may not exist
    #  if it does not, make it null
    try:
        old_insert_date = doc[insert_date_field]
    except:
        old_insert_date = None
        if (verbose == True):
            print ("Document", id,"does not have the insert field ", insert_date_field, flush=True)

    try:
        old_update_date = doc[update_date_field]
    except:
        old_update_date = None

    # try to convert old insert date to new format. If it doesn't exist, or is null, keep it so
    #   otherwise, if conversion with supplied mask can be applied, convert it, otherwise, leave it unchanged
    if (old_insert_date is None):
        new_insert_date = None
    else:
        try:
            old_insert_datetime = datetime.datetime.strptime(old_insert_date, db_date_format)
            new_insert_date = old_insert_datetime.isoformat()
            has_update = True
        except:
            had_warnings = True
            new_insert_date = old_insert_date
            if (verbose == True):            
                print ("Document", id, "insert date", old_insert_date, "does not match the format provided" ,
                    flush=True)

    #we want to change update date only if the insert date has been updated
    if (has_update == True):
        new_update_date = datetime.datetime.now().isoformat()
    else:
        # if old update date has a mask and we haven't changed insert date, update it still
        try: 
            old_update_datetime = datetime.datetime.strptime(old_update_date, db_date_format)
            new_update_date = datetime.datetime.now().isoformat()
            has_update = True
        except:
            if (verbose == True):            
                print ("Document ", id, "update date", old_update_date, "does not match the format provided" ,
                    flush=True)
            new_update_date = old_update_date

    if (has_update):
        possible_update = True
        if (verbose == True):
            print ("Setting new values for _id", id, "insert date", new_insert_date, 
                "update_date", new_update_date, flush=True)
        db_cm.update({"_id": id},{"$set":{insert_date_field: new_insert_date, 
            update_date_field: new_update_date}})

# we want to iterate all the entries in a collection, except if
#  we are checking sequences for valid date format - because it would 
#  take too long, pick one, because date format should be consistent accross
#  the repository
record_count = 0
if (update == False and collection_name in rearrangement_names):
    record_list = db_cm.find().limit(1)
else: 
    record_list = db_cm.find()
if (verbose):
    print ("Starting the proces at ",datetime.datetime.now().isoformat(), flush=True )
for r in record_list:
    record_count +=1
    if (update == False):
        check_document(r)
    else:
        updateDocument(r,db_cm)
    if (record_count % 1000000 == 0 and verbose == True):
        print ("Processing record ", record_count, flush=True)
if (verbose):
    print ("Ended the process at ", datetime.datetime.now().isoformat(), flush=True)
#print out any warnings in verbose mode
if (had_warnings == True):
    print ("There were issues with some of the attempted updates", flush=True)
    if (verbose == True):
        print ("Please consult the program output for warnings", flush=True)
    else: 
        print ("Consider running the script with verbose or check-verbose option", flush=True)

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

