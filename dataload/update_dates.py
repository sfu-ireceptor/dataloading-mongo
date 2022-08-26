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
db_date_format == sys.argv[6]

#filename = "master_metadata_20171031.csv"

def updateDocument(doc, targetCollections):
    id = doc["_id"]

    # the "added to database" field may or may not exist
    #  if it does not, make it null
    try:
        old_insert_date = doc[insert_date_field]
    except:
        old_insert_date = None

    if (old_insert_date is None):
        new_insert_date = None
    else:
        old_insert_datetime = datetime.datetime(old_insert_date, db_date_format)
        new_insert_date = old_insert_date.isoformat()

    new_update_date = datetime.datetime.now().isoformat()

    #print (id, ": ", old_insert_date ," to " , new_insert_date, " |  to ", new_update_date, "\n")
    db_cm.update({"_id": id},{"$set":{insert_date_field: new_insert_date, update_date_field: new_update_date}})

record_count = 0
record_list = db_cm.find();
for r in record_list:
    record_count +=1
    updateDocument(r,db_cm)
    if (record_count % 1000000 == 0):
        print ("Processing record ", record_count)
print (datetime.datetime.now().isoformat())
