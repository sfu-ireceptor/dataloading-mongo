#!/usr/bin/env python

import pandas as pd
import pymongo
import json
import sys

#script to update a samples collection in mongodb from curator file
#  parameters (all mandatory)
#  database_name - db that collection is in
#  collection_name - name of the collections (samples, most of the time)
#  file_name - name of the CSV file that has the sample-level metadata

mng_client = pymongo.MongoClient('localhost', 27017)
# Replace mongo db name
db_name = sys.argv[1]
mng_db = mng_client[db_name]
#  Replace mongo db collection name
collection_name = sys.argv[2]
db_cm = mng_db[collection_name]

#filename = "master_metadata_20171031.csv"
filename = sys.argv[3]

def updateDocument(doc, targetCollections):
    rr_number = doc["ir_rearrangement_number"]
    result = db_cm.find({"ir_rearrangement_number": rr_number})
    result_count = db_cm.count_documents({"ir_rearrangement_number": rr_number})
    if result_count==1 :
        sample_id = result[0]["_id"]
        print (str(sample_id) )
        db_cm.update({"_id": sample_id},{"$set":doc});
    else:
        print ("Query found " + str(result_count) + " results.")
df = pd.read_csv(filename,sep=None,engine='python')
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
records = json.loads(df.T.to_json()).values()
record_list = list(records)
# db_cm.insert(records)
for r in record_list:
    updateDocument(r,db_cm)