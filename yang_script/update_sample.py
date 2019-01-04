#!/usr/bin/env python

import pandas as pd
import pymongo
import json
import sys

mng_client = pymongo.MongoClient('localhost', 27017)
# Replace mongo db name
db_name = sys.argv[1]
mng_db = mng_client[db_name]
#  Replace mongo db collection name
collection_name = sys.argv[2]
db_cm = mng_db[collection_name]
total_found = 0

id_array = [None] * 1000 
find_ids = db_cm.find({})
for sample in list(find_ids):
  sample_id = sample["_id"]
  print (str(sample_id))
  id_array[sample_id] =  "Not Set"
print ("-----------")  
#filename = "master_metadata_20171031.csv"
filename = sys.argv[3]

def updateDocument(doc, targetCollections):
    global total_found
    global id_array
    file_name = doc["ir_rearrangement_file_name"]
    fasta_file_name =  doc["ir_fasta_file_name"]
    if not file_name: 
        return()
    result = db_cm.find({"$or":[{"mixcr_file_name": {"$regex": file_name}}, {"imgt_file_name": {"$regex": file_name}}, {"fasta_file_name": {"$regex": fasta_file_name}}]})
    result_count = db_cm.count_documents({"$or":[{"mixcr_file_name": {"$regex": file_name}}, {"imgt_file_name": {"$regex": file_name}}, {"fasta_file_name": {"$regex": fasta_file_name}}]})
    if result_count==1 :
        sample_id = result[0]["_id"]
        print (sample_id)
        total_found = total_found + 1
        id_array[sample_id]= "Set"
        db_cm.update({"_id": sample_id},{"$set":doc});
    else:
        print ("Query found " + str(result_count) + " results " + " for number " + str(doc["ir_rearrangement_number"]))


df = pd.read_csv(filename,sep=None,engine='python')
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
records = json.loads(df.T.to_json()).values()
record_list = list(records)
for r in record_list:
    updateDocument(r,db_cm)
print ("Found " + str(total_found) + " samples")

for key, value in enumerate(id_array):
    if value == "Not Set":
       print ("Not found " + str(key))

