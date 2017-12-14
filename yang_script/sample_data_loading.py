import pandas as pd
import pymongo
import json

mng_client = pymongo.MongoClient('localhost', 27017)
# Replace mongo db name
mng_db = mng_client['mydb']
#  Replace mongo db collection name
collection_name = 'sample' 
db_cm = mng_db[collection_name]

def insertDocument(doc, targetCollections):
    cursor = db_cm.find( {}, { "_id": 1 } ).sort("_id", -1).limit(1)
    empty = False
    try:
        record = cursor.next()
    except StopIteration:
        print("Warning! NO PREVIOUS RECORD, THIS IS THE FIRST TIME INSERTING ")
        empty = True
    if empty:
        seq = 1
    else:
        seq = record["_id"]+1
    doc["_id"] = seq
    results = targetCollections.insert(doc)
    
filename = "master_metadata_20171031.csv"
df = pd.read_csv(filename,sep=None)
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
df['ir_sequence_count'] = 0
records = json.loads(df.T.to_json()).values()
record_list = list(records)
# db_cm.insert(records)
for r in record_list:
    insertDocument(r,db_cm)