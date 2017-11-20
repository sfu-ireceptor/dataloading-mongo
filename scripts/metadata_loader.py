#!/usr/bin/python3

import pandas as pd
import pymongo
import json
import optparse

# Default host
dbHost = 'localhost'

# Default database
dbName = 'ireceptor'

# Default collection
dbCollection = 'sample'

# Default connection
dbQuery = None

def inputParameters():

	parser = optparse.OptionParser()
	
	parser.add_option('-i', '--input', 
	                  dest="inputFileName", 
	                  default="data.csv",
	                  )
	                  
	parser.add_option('-v', '--verbose',
	                  dest="verbose",
	                  default=False,
	                  action="store_true",
	                  )
	                  
	parser.add_option('--version',
	                  dest="version",
	                  default=1.0,
	                  type="float",
	                  )
	                  
	options, remainder = parser.parse_args()
	
	print('VERSION   :', options.version)
	print('VERBOSE   :', options.verbose)
	print('OUTPUT    :', options.inputFileName)
	print('REMAINING :', remainder)
	
	return options

def dbConnect():

	# Connect with Mongo db
	mng_client = pymongo.MongoClient(dbHost, 27017)
	
	# Set Mongo db name
	mng_db = mng_client[dbName]
	
	# Set Mongo db collection name
	dbQuery = mng_db[dbCollection]
	
def insertDocument(doc, targetCollections):

    cursor = dbQuery.find( {}, { "_id": 1 } ).sort("_id", -1).limit(1)
    empty = False
    try:
        record = cursor.next()
    except StopIteration:
        print("Warning! NO PREVIOUS RECORD, THIS IS THE FIRST INSERTION")
        empty = True
    if empty:
        seq = 1
    else:
        seq = record["_id"]+1
    doc["_id"] = seq
    results = targetCollections.insert(doc)

from os.path import exists

def process(options):

	if not options.inputFileName or \
           not exists(options.inputFileName): 
	    return False 
	
	df = pd.read_csv( options.inputFileName, sep=None )
	
	# Yang: there is an extra field with the same name library_source
	# if bojan delete that field, I need to change this code
	# df = df.drop('library_source.1', axis=1)"
	
	df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
	
	df['ir_sequence_count'] = 0
	
	records = json.loads(df.T.to_json()).values()
	record_list = list(records)
	
	# Connect with the database...
	dbConnect()
	
	# .. then load records
	for r in record_list:
	    insertDocument(r,dbQuery)

	return True

if __name__ == "__main__":

    if process(inputParameters()):
        print("Input file loaded")
    else:
        print("Input file not found?")
