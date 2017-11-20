#!/usr/bin/python3

import os
from os.path import exists

import pandas as pd
import pymongo
import json
import optparse

# Default collection
targetCollection = 'sample'

def inputParameters():

	parser = optparse.OptionParser()
	
	default_host =  os.environ.get('MONGODB_HOST', 'localhost')
	
	parser.add_option('--host', 
	                  dest="host", 
	                  default=default_host,
	                  help="MongoDb server hostname. If the MONGODB_HOST environment variable is set, it is used. Defaults to 'localhost' otherwise."
	                  )
	
	parser.add_option('--port', 
	                  dest="port", 
	                  default=27017,
	                  type="int",
	                  help="MongoDb server port number. Defaults to 27017."
	                  
	                  )
	
	default_user =  os.environ.get('MONGODB_SERVICE_USER', 'admin')
	
	parser.add_option('-u', '--user',
	                  dest="user", 
	                  default=default_user,
	                  help="MongoDb service user name. Defaults to the MONGODB_SERVICE_USER environment variable if set. Defaults to 'admin' otherwise."
	                  )
	     
	default_password =  os.environ.get('MONGODB_SERVICE_SECRET', '')
	    
	parser.add_option('-p', '--password', 
	                  dest="password", 
	                  default=default_password,
	                  help="MongoDb service user account secret ('password'). Defaults to the MONGODB_SERVICE_SECRET environment variable if set. Defaults to empty string otherwise."
	                  )
	
	default_database = os.environ.get('MONGODB_DB', 'ireceptor')
	
	parser.add_option('-d', '--database', 
	                  dest="database", 
	                  default=default_database,
	                  help="Target MongoDb database. Defaults to the MONGODB_DB environment variable if set. Defaults to 'ireceptor' otherwise." 
	                  )
	                  
	parser.add_option('-c', '--collection', 
	                  dest="collection", 
	                  default='sample',
	                  help="MongoDb collection name. Defaults to 'sample'." 
	                  )
	                  
	parser.add_option('-l', '--library', 
	                  dest="library", 
	                  default=".",
	                  help="Path to 'library' directory of data files. Defaults to the current working directory."
	                  )
	                  
	parser.add_option('-f', '--filename', 
	                  dest="filename", 
	                  default="metadata.csv",
	                  help="Name of file to load. Defaults to 'metadata.csv'."
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
	
	if options.verbose:
		print('HOST      :', options.host)
		print('USER      :', options.user)
		print('PORT      :', options.port)
		print('PASSWORD  :', options.password)
		print('DATABASE  :', options.database)
		print('COLLECTION:', options.collection)
		print('LIBRARY   :', options.library)
		print('FILENAME  :', options.filename)
		print('VERSION   :', options.version)
		print('VERBOSE   :', options.verbose)
		#print('REMAINING :', remainder)
	
	return options

def getDbCollection(options):
         
	# Connect with Mongo db
	mng_client = pymongo.MongoClient(
		options.host, 
		options.port, 
		user=options.user, 
		password=options.password
	)
	
	# Set Mongo db name
	mng_db = mng_client[options.database]
	
	# Set Mongo db collection name
	dbCollection = mng_db[options.collection]
	
	return dbCollection

def insertDocument(doc, dbCollection):

    cursor = dbCollection.find( {}, { "_id": 1 } ).sort("_id", -1).limit(1)
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
    results = dbCollection.insert(doc)

def process(options):

	if not options.filename: 
	    return False
	   
	if options.library:
		path = options.library+"/"+options.filename
	else:
		path = filename
	
	if not exists(path): 
	    return False 

	df = pd.read_csv( path, sep=None )
	
	# Yang: there is an extra field with the same name library_source
	# if bojan delete that field, I need to change this code
	# df = df.drop('library_source.1', axis=1)"
	
	df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
	
	df['ir_sequence_count'] = 0
	
	records = json.loads(df.T.to_json()).values()
	record_list = list(records)
	
	# Connect with the database...
	dbCollection = getDbCollection(options)
	
	# .. then load records
	for r in record_list:
	    insertDocument(r,dbCollection)

	return True

if __name__ == "__main__":

    if process(inputParameters()):
        print("Input file loaded")
    else:
        print("Input file not found?")
