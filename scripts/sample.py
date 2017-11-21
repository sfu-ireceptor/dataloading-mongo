#!/usr/bin/python3

import pandas as pd
import json

class Sample:
	
	def __init__(self,context):
		self.path = context.path
		self.collection = context.collection
		
	def insertDocument( doc ):
	
	    cursor = collection.find( {}, { "_id": 1 } ).sort("_id", -1).limit(1)
	    
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
	    
	    results = collection.insert(doc)
	
	def process():
	
		df = pd.read_csv( path , sep=None )
		
		# Yang: there is an extra field with the same name library_source
		# if bojan delete that field, I need to change this code
		# df = df.drop('library_source.1', axis=1)"
		
		df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
		
		df['ir_sequence_count'] = 0
		
		records = json.loads(df.T.to_json()).values()
		record_list = list(records)
		
		# .. then load records
		for r in record_list:
		    insertDocument( r )
	
		return True
