#!/usr/bin/python3

import pandas as pd
import json

class Sample:
	
	def __init__(self,context):
		self.context = context
		
	def insertDocument( self, doc ):
	
	    cursor = self.context.samples.find( {}, { "_id": 1 } ).sort("_id", -1).limit(1)
	    
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
	    
	    results = self.context.samples.insert(doc)
	
	def process(self):
	
                # Read in the CSV file
		df = pd.read_csv( self.context.path , sep=None, engine='python' )
		# Remove any records that are Unnamed
		df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
		# Add the sequence count field
		df['ir_sequence_count'] = 0
		# Conver to JSON
		records = json.loads(df.T.to_json()).values()
		record_list = list(records)
		
		# Iterate over the list and load records
		for r in record_list:
		    self.insertDocument( r )
	
		return True
