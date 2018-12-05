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
		# Remove any records that are Unnamed. Note: This occurs when a 
		# Pandas dataframe has a column without a name. In general, this 
		# should not occur and it should probably be detected as an error or
		# at least a warning given.
		if (df.columns.str.contains('^Unnamed').any()):
			print("Warning: column without a title detected in file ", self.context.path)	
		df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

		# Do some sanity checking
		# First, report on any missing repertoire fields that are missing from the file.
		for repertoire_field in self.context.airr_map.airr_repertoire_map['airr']:
			if not repertoire_field in df.columns:
				print("Warning: Could not find MiAIRR repertoire field %s in the file, will be missing from the repository." % (repertoire_field))

		# Next, report on any extra repertoire fields that are not in the MiAIRR standard.
		for repertoire_field in df.columns:
			if not self.context.airr_map.airr_repertoire_map['airr'].str.contains(repertoire_field).any():
				print("Warning: Non MiAIRR field %s found, will be added to the repository." % (repertoire_field))

		# Add the sequence count field
		df['ir_sequence_count'] = 0
		# Conver to JSON
		records = json.loads(df.T.to_json()).values()
		record_list = list(records)
		
		# Iterate over the list and load records. Note that this code inserts all data
		# that was read in the CSV file. That is, all of the non MiAIRR fileds that exist
		# are stored in the repository. So if the provided CSV file has lots of extra fields
		# they will exist in the repository.
		for r in record_list:
		    self.insertDocument( r )
	
		return True
