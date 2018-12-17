#!/usr/bin/python3

import pandas as pd
import json
from datetime import datetime
from datetime import timezone
from parser import Parser

class Sample(Parser):
	
	def __init__(self,context):
		self.context = context
		Parser.__init__(self, context)
		
	def insertDocument( self, doc ):
	
	    cursor = self.context.samples.find( {}, { "_id": 1 } ).sort("_id", -1).limit(1)
	    
	    empty = False
	    
	    try:
	        record = cursor.next()
	        
	    except StopIteration:
	        print("Info: No previous record, this is the first insertion")
	        empty = True
	        
	    if empty:
	        seq = 1
	    else:
	        seq = record["_id"]
	        if not type(seq) is int:
	            print("ERROR: Invalid ID for samples found, expecting an integer, got " + str(seq))
	            print("ERROR: DB may be corrupt")
	            return False
	        else:
	            seq = seq+1
	        
	    doc["_id"] = seq
	    if self.context.verbose:
	        # If we are in verbose mode, print out a summary of the record we are inserting.
	        study_tag = self.context.airr_map.getMapping("study_id", "ir_id", self.context.repository_tag)
	        study = "NULL" if not study_tag in doc else doc[study_tag]
	        sample_tag = self.context.airr_map.getMapping("sample_id", "ir_id", self.context.repository_tag)
	        sample = "NULL" if not sample_tag in doc else doc[sample_tag]
	        file_tag = self.context.airr_map.getMapping("ir_rearrangement_file_name", "ir_id", self.context.repository_tag)
	        filestr = "NULL" if not file_tag in doc else doc[file_tag]
	        print("Info: Writing repertoire record <%s, %s, %s (ID: %d)>" % (study, sample, filestr, seq))

	    
	    results = self.context.samples.insert(doc)
	
	def process(self):
	
		# Set the tag for the repository that we are using.
		repository_tag = self.context.repository_tag

		# Extract the fields that are of interest for this file. Essentiall all
		# non null curator fields
		curation_tag = "ir_curator"
		if not curation_tag in self.context.airr_map.airr_repertoire_map:
			print("ERROR: Could not find Curation mapping (" + curation_tag + ") in mapping file")
			return False

		field_of_interest = self.context.airr_map.airr_repertoire_map[curation_tag].notnull()
		
		# We select the rows in the mapping that contain fields of interest for curataion.
		# At this point, file_fields contains N columns that contain our mappings for the
		# the specific formats (e.g. ir_id, airr, vquest). The rows are limited to have
		# only data that is relevant to curataion
		airr_fields = self.context.airr_map.airr_repertoire_map.loc[field_of_interest]
		
		# We need to build the set of fields that the repository can store. We don't
		# want to extract fields that the repository doesn't want.
		curationColumns = []
		columnMapping = {}

		if self.context.verbose:
			print("Info: Dumping AIRR repertoire mapping")
		for index, row in airr_fields.iterrows():
			if self.context.verbose:
				print("    " + str(row[curation_tag]) + " -> " + str(row[repository_tag]))
			# If the repository column has a value for the curator field, track the field
			# from both the curator and repository side.
			if not pd.isnull(row[repository_tag]):
				curationColumns.append(row[curation_tag])
				columnMapping[row[curation_tag]] = row[repository_tag]
			else:
				print("Repository does not map " +
					str(row[curation_tag]) + ", inserting into repository as is")

                # Read in the CSV file
		df = pd.read_csv( self.context.path , sep=None, engine='python' )
		# Remove any records that are Unnamed. Note: This occurs when a 
		# Pandas dataframe has a column without a name. In general, this 
		# should not occur and it should probably be detected as an error or
		# at least a warning given.
		if (df.columns.str.contains('^Unnamed').any()):
			print("Warning: column without a title detected in file ", self.context.path)	
		df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

		# Check the validity of the columns in the actual file being loaded.
		for curation_file_column in df.columns:
			if curation_file_column in columnMapping:
				# If the file column is in the mapping, change the data frame column name
				# to be the column name for the repository.
				mongo_column = columnMapping[curation_file_column]
				if self.context.verbose:
					print("Info: Mapping input file column " + curation_file_column + " -> " + mongo_column)
				df.rename({curation_file_column:mongo_column}, axis='columns', inplace=True)
			else:
				# If we don't have a mapping, keep the name the same, as we want to
				# still save the data even though we don't have a mapping.
				if self.context.verbose:
					print("Info: No mapping for input file column " + curation_file_column + ", storing in repository as is")
		# Check to see which desired Curation mappings we don't have... We check this
		# against the "mongo_column" from the repository in the data frame, because
		# we have already mapped the columns from the file columns to the repository columns.
		for curation_column, mongo_column in columnMapping.items():
			if not mongo_column in df.columns:
				if self.context.verbose:
					print("Warning: Missing data in input file for " + curation_column)

		# Get the mapping for the sequence count field for the repository and 
		# initialize the sequeunce count to 0. If we can't find a mapping for this
		# field then we can't do anything. 
		count_field = self.context.airr_map.getMapping("ir_sequence_count", "ir_id", repository_tag)
		if count_field is None:
			print("Warning: Could not find ir_sequence_count tag in repository " + repository_tag + ", field not initialized")
		else:
			df[count_field] = 0

		# Ensure that we have a correct file name to link fields. If not return.
		# This is a fatal error as we can not link any data to this set of samples,
		# so there is no point adding the samples...
		repository_file_field = self.context.airr_map.getMapping("ir_rearrangement_file_name", "ir_id", repository_tag)
		# If we can't find a mapping for this field in the repository mapping, then
		# we might still be OK if the metadata spreadsheet has the field. If the fails, 
		# then we should exit.
		if repository_file_field is None or len(repository_file_field) == 0:
			print("Warning: Could not find a valid repository mapping for the rearrangement file name (ir_rearrangement_file_name)")
			repository_file_field = "ir_rearrangement_file_name"

		# If we can't find the file field for the rearrangement field in the repository, then
		# abort, as we won't be able to link the repertoire to the rearrangement.
		if not repository_file_field in df.columns:
			print("ERROR: Could not find a rearrangement file field in the metadata (ir_rearrangement_file_name)")
			print("ERROR: Will not be able to link repertoire to rearrangement annotations")
			df["ir_rearrangment_file_name"] = ""
			return False

		# Add a created_at and updated_at field in the repository.
		now_str = Parser.getDateTimeNowUTC()
		df["ir_created_at"] = now_str
		df["ir_updated_at"] = now_str

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
