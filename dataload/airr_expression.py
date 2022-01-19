# Script for loading MIXCR formatted annotation file 
# into an iReceptor data node MongoDb database

import sys
import os.path
import pandas as pd
import numpy as np 
import json
import gzip
import time

from expression import Expression
from annotation import Annotation
from parser import Parser

class AIRR_Expression(Expression):
    
    def __init__( self, verbose, repository_tag, repository_chunk, airr_map, repository):
        Expression.__init__(self, verbose, repository_tag, repository_chunk, airr_map, repository)
        # The annotation tool used for the AIRR-Expression is ambiguous, use a generic name.
        self.setAnnotationTool("AIRR-Expression")
        # The default column in the AIRR Mapping file is "airr" as this is parsing AIRR
        # fields.. This can be overrideen by the user should they choose to use a
        # differnt set of columns from the file.
        self.setFileMapping("airr")

    def process(self, filewithpath):

        # This reads one AIRR Expression JSON file at a time, given the full file (path) name
        # May also be gzip compressed file
        
        # Open, decompress then read(), if it is a gz archive
        success = True

        # Check to see if the file exists and return if not.
        if not os.path.isfile(filewithpath):
            print("ERROR: Could not open AIRR Expression file ", filewithpath)
            return False

        # Get root filename from the path, should be a file if the path is file,
        # so not checking again 8-)
        filename = os.path.basename(filewithpath)

        if filewithpath.endswith(".gz"):
            if self.verbose():
                print("Info: Reading data gzip archive: "+filewithpath)
            with gzip.open(filewithpath, 'rb') as file_handle:
                # read file directly from the file handle 
                # (Pandas read_csv call handles this...)
                success = self.processAIRRExpressionFile(file_handle, filename)

        else: # read directly as a regular text file
            if self.verbose():
                print("Info: Reading text file: "+filewithpath)
            file_handle = open(filewithpath, "r")
            success = self.processAIRRExpressionFile(file_handle, filename)

        return success

    def processAIRRExpressionFile( self, file_handle, filename ):

        # Start a timer for performance reasons.
        t_start_full = time.perf_counter()

        # Get the AIRR Map object for this class (for convenience).
        airr_map = self.getAIRRMap()

        # Set the tag for the repository that we are using.
        repository_tag = self.getRepositoryTag()
        # Get the tag to use for iReceptor specific mappings
        ireceptor_tag = self.getiReceptorTag()
        # Set the tag for the AIRR column
        airr_tag = self.getAIRRTag()

        # Get the fields to use for finding repertoire IDs, either using those IDs
        # directly or by looking for a repertoire ID based on a rearrangement file
        # name.
        repertoire_link_field = self.getRepertoireLinkIDField()
        expression_link_field = self.getAnnotationLinkIDField()

        # Set the tag for the file mapping that we are using. Ths is essentially the
        # look up into the columns of the AIRR Mapping that we are using. 
        filemap_tag = self.getFileMapping()

        # Define the number of records to iterate over
        chunk_size = self.getRepositoryChunkSize()

        # Get the single, unique repertoire link id for the filename we are loading. If
        # we can't find one, this is an error and we return failure.
        repertoire_link_id = self.getRepertoireInfo(filename)
        if repertoire_link_id is None:
            print("ERROR: Could not link file %s to a valid repertoire"%(filename))
            return False

        # Get the column of values from the AIRR tag. We only want the
        # Expression related fields.
        map_column = self.getAIRRMap().getIRExpressionMapColumn(airr_tag)
        # Get a boolean column that flags columns of interest. Exclude nulls.
        fields_of_interest = map_column.notnull()
        # Afer the following airr_fields contains N columns (e.g. iReceptor, AIRR)
        # that contain the AIRR Repertoire mappings.
        airr_fields = self.getAIRRMap().getIRExpressionRows(fields_of_interest)

        # Extract the fields that are of interest for this file. Essentially all non
        # null fields in the file. This is a boolean array that is T everywhere there
        # is a notnull field in the column of interest.
        map_column = airr_map.getIRExpressionMapColumn(filemap_tag)
        fields_of_interest = map_column.notnull()

        # We select the rows in the mapping that contain fields of interest for Expression.
        # At this point, file_fields contains N columns that contain our mappings for
        # the specific formats (e.g. airr). The rows are limited to 
        # only data that is relevant to Expression
        file_fields = airr_map.getIRExpressionRows(fields_of_interest)

        # We need to build the set of fields that the repository can store. We don't
        # want to extract fields that the repository doesn't want.
        expressionColumns = []
        columnMapping = {}
        if self.verbose():
            print("Info: Dumping expected %s (%s) to repository mapping"
                  %(self.getAnnotationTool(),filemap_tag))
        for index, row in file_fields.iterrows():
            if self.verbose():
                print("Info:    %s -> %s"
                      %(str(row[filemap_tag]), str(row[repository_tag])))
            # If the repository column has a value for the Expression field, track the field
            # from both the Expression and repository side.
            if not pd.isnull(row[repository_tag]):
                expressionColumns.append(row[filemap_tag])
                columnMapping[row[filemap_tag]] = row[repository_tag]
            else:
                if self.verbose():
                    print("Info:    Repository does not support " +
                          str(row[filemap_tag]) + ", not inserting into repository")

        # Load in the JSON file. The file should be an array of expression objects as
        # per the AIRR spec.
        if self.verbose():
            print("Info: Reading the Expression JSON array", flush=True)
        expression_array = json.load(file_handle)
        if self.verbose():
            print("Info: Read %d Expression objects"%(len(expression_array)), flush=True)

        # Iterate over each element in the array 
        total_records = 0
        for expression_dict in expression_array:

            if self.verbose():
                print("Info: Processing raw data frame...", flush=True)
            # Remap the column names. We need to remap because the columns may be in 
            # a different order in the file than in the column mapping. We leave any
            # non-mapped columns in the data frame as we don't want to discard data.
            add_dict = dict() 
            del_dict = dict()
            for expression_key, expression_value in expression_dict.items():
                if expression_key in columnMapping:
                    mongo_column = columnMapping[expression_key]
                    if self.verbose():
                        print("Info: Mapping %s field in file: %s -> %s"
                              %(self.getAnnotationTool(), expression_key, mongo_column))
                    # If they are different swap them.
                    if mongo_column != expression_key:
                        add_dict[mongo_column] = expression_value
                        del_dict[expression_key] = True
                else:
                    if self.verbose():
                        print("Info: No mapping for %s column %s, storing as is"
                              %(self.getAnnotationTool(), expression_key))

            for add_key, add_value in add_dict.items():
                expression_dict[add_key] = add_value
                if self.verbose():
                    print("Info: Adding %s -> %s"%(add_key, add_value))
            for del_key in del_dict:
                del expression_dict[del_key]
                if self.verbose():
                    print("Info: Removing %s "%(del_key))
            # Check to see which desired Expression mappings we don't have in the file...
            for expression_column, mongo_column in columnMapping.items():
                if not mongo_column in expression_dict:
                    if self.verbose():
                        print("Info: Missing data in input %s file for %s"
                              %(self.getAnnotationTool(), expression_column))
            

            rep_expression_link_field = airr_map.getMapping(
                                             expression_link_field,
                                             ireceptor_tag, repository_tag)
            if not rep_expression_link_field is None:
                expression_dict[rep_expression_link_field] = repertoire_link_id
            else:
                print("ERROR: Could not get repertoire link field from AIRR mapping.")
                return False

            # Set the relevant IDs for the record being inserted. It updates the dictionary
            # (passed by reference) and returns False if it fails. If it fails, don't
            # load any data.
            if (not self.checkIDFieldsJSON(expression_dict, repertoire_link_id)):
                return False

            # Check to make sure all AIRR required columns exist
            #####if not self.checkAIRRRequired(df_chunk, airr_fields):
            #####    return False

            # Create the created and update values for this block of records. Note that
            # this means that each block of inserts will have the same date.
            now_str = self.getDateTimeNowUTC()
            ir_created_at = airr_map.getMapping("ir_created_at", 
                                                ireceptor_tag, repository_tag)
            ir_updated_at = airr_map.getMapping("ir_updated_at",
                                                ireceptor_tag, repository_tag)
            expression_dict[ir_created_at] = now_str
            expression_dict[ir_updated_at] = now_str

            # Transform the data frame so that it meets the repository type requirements
            ####if not self.mapToRepositoryType(df_chunk,
            ####                                airr_map.getRearrangementClass(),
            ####                                airr_map.getIRRearrangementClass()):
            ####    print("ERROR: Unable to map data to the repository")
            ####    return False

            # Insert the chunk of records into Mongo.
            ####num_records = len(df_chunk)
            ####print("Info: Inserting", num_records, "records into Mongo...", flush=True)
            t_start = time.perf_counter()
            ####records = json.loads(df_chunk.T.to_json()).values()
            ####self.repositoryInsertRecords(records)
            self.repositoryInsertRecords(expression_dict)
            t_end = time.perf_counter()
            print("Info: Inserted records, time =", (t_end - t_start),
                  "seconds", flush=True)

            # Keep track of the total number of records processed.
            ####total_records = total_records + num_records
            total_records = total_records + 1
            print("Info: Total records so far =", total_records, flush=True)

        # Get the number of annotations for this repertoire 
        if self.verbose():
            print("Info: Getting the number of annotations for this repertoire")
        annotation_count = self.repositoryCountRecords(repertoire_link_id)
        if annotation_count == -1:
            print("ERROR: invalid annotation count (%d), write failed." %
                  (annotation_count))
            return False

        # Set the cached expression count field for the repertoire/sample.
        if not self.repositoryUpdateCount(repertoire_link_id, annotation_count):
            print("ERROR: Unable to write expression count to repository.")
            return False

        # Inform on what we added and the total count for the this record.
        t_end_full = time.perf_counter()
        print("Info: Inserted %d records, annotation count = %d, %f s, %f insertions/s" %
              (total_records, annotation_count, t_end_full - t_start_full,
              total_records/(t_end_full - t_start_full)), flush=True)

        return True
        
