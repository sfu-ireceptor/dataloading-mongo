# Script for loading AIRR formatted reactivity file 
# into an iReceptor MongoDb database

import sys
import os.path
import pandas as pd
import numpy as np 
import json
import gzip
import time

from reactivity import Reactivity
from annotation import Annotation
from parser import Parser

class AIRR_Reactivity(Reactivity):
    
    def __init__( self, verbose, repository_tag, repository_chunk, airr_map, repository):
        Reactivity.__init__(self, verbose, repository_tag, repository_chunk, airr_map, repository)
        # The annotation tool used for the AIRR-Reactivity is ambiguous, use a generic name.
        self.setAnnotationTool("AIRR-Reactivity")
        # The default column in the AIRR Mapping file is "airr" as this is parsing AIRR
        # fields.. This can be overrideen by the user should they choose to use a
        # differnt set of columns from the file.
        self.setFileMapping("airr")

    def process(self, filewithpath):

        # This reads one AIRR Reactivity JSON file at a time, given the full file (path) name
        # May also be gzip compressed file
        
        # Open, decompress then read(), if it is a gz archive
        success = True

        # Check to see if the file exists and return if not.
        if not os.path.isfile(filewithpath):
            print("ERROR: Could not open AIRR Reactivity file ", filewithpath)
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
                success = self.processAIRRReactivityFile(file_handle, filename)

        else: # read directly as a regular text file
            if self.verbose():
                print("Info: Reading text file: "+filewithpath)
            file_handle = open(filewithpath, "r")
            success = self.processAIRRReactivityFile(file_handle, filename)

        return success

    def processAIRRReactivityFile( self, file_handle, filename ):

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
        reactivity_link_field = self.getAnnotationLinkIDField()

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

        # Look up the repertoire data for the record of interest. This is an array
        # and it should be of length 1
        repertoires = self.repository.getRepertoires(repertoire_link_field,
                                                     repertoire_link_id)
        if not len(repertoires) == 1:
            print("ERROR: Could not find unique repertoire for id %s"%(repertoire_link_id))
            return False
        repertoire = repertoires[0]
        
        # Get mapping of the ID fields we want to generate.
        map_class = self.getAIRRMap().getRepertoireClass()
        rep_id_field = self.getAIRRRepositoryField("repertoire_id", map_class)
        data_id_field = self.getAIRRRepositoryField("data_processing_id", map_class)
        sample_id_field = self.getAIRRRepositoryField("sample_processing_id", map_class)

        # Cache some data we need to use often.
        if rep_id_field in repertoire:
            repertoire_id_value = repertoire[rep_id_field]
        else:
            repertoire_id_value = None

        if data_id_field in repertoire:
            data_processing_id_value = repertoire[data_id_field]
        else:
            data_processing_id_value = None

        if sample_id_field in repertoire:
            sample_processing_id_value = repertoire[sample_id_field]
        else:
            sample_processing_id_value = None

        # Get the column of values from the AIRR tag. We only want the
        # Reactivity related fields.
        map_column = self.getAIRRMap().getIRReactivityMapColumn(airr_tag)
        # Get a boolean column that flags columns of interest. Exclude nulls.
        fields_of_interest = map_column.notnull()
        # Afer the following airr_fields contains N columns (e.g. iReceptor, AIRR)
        # that contain the AIRR Repertoire mappings.
        airr_fields = self.getAIRRMap().getIRReactivityRows(fields_of_interest)

        # Extract the fields that are of interest for this file. Essentially all non
        # null fields in the file. This is a boolean array that is T everywhere there
        # is a notnull field in the column of interest.
        map_column = airr_map.getIRReactivityMapColumn(filemap_tag)
        fields_of_interest = map_column.notnull()

        # We select the rows in the mapping that contain fields of interest for Reactivity.
        # At this point, file_fields contains N columns that contain our mappings for
        # the specific formats (e.g. airr). The rows are limited to 
        # only data that is relevant to Reactivity
        file_fields = airr_map.getIRReactivityRows(fields_of_interest)

        # We need to build the set of fields that the repository can store. We don't
        # want to extract fields that the repository doesn't want.
        reactivityColumns = []
        columnMapping = {}
        if self.verbose():
            print("Info: Dumping expected %s (%s) to repository mapping"
                  %(self.getAnnotationTool(),filemap_tag))
        for index, row in file_fields.iterrows():
            if self.verbose():
                print("Info:    %s -> %s"
                      %(str(row[filemap_tag]), str(row[repository_tag])))
            # If the repository column has a value for the Reactivity field, track the field
            # from both the Reactivity and repository side.
            if not pd.isnull(row[repository_tag]):
                reactivityColumns.append(row[filemap_tag])
                columnMapping[row[filemap_tag]] = row[repository_tag]
            else:
                if self.verbose():
                    print("Info:    Repository does not support " +
                          str(row[filemap_tag]) + ", not inserting into repository")

        # Load in the JSON file. The file should be an array of Reactivity objects as
        # per the AIRR spec.
        if self.verbose():
            print("Info: Reading the Reactivity JSON array", flush=True)
        try:
            reactivity_array = json.load(file_handle)
        except json.JSONDecodeError as error:
            print("ERROR: %s"%(error))
            print("ERROR: Invalid JSON in file %s"%(filename))
            return False
        except Exception as error:
            print("ERROR: %s"%(error))
            return False

        if self.verbose():
            print("Info: Read %d Reactivity objects"%(len(reactivity_array)), flush=True)

        # Iterate over each element in the array 
        total_records = 0
        for reactivity_dict in reactivity_array:
            # Remap the column names. We need to remap because the columns may be in 
            # a different order in the file than in the column mapping. We leave any
            # non-mapped columns in the data frame as we don't want to discard data.
            add_dict = dict() 
            del_dict = dict()
            for reactivity_key, reactivity_value in reactivity_dict.items():
                if reactivity_key in columnMapping:
                    mongo_column = columnMapping[reactivity_key]
                    if self.verbose() and total_records == 0:
                        print("Info: Mapping %s field in file: %s -> %s"
                              %(self.getAnnotationTool(), reactivity_key, mongo_column))
                    # If they are different swap them.
                    if mongo_column != reactivity_key:
                        add_dict[mongo_column] = reactivity_value
                        del_dict[reactivity_key] = True
                else:
                    if self.verbose() and total_records == 0:
                        print("Info: No mapping for %s column %s, storing as is"
                              %(self.getAnnotationTool(), reactivity_key))
            # Add any key value pairs in the add_dict to the reactivity_dict. These are mapped
            # columns that changed from the file field to the repository field.
            for add_key, add_value in add_dict.items():
                reactivity_dict[add_key] = add_value
                if self.verbose() and total_records == 0:
                    print("Info: Adding %s -> %s"%(add_key, add_value))
            # Remove any key value pairs  that are in the delete_dict. These are the keys 
            # that changed name between the file and the repository. We store these through
            # add_dict so don't want them twice.
            for del_key in del_dict:
                del reactivity_dict[del_key]
                if self.verbose() and total_records == 0:
                    print("Info: Removing %s "%(del_key))
            # Check to see which desired Reactivity mappings we don't have in the file...
            for reactivity_column, mongo_column in columnMapping.items():
                if not mongo_column in reactivity_dict:
                    if self.verbose() and total_records == 0:
                        print("Info: Missing data in input %s file for %s"
                              %(self.getAnnotationTool(), reactivity_column))
            
            # Get the all important link field that maps repertoires to receptors.
            rep_reactivity_link_field = airr_map.getMapping(
                                             reactivity_link_field,
                                             ireceptor_tag, repository_tag)
            if not rep_reactivity_link_field is None:
                reactivity_dict[rep_reactivity_link_field] = repertoire_link_id
            else:
                print("ERROR: Could not get repertoire link field from AIRR mapping.")
                return False

            # Get the field names for the cell_id. When loading we want to copy the
            # cell_id field from the AIRR Standard into a annotation tool specific
            # cell id for the ADC. We don't want to lose the original barcode.
            airr_cell_id = airr_map.getMapping("cell_id_reactivity",
                                                ireceptor_tag, repository_tag,
                                                airr_map.getReactivityClass())
            ir_cell_id = airr_map.getMapping("ir_cell_id_reactivity",
                                             ireceptor_tag, repository_tag,
                                             airr_map.getIRReactivityClass())
            if airr_cell_id in reactivity_dict:
                reactivity_dict[ir_cell_id] = reactivity_dict[airr_cell_id]


            # Set the relevant IDs for the record being inserted. It updates the dictionary
            # (passed by reference) and returns False if it fails. If it fails, don't
            # load any data.
            if (not self.checkIDFieldsJSON(reactivity_dict,
                                           repertoire_link_field, repertoire_link_id,
                                           repertoire_id_value,
                                           data_processing_id_value,
                                           sample_processing_id_value)):
                return False

            # Create the created and update values for this block of records. Note that
            # this means that each block of inserts will have the same date.
            now_str = self.getDateTimeNowUTC()
            ir_created_at = airr_map.getMapping("ir_created_at_reactivity", 
                                                ireceptor_tag, repository_tag,
                                                airr_map.getIRReactivityClass())
            ir_updated_at = airr_map.getMapping("ir_updated_at_reactivity",
                                                ireceptor_tag, repository_tag,
                                                airr_map.getIRReactivityClass())

            reactivity_dict[ir_created_at] = now_str
            reactivity_dict[ir_updated_at] = now_str

            # Insert the chunk of records into Mongo.
            t_start = time.perf_counter()
            #print("Info: JSON written =", json.dumps(reactivity_dict), flush=True)
            self.repositoryInsertRecords(reactivity_dict)
            t_end = time.perf_counter()

            # Keep track of the total number of records processed.
            ####total_records = total_records + num_records
            total_records = total_records + 1
            if total_records % 1000 == 0:
                print("Info: Total records so far =", total_records, flush=True)

        # Get the number of annotations for this repertoire 
        #if self.verbose():
        #    print("Info: Getting the number of annotations for this repertoire")
        #annotation_count = self.repositoryCountRecords(repertoire_link_id)
        #if annotation_count == -1:
        #    print("ERROR: invalid annotation count (%d), write failed." %
        #          (annotation_count))
        #    return False

        # Set the cached receptor count field for the repertoire/sample.
        #if not self.repositoryUpdateCount(repertoire_link_id, annotation_count):
        #    print("ERROR: Unable to write receptor count to repository.")
        #    return False

        # Inform on what we added and the total count for the this record.
        t_end_full = time.perf_counter()
        print("Info: Inserted %d records, %f s, %f insertions/s" %
              (total_records, t_end_full - t_start_full,
              total_records/(t_end_full - t_start_full)), flush=True)

        return True
        
