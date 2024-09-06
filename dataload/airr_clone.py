# Script for loading MIXCR formatted annotation file 
# into an iReceptor data node MongoDb database

import sys
import os.path
import pandas as pd
import numpy as np 
import json
import gzip
import time

from clone import Clone
from annotation import Annotation
from parser import Parser

class AIRR_Clone(Clone):
    
    def __init__( self, verbose, repository_tag, repository_chunk, airr_map, repository):
        Clone.__init__(self, verbose, repository_tag, repository_chunk, airr_map, repository)
        # The annotation tool used for the AIRR-Clone is ambiguous, use a generic name.
        self.setAnnotationTool("AIRR-Clone")
        # The default column in the AIRR Mapping file is "airr" as this is parsing AIRR
        # fields.. This can be overrideen by the user should they choose to use a
        # differnt set of columns from the file.
        self.setFileMapping("airr")

    def process(self, filewithpath):

        # This reads one AIRR Clone JSON file at a time, given the full file (path) name
        # May also be gzip compressed file
        
        # Open, decompress then read(), if it is a gz archive
        success = True

        # Check to see if the file exists and return if not.
        if not os.path.isfile(filewithpath):
            print("ERROR: Could not open AIRR Clone file ", filewithpath)
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
                success = self.processAIRRCloneFile(file_handle, filename)

        else: # read directly as a regular text file
            if self.verbose():
                print("Info: Reading text file: "+filewithpath)
            file_handle = open(filewithpath, "r")
            success = self.processAIRRCloneFile(file_handle, filename)

        return success

    def processAIRRCloneFile( self, file_handle, filename ):

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
        clone_link_field = self.getAnnotationLinkIDField()

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
        # Clone related fields.
        map_column = self.getAIRRMap().getIRCloneMapColumn(airr_tag)
        # Get a boolean column that flags columns of interest. Exclude nulls.
        fields_of_interest = map_column.notnull()
        # Afer the following airr_fields contains N columns (e.g. iReceptor, AIRR)
        # that contain the AIRR Repertoire mappings.
        airr_fields = self.getAIRRMap().getIRCloneRows(fields_of_interest)

        # Extract the fields that are of interest for this file. Essentially all non
        # null fields in the file. This is a boolean array that is T everywhere there
        # is a notnull field in the column of interest.
        map_column = airr_map.getIRCloneMapColumn(filemap_tag)
        fields_of_interest = map_column.notnull()

        # We select the rows in the mapping that contain fields of interest for Clones.
        # At this point, file_fields contains N columns that contain our mappings for
        # the specific formats (e.g. airr). The rows are limited to 
        # only data that is relevant to Clones
        file_fields = airr_map.getIRCloneRows(fields_of_interest)

        # We need to build the set of fields that the repository can store. We don't
        # want to extract fields that the repository doesn't want.
        cloneColumns = []
        columnMapping = {}
        if self.verbose():
            print("Info: Dumping expected %s (%s) to repository mapping"
                  %(self.getAnnotationTool(),filemap_tag))
        for index, row in file_fields.iterrows():
            if self.verbose():
                print("Info:    %s -> %s"
                      %(str(row[filemap_tag]), str(row[repository_tag])))
            # If the repository column has a value for the Clone field, track the field
            # from both the Clone and repository side.
            if not pd.isnull(row[repository_tag]):
                cloneColumns.append(row[filemap_tag])
                columnMapping[row[filemap_tag]] = row[repository_tag]
            else:
                if self.verbose():
                    print("Info:    Repository does not support " +
                          str(row[filemap_tag]) + ", not inserting into repository")

        # Load in the JSON file. The file should be an array of Clone objects as
        # per the AIRR spec.
        if self.verbose():
            print("Info: Reading the Clone JSON array", flush=True)
        try:
            clone_array = json.load(file_handle)
        except json.JSONDecodeError as error:
            print("ERROR: %s"%(error))
            print("ERROR: Invalid JSON in file %s"%(filename))
            return False
        except Exception as error:
            print("ERROR: %s"%(error))
            return False

        if self.verbose():
            print("Info: Read %d Clone objects"%(len(clone_array)), flush=True)

        # Iterate over each element in the array 
        total_records = 0
        for clone_dict in clone_array:
            # Remap the column names. We need to remap because the columns may be in 
            # a different order in the file than in the column mapping. We leave any
            # non-mapped columns in the data frame as we don't want to discard data.
            add_dict = dict() 
            del_dict = dict()
            for clone_key, clone_value in clone_dict.items():
                if clone_key in columnMapping:
                    mongo_column = columnMapping[clone_key]
                    if self.verbose() and total_records == 0:
                        print("Info: Mapping %s field in file: %s -> %s"
                              %(self.getAnnotationTool(), clone_key, mongo_column))
                    # If they are different swap them.
                    if mongo_column != clone_key:
                        add_dict[mongo_column] = clone_value
                        del_dict[clone_key] = True
                else:
                    if self.verbose() and total_records == 0:
                        print("Info: No mapping for %s column %s, storing as is"
                              %(self.getAnnotationTool(), clone_key))
            # Add any key value pairs in the add_dict to the clone_dict. These are mapped
            # columns that changed from the file field to the repository field.
            for add_key, add_value in add_dict.items():
                clone_dict[add_key] = add_value
                if self.verbose() and total_records == 0:
                    print("Info: Adding %s -> %s"%(add_key, add_value))
            # Remove any key value pairs  that are in the delete_dict. These are the keys 
            # that changed name between the file and the repository. We store these through
            # add_dict so don't want them twice.
            for del_key in del_dict:
                del clone_dict[del_key]
                if self.verbose() and total_records == 0:
                    print("Info: Removing %s "%(del_key))
            # Check to see which desired Clone mappings we don't have in the file...
            for clone_column, mongo_column in columnMapping.items():
                if not mongo_column in clone_dict:
                    if self.verbose() and total_records == 0:
                        print("Info: Missing data in input %s file for %s"
                              %(self.getAnnotationTool(), clone_column))
            
            # We need to look up the field from an iReceptor perspective. We want the
            # field name in the iReceptor column mapping and map that to the correct
            # field name for the repository we are writing to.
            v_call = airr_map.getMapping("v_call", ireceptor_tag, repository_tag)
            d_call = airr_map.getMapping("d_call", ireceptor_tag, repository_tag)
            j_call = airr_map.getMapping("j_call", ireceptor_tag, repository_tag)
            ir_vgene_gene = airr_map.getMapping("ir_vgene_gene_clone",
                                                ireceptor_tag, repository_tag)
            ir_dgene_gene = airr_map.getMapping("ir_dgene_gene_clone",
                                                ireceptor_tag, repository_tag)
            ir_jgene_gene = airr_map.getMapping("ir_jgene_gene_clone",
                                                ireceptor_tag, repository_tag)
            ir_vgene_family = airr_map.getMapping("ir_vgene_family_clone",
                                                ireceptor_tag, repository_tag)
            ir_dgene_family = airr_map.getMapping("ir_dgene_family_clone",
                                                ireceptor_tag, repository_tag)
            ir_jgene_family = airr_map.getMapping("ir_jgene_family_clone",
                                                ireceptor_tag, repository_tag)

            # Convert the gene call to a valid gene and family for each of the VDJ calls.
            if v_call in clone_dict:
                array_result =  Annotation.setGeneGene([clone_dict[v_call]])
                if len(array_result) == 1:
                    clone_dict[ir_vgene_gene] = array_result[0]
                else:
                    print("ERROR: Ambiguous v_call, not storing clone gene information.")

                array_result = Annotation.setGeneFamily([clone_dict[v_call]])
                if len(array_result) == 1:
                    clone_dict[ir_vgene_family] = array_result[0]
                else:
                    print("ERROR: Ambiguous v_call, not storing clone family information.")
            if d_call in clone_dict:
                array_result =  Annotation.setGeneGene([clone_dict[d_call]])
                if len(array_result) == 1:
                    clone_dict[ir_dgene_gene] = array_result[0]
                else:
                    print("ERROR: Ambiguous d_call, not storing clone gene information.")

                array_result = Annotation.setGeneFamily([clone_dict[d_call]])
                if len(array_result) == 1:
                    clone_dict[ir_dgene_family] = array_result[0]
                else:
                    print("ERROR: Ambiguous d_call, not storing clone family information.")
            if j_call in clone_dict:
                array_result =  Annotation.setGeneGene([clone_dict[j_call]])
                if len(array_result) == 1:
                    clone_dict[ir_jgene_gene] = array_result[0]
                else:
                    print("ERROR: Ambiguous j_call, not storing clone gene information.")

                array_result = Annotation.setGeneFamily([clone_dict[j_call]])
                if len(array_result) == 1:
                    clone_dict[ir_jgene_family] = array_result[0]
                else:
                    print("ERROR: Ambiguous d_call, not storing clone family information.")

            # Process the junction_aa to generate our substring optimization
            junction_aa = airr_map.getMapping("junction_aa",
                                              ireceptor_tag, repository_tag)
            ir_substring = airr_map.getMapping("ir_substring_clone",
                                               ireceptor_tag, repository_tag)
            if junction_aa in clone_dict:
                clone_dict[ir_substring] = Annotation.get_substring(clone_dict[junction_aa])

            # Get the all important link field that maps repertoires to clones.
            rep_clone_link_field = airr_map.getMapping(
                                             clone_link_field,
                                             ireceptor_tag, repository_tag)
            if not rep_clone_link_field is None:
                clone_dict[rep_clone_link_field] = repertoire_link_id
            else:
                print("ERROR: Could not get repertoire link field from AIRR mapping.")
                return False

            # Check to see if clone_id exists, and if so, store it in the special
            # ADC clone_id record, since clone_id is overwritten in the repository.
            airr_clone_id = airr_map.getMapping("clone_id_clone",
                                                ireceptor_tag, repository_tag,
                                                airr_map.getCloneClass())
            ir_clone_id = airr_map.getMapping("ir_clone_id_clone",
                                             ireceptor_tag, repository_tag,
                                             airr_map.getIRCloneClass())
            if airr_clone_id in clone_dict:
                clone_dict[ir_clone_id] = clone_dict[airr_clone_id]


            # Set the relevant IDs for the record being inserted. It updates the dictionary
            # (passed by reference) and returns False if it fails. If it fails, don't
            # load any data.
            if (not self.checkIDFieldsJSON(clone_dict,
                                           repertoire_link_field, repertoire_link_id,
                                           repertoire_id_value,
                                           data_processing_id_value,
                                           sample_processing_id_value)):

                return False

            # Create the created and update values for this block of records. Note that
            # this means that each block of inserts will have the same date.
            now_str = self.getDateTimeNowUTC()
            ir_created_at = airr_map.getMapping("ir_created_at_clone", 
                                                ireceptor_tag, repository_tag,
                                                airr_map.getIRCloneClass())
            ir_updated_at = airr_map.getMapping("ir_updated_at_clone",
                                                ireceptor_tag, repository_tag,
                                                airr_map.getIRCloneClass())

            clone_dict[ir_created_at] = now_str
            clone_dict[ir_updated_at] = now_str

            # Insert the chunk of records into Mongo.
            t_start = time.perf_counter()
            self.repositoryInsertRecords(clone_dict)
            t_end = time.perf_counter()

            # Keep track of the total number of records processed.
            ####total_records = total_records + num_records
            total_records = total_records + 1
            if total_records % 1000 == 0:
                print("Info: Total records so far =", total_records, flush=True)

        # Get the number of annotations for this repertoire 
        if self.verbose():
            print("Info: Getting the number of annotations for this repertoire")
        annotation_count = self.repositoryCountRecords(repertoire_link_id)
        if annotation_count == -1:
            print("ERROR: invalid annotation count (%d), write failed." %
                  (annotation_count))
            return False

        # Set the cached clone count field for the repertoire/sample.
        if not self.repositoryUpdateCount(repertoire_link_id, annotation_count):
            print("ERROR: Unable to write clone count to repository.")
            return False

        # Inform on what we added and the total count for the this record.
        t_end_full = time.perf_counter()
        print("Info: Inserted %d records, annotation count = %d, %f s, %f insertions/s" %
              (total_records, annotation_count, t_end_full - t_start_full,
              total_records/(t_end_full - t_start_full)), flush=True)

        return True
        
