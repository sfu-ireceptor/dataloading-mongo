#!/usr/bin/python3

import pandas as pd
import json
import os
from datetime import datetime
from datetime import timezone
from repertoire import Repertoire
import airr

class AIRRRepertoire(Repertoire):
    
    # Constructor - call the parent class constructor.
    def __init__(self, verbose, repository_tag, repository_chunk, airr_map, repository):
        Repertoire.__init__(self, verbose, repository_tag, repository_chunk, airr_map, repository) 

    def ir_maptorepository(self, field):
        # Check to see if the field is in the AIRR mapping, if not warn.
        airr_field = self.getAIRRMap().getMapping(field, "airr", "airr")
        if airr_field is None:
            print("Warning: Could not find %s in AIRR mapping"%(field))

        # Check to see if the field can be mapped to a field in the repository, if not warn.
        repo_field = self.getAIRRMap().getMapping(field, "airr", self.getRepositoryTag())
        if repo_field is None:
            repo_field = field
            print("Warning: Could not find repository mapping for %s, storing as is"%(field))

        # If we are verbose, tell about the mapping...
        if self.verbose():
            print("Info: Mapping %s => %s" % (field, repo_field))

        # Return the mapping.
        return repo_field

    # This method is a recursive function that takes a key and value in a JSON
    # object and recursively flattens the values adding each element to the dictionary 
    # as it finds a "leaf node". Note a leaf node in general is a key value pair where
    # the value is not a compoud object (not a dict or a list). If it is not a leaf node
    # then the fucntion recurses on all of the elements in the dict or list. Note that
    # a leaf node is a bit complex and specialized based on both the AIRR spec and how
    # they are represented in the iReceptor repository. 
    def ir_flatten(self, key, value, dictionary):
        # If it is an integer, float, or bool we just use the key value pair.
        if isinstance(value, (int, float, bool)):
            if self.validAIRRFieldType(key, value, False):
                dictionary[self.ir_maptorepository(key)] = value
            else:
                raise TypeError("AIRR type error for " + key)
        # If it is a string we just use the key value pair.
        elif isinstance(value, str):
            if self.validAIRRFieldType(key, value, False):
                dictionary[self.ir_maptorepository(key)] = value
            else:
                raise TypeError("AIRR type error for " + key)
        elif isinstance(value, dict):
            # We need to handle the AIRR ontology terms. If we get one we want 
            # to use the value of the ontology term in our repository for now.
            # We also store the id and value separately as two non AIRR keywords.
            type_info = self.getAIRRMap().getMapping(key, "ir_id", "airr_type")
            if type_info == "ontology":
                # TODO: need to implement type checking on ontology fields.
                #if self.validAIRRFieldType(key, value, False):
                #    dictionary[self.ir_maptorepository(key)] = value['value']
                #else:
                #    raise TypeError(key)
                dictionary[self.ir_maptorepository(key)] = value['value']
                dictionary[self.ir_maptorepository(key+"_value")] = value['value']
                dictionary[self.ir_maptorepository(key+"_id")] = value['id']
            else:
                for sub_key, sub_value in value.items():
                    self.ir_flatten(sub_key, sub_value, dictionary)
        elif isinstance(value, list):
            # There are currently three possible list  situations in the spec. 
            # - keywords_study: This is an array of strings that should be concatenated
            # - diagnosis: We only support one per repertoire. Warn and continue with 1st
            # - pcr_target: We only support one per repertoire. Warn and continue with 1st
            # - data_processing: We only support one per repertoire. Warn and continue with 1st

            # We flatten this explicitly as a special case. We want to store the list of strings.
            if key == "keywords_study":
                # TODO: Need to implement type checking on this field...
                #if self.validAIRRFieldType(key, value, False):
                #    dictionary[self.ir_maptorepository(key)] = value
                #else:
                #    raise TypeError(key)
                dictionary[self.ir_maptorepository(key)] = value
            else:
                # If we are handling a data processing element list, we have a hint as 
                # to which element is the most important, as we can use the "primary_annotation"
                # field to determine which one to use.
                if key == "data_processing":
                    # Warn if we found more than one, as we only store one per repertoire. If
                    # you have more than one and want to store the rearrangements separately
                    # then you need to split this up into two repertoires.
                    if len(value) > 1:
                        print("Warning: Found more than one %s element (found %d)."%
                              (key, len(value)))
                    # Look for the primary annotation
                    got_primary = False
                    for element in value:
                        if 'primary_annotation' in element and element['primary_annotation']:
                            # If we found it, flatten it and the break out of the loop
                            for sub_key, sub_value in element.items():
                                self.ir_flatten(sub_key, sub_value, dictionary)
                            got_primary = True
                            print("Warning: Found a primary annotation, using it.")
                            break
                    # If we didn't find the primary, then use the first one as a best guess.
                    if not got_primary:
                        print("Warning: Could not find a primary annotation, using the first one.")
                        for sub_key, sub_value in value[0].items():
                            self.ir_flatten(sub_key, sub_value, dictionary)
                else:
                    # In the general case, iReceptor only supports a single instance in 
                    # array subtypes. If this occurs, we print a warning and use the first
                    # element in the array and ignore the rest. This is a fairly substantial
                    # issue and MAYBE it should be a FATAL ERROR???
                    if len(value) > 1:
                        print("Warning: Found a repertoire list for %s > 1 (%d)."%(key, len(value)))
                        print("Warning: iReceptor only supports a single array, using first instance.")
                    for sub_key, sub_value in value[0].items():
                        self.ir_flatten(sub_key, sub_value, dictionary)
        return dictionary

    def process(self, filename):

        # Check to see if we have a file    
        if not os.path.isfile(filename):
            print("ERROR: input file " + filename + " is not a file")
            return False

        # Check the validity of the repertoires from an AIRR perspective
        try:
            data = airr.load_repertoire(filename, validate=True)
        except airr.ValidationError as err:
            print("ERROR: AIRR repertoire validation failed for file %s - %s" % (filename, err))
            return False

        # The 'Repertoire' contains a dictionary for each repertoire.
        repertoire_list = []
        for repertoire in data['Repertoire']:
            repertoire_dict = dict()
            for key, value in repertoire.items():
                try:
                    self.ir_flatten(key, value, repertoire_dict)
                except TypeError as error:
                    print("ERROR: %s"%(error))
                    return False

            # Add repository specific fields to each repertoire as required.
            # Add a created_at and updated_at field in the repository.
            now_str = Repertoire.getDateTimeNowUTC()
            repertoire_dict['ir_created_at'] = now_str
            repertoire_dict['ir_updated_at'] = now_str

            # Get the mapping for the sequence count field for the repository and 
            # initialize the sequeunce count to 0. If we can't find a mapping for this
            # field then we can't do anything. 
            count_field = self.getAIRRMap().getMapping("ir_sequence_count", "ir_id",
                                                       self.getRepositoryTag() )
            if count_field is None:
                print("Warning: Could not find ir_sequence_count tag in %s, field not initialized"
                      % ( self.getRepositoryTag() ))
            else:
                repertoire_dict[count_field] = 0

            # Ensure that we have a correct file name to link fields. If we can't find it 
            # this is a fatal error as we can not link any data to this set repertoire,
            # so there is no point adding the repertoire...
            repository_file_field = self.getAIRRMap().getMapping("ir_rearrangement_file_name",
                                                    "ir_id", self.getRepositoryTag())
            # If we can't find a mapping for this field in the repository mapping, then
            # we might still be OK if the metadata spreadsheet has the field. If the fails, 
            # then we should exit.
            if repository_file_field is None or len(repository_file_field) == 0:
                print("Warning: Could not find a valid repository mapping for the rearrangement file name (ir_rearrangement_file_name)")
                repository_file_field = "ir_rearrangement_file_name"
    
            # If we can't find the file field for the rearrangement field in the repository, then
            # abort, as we won't be able to link the repertoire to the rearrangement.
            if not repository_file_field in repertoire_dict:
                print("ERROR: Could not find a rearrangement file field in the metadata (ir_rearrangement_file_name)")
                print("ERROR: Will not be able to link repertoire to rearrangement annotations")
                repertoire_dict['ir_rearrangement_file_name'] = ""
                return False

            repertoire_list.append(repertoire_dict)
                
        # Iterate over the list and load records. Note that this code inserts all data
        # that was read in. That is, all of the non MiAIRR fileds that exist
        # are stored in the repository. So if the provided file has lots of extra fields
        # they will exist in the repository.
        for r in repertoire_list:
            self.repositoryInsertRepertoire( r )
    
        return True
