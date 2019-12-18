#!/usr/bin/python3

import pandas as pd
import json
import os
from datetime import datetime
from datetime import timezone
from parser import Parser

class Repertoire(Parser):
    
    def __init__(self, verbose, repository_tag, repository_chunk, airr_map, repository):
        Parser.__init__(self, verbose, repository_tag, repository_chunk, airr_map, repository)
        
    def validAIRRFieldType(self, key, value, strict):
        field_type = self.getAIRRMap().getMapping(key, "airr", "airr_type")
        #print(key)
        #print(field_type)
        #print(type(value))
        # If we are not doing strict typing, then if the key is not an AIRR
        # key (field_type == None) then we return True. This allows us to
        # check AIRR keys only and skip non-AIRR keys. If strict checking is
        # on, then if we find a non-AIRR key, we return False, as this is
        # checking AIRR typing explicitly, not typing in general.
        if field_type is None:
            if strict: return False
            else: return True

        # If we get here, we have an AIRR field, so no matter what we 
        # return False if the type doesn't match.
        valid_type = False
        if isinstance(value, (str)) and field_type == "string":
            valid_type = True
        elif isinstance(value, (bool)) and field_type == "boolean":
            valid_type = True
        elif isinstance(value, (int)) and field_type == "integer":
            valid_type = True
        elif isinstance(value, (float,int)) and field_type == "number":
            valid_type = True
        if self.verbose():
            if valid_type:
                print("Info: Field %s type OK"%(key))
            else:
                print("Warning: Field %s type ERROR, expected %s, got %s"%
                      (key, field_type, str(type(value))))
        return valid_type

    # Hide the impementation of the repository from the Repertoire subclasses. The subclasses
    # don't ask much of the repository, just insert a single JSON document at a time.
    def repositoryInsertRepertoire( self, json_document ):
    
        self.repository.insertRepertoire(json_document)
        if self.verbose:
            # If we are in verbose mode, print out a summary of the record we are inserting.
            study_tag = self.getAIRRMap().getMapping("study_id", "ir_id",
                                                     self.getRepositoryTag())
            study = "NULL" if not study_tag in json_document else json_document[study_tag]
            sample_tag = self.getAIRRMap().getMapping("sample_id", "ir_id",
                                                      self.getRepositoryTag())
            sample = "NULL" if not sample_tag in json_document else json_document[sample_tag]
            file_tag = self.getAIRRMap().getMapping("ir_rearrangement_file_name", "ir_id",
                                                    self.getRepositoryTag())
            filestr = "NULL" if not file_tag in json_document else json_document[file_tag]
            print("Info: Writing repertoire record <%s, %s, %s>" % (study, sample, filestr))
