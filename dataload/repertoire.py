
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime
from datetime import timezone
from parser import Parser

class Repertoire(Parser):
    
    def __init__(self, verbose, repository_tag, repository_chunk, airr_map, repository):
        Parser.__init__(self, verbose, repository_tag, repository_chunk, airr_map, repository)
        
    def validAIRRFieldType(self, key, value, strict):
        field_type = self.getAIRRMap().getMapping(key, "airr", "airr_type",
                                  self.getAIRRMap().getRepertoireClass())
        field_nullable = self.getAIRRMap().getMapping(key, "airr", "airr_nullable",
                                  self.getAIRRMap().getRepertoireClass())
        # If we are not doing strict typing, then if the key is not an AIRR
        # key (field_type == None) then we return True. This allows us to
        # check AIRR keys only and skip non-AIRR keys. If strict checking is
        # on, then if we find a non-AIRR key, we return False, as this is
        # checking AIRR typing explicitly, not typing in general.
        if field_type is None:
            if strict: return False
            else: return True

        # If the value is null and the field is nullable or there is no nullable
        # entry in the AIRR mapping (meaning NULL is OK) then return True.
        if pd.isnull(value) and (field_nullable == None or field_nullable):
            return True

        # If we get here, we have an AIRR field, so no matter what we 
        # return False if the type doesn't match.
        valid_type = False
        if isinstance(value, (str)) and field_type == "string":
            valid_type = True
        elif isinstance(value, (bool,np.bool_)) and field_type == "boolean":
            valid_type = True
        elif isinstance(value, (int,np.integer)) and field_type == "integer":
            valid_type = True
        elif isinstance(value, (float,int,np.floating,np.integer)) and field_type == "number":
            # We need to accept integers and floats as numbers.
            valid_type = True
        elif isinstance(value, (str)) and field_type == "ontology":
            # Ontology is a special case, as we store the ontology
            # field value in the name directly as a string.
            valid_type = True

        if self.verbose():
            if not valid_type:
                print("Warning: Field %s type ERROR, expected %s, got %s"%
                      (key, field_type, str(type(value))))
                #print(key)
                #print(field_type)
                #print(field_nullable)
                #print(value)
                #print(type(value))

        return valid_type

    # Hide the impementation of the repository from the Repertoire subclasses.
    # The subclasses don't ask much of the repository, just insert a single
    #JSON document at a time.
    def repositoryInsertRepertoire( self, json_document ):
    
        self.repository.insertRepertoire(json_document)
        if self.verbose:
            # If we are in verbose mode, print out a summary of the record.
            study_tag = self.getAIRRMap().getMapping("study_id", "ir_id",
                                                     self.getRepositoryTag())
            study = "NULL" if not study_tag in json_document else json_document[study_tag]
            sample_tag = self.getAIRRMap().getMapping("sample_id", "ir_id",
                                                      self.getRepositoryTag())
            sample = "NULL" if not sample_tag in json_document else json_document[sample_tag]
            file_tag = self.getAIRRMap().getMapping("ir_rearrangement_file_name", "ir_id",
                                                    self.getRepositoryTag())
            filestr = "NULL" if not file_tag in json_document else json_document[file_tag]
            print("Info: Writing repertoire record <%s, %s, %s>" %
                  (study, sample, filestr))
