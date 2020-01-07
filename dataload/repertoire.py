
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
    
    # Utility function to check to see if a given value is a valid type for a specific
    # AIRR field.  If doing strict AIRR checks, if the field is not an AIRR field then
    # it returns FALSE. If not doing strict AIRR checks, then it doesn't do any checks
    # against the the field if it isn't an AIRR field (it returns TRUE). 
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
            if pd.isnull(value):
                print("Info: Field %s type ERROR, null value, field is non-nullable"%
                      (key))
            elif not valid_type:
                print("Info: Field %s type ERROR, expected %s, got %s"%
                      (key, field_type, str(type(value))))
        return valid_type

    # Hide the impementation of the repository from the Repertoire subclasses.
    # The subclasses don't ask much of the repository, just insert a single
    # JSON document at a time.
    def repositoryInsertRepertoire( self, json_document ):
    
        # Check to see if the repertoire already exists. We do this by using
        # the rearrangement file field and check to see if the file field
        # already exists in another record or not...
        # First get the file field we use to connect rearrangments and reperotires
        rearrangement_file_field = self.getRearrangementFileField()
        # Then get the repository field
        repository_field = self.getAIRRMap().getMapping(rearrangement_file_field,
                                                        "ir_id", self.getRepositoryTag())
        # Then get the actual files we are trying to write to the repository
        file_names = json_document[repository_field]
        # Finally we search for and get a list of the repertoires that have this value.
        idarray = self.repositoryGetRepertoireIDs(rearrangement_file_field, file_names)
        # The number of repertoires should be 0 other wise it already exists. Fail if
        # the number is not 0.
        num_repertoires = len(idarray)
        # Get some info to help write out messages
        study_tag = self.getAIRRMap().getMapping("study_id", "ir_id",
                                                 self.getRepositoryTag())
        study = "NULL" if not study_tag in json_document else json_document[study_tag]
        sample_tag = self.getAIRRMap().getMapping("sample_id", "ir_id",
                                                  self.getRepositoryTag())
        sample = "NULL" if not sample_tag in json_document else json_document[sample_tag]
        # Print an error if record already exists.
        if not num_repertoires == 0:
            print("ERROR: Unable to write repertoire, already exists in the repository")
            print("ERROR:     Write failed for study '%s', sample '%s'"%(study, sample))
            print("ERROR:     File field %s contains rearrangement files %s"%
                  (rearrangement_file_field, file_names))
            print("ERROR:     Files found in record %s"%(str(idarray)))
            return False

        # Try to write the record and return True/False as appropriate.
        insert_ok = self.repository.insertRepertoire(json_document)
        if insert_ok and self.verbose:
            print("Info: Writing repertoire record <%s, %s, %s>" %
                  (study, sample, file_names))

        return insert_ok
