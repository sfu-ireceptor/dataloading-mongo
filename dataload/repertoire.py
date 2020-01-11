
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
    # JSON document at a time. Returns a record_id > 0 on success, -1 on failure
    def repositoryInsertRepertoire( self, json_document ):
    
        # Check to see if the repertoire already exists. We do this by using
        # the rearrangement file field and check to see if the file field
        # already exists in another record or not...
        # First get the file field we use to connect rearrangments and reperotires
        rearrangement_file_field = self.getRearrangementFileField()
        # Then get the repository field
        file_repository_field = self.getAIRRMap().getMapping(rearrangement_file_field,
                                                        self.getiReceptorTag(),
                                                        self.getRepositoryTag())
        # Also get the field that links repertoires to rearrangements
        link_field = self.getRepertoireLinkIDField()
        # Map it to a repository field
        link_repository_field = self.getAIRRMap().getMapping(link_field,
                                                             self.getiReceptorTag(),
                                                             self.getRepositoryTag())
        # Then get the actual files that belong to this repertoire.
        file_names = json_document[file_repository_field]
        # Check to see if there are files in the file field. If not, then pring a warning
        # as we won't be able to link any rearrangements to this repertoire. We set an empty
        # array as we want to still insert the record with the following warning...
        if file_names is None or file_names == "":
            print("Warning: Repertoire does not have any rearrangement files.")
            print("Warning:     Will not be able to link rearrangements to this repertoire")
            idarray = []
        else:
            # Finally we search for and get a list of the repertoires that have the files.
            idarray = self.repositoryGetRepertoireIDs(rearrangement_file_field, file_names)

        # If idarray is None, there was a problem with the query.
        if idarray is None:
            print("ERROR: Unable to check for repertoire existance for file %s"%(file_names))
            print("ERROR:     Repertoires must have valid rearrangement files.")
            print("ERROR:     Rearrangement files must be unique in the repository.")
            return False

        # The number of repertoires should be 0 other wise it already exists. Fail if
        # the number is not 0.
        num_repertoires = len(idarray)
        # Get some info to help write out messages
        study_tag = self.getAIRRMap().getMapping("study_id",
                                                 self.getiReceptorTag(),
                                                 self.getRepositoryTag())
        study = "NULL" if not study_tag in json_document else json_document[study_tag]
        sample_tag = self.getAIRRMap().getMapping("sample_id", self.getiReceptorTag(),
                                                  self.getRepositoryTag())
        sample = "NULL" if not sample_tag in json_document else json_document[sample_tag]
        # Print an error if record already exists.
        if not num_repertoires == 0:
            print("ERROR: Unable to write repertoire, already exists in the repository")
            print("ERROR:     Write failed for study '%s', sample '%s'"%(study, sample))
            print("ERROR:     File field %s contains rearrangement files %s"%
                  (rearrangement_file_field, file_names))
            print("ERROR:     Files found in records with record IDs %s"%(str(idarray)))
            return False

        # Get the repertoire, data_processing, and sample_processing IDs for the record
        # being inserted.
        repertoire_id_tag =  self.getAIRRMap().getMapping("repertoire_id",
                                                          self.getiReceptorTag(),
                                                          self.getRepositoryTag())
        display_proc_id_tag =  self.getAIRRMap().getMapping("display_processing_id",
                                                          self.getiReceptorTag(),
                                                          self.getRepositoryTag())
        sample_proc_id_tag =  self.getAIRRMap().getMapping("sample_processing_id",
                                                          self.getiReceptorTag(),
                                                          self.getRepositoryTag())
        if repertoire_id_tag in json_document:
            repetoire_id = json_document[repertoire_id_tag]
        else: repertoire_id = None

        if display_proc_id_tag in json_document:
            display_processing_id = json_document[display_proc_id_tag]
        else: display_processing_id = None

        if sample_proc_id_tag in json_document:
            sample_processing_id = json_document[sample_proc_id_tag]
        else: sample_processing_id = None

        # Try to write the record and return record_id as appropriate.
        record_id = self.repository.insertRepertoire(json_document, link_repository_field)
        if record_id > 0 and self.verbose:
            print("Info: Successfully wrote repertoire record <%s, %s, %s>" %
                  (study, sample, file_names))

        #print("############ %s %s %s"%(repetoire_id, display_processing_id, sample_processing_id))
        return record_id
