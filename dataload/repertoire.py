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
