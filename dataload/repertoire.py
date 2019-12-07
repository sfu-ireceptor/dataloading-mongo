#!/usr/bin/python3

import pandas as pd
import json
import os
from datetime import datetime
from datetime import timezone
from parser import Parser

class Repertoire(Parser):
    
    def __init__(self,context):
        self.context = context
        Parser.__init__(self, context)
        
    def repositoryInsertRepertoire( self, doc ):
    
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
            study_tag = self.getMapping("study_id", "ir_id", self.getRepositoryTag())
            study = "NULL" if not study_tag in doc else doc[study_tag]
            sample_tag = self.getMapping("sample_id", "ir_id", self.getRepositoryTag())
            sample = "NULL" if not sample_tag in doc else doc[sample_tag]
            file_tag = self.getMapping("ir_rearrangement_file_name", "ir_id", self.getRepositoryTag())
            filestr = "NULL" if not file_tag in doc else doc[file_tag]
            print("Info: Writing repertoire record <%s, %s, %s (ID: %d)>" % (study, sample, filestr, seq))

        
        results = self.context.samples.insert(doc)
