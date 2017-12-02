# Script for loading MIXCR formatted data file 
# into an iReceptor data node MongoDb database
#
## WORK-IN-PROGRESS, not yet completed (nor used)
#

import pandas as pd
import json
import pymongo
import os
import tarfile
import sys
import re
import numpy as np
from os import listdir
from os.path import isfile, join

import parser

mng_client = pymongo.MongoClient('localhost', 27017)

# Replace mongo db name
mng_db = mng_client['mydb']

#  Replace mongo db collection name
sd_collection_name = 'sampleDataNew' 

sample_db_cm = mng_db[sd_collection_name]
# sq_collection_name = 'sequenceDataNew' 

sq_collection_name = 'sequence_data_newnames' 
sequence_db_cm = mng_db[sq_collection_name]

mypath = '/mnt/mixcrData/'
filename = 'SRR4084213_aa_mixcr_annotation.txt'

# onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

# from sh import gunzip
# gunzip(mypath+filename+'.gz')


class MiXCR(Parser):
    
    def __init__( self, context ):
        Parser._init_(context)

    def process(self):
        df_raw = pd.read_table(mypath+filename)
        
        df = df_raw[['bestVHit','bestDHit','bestJHit','bestVGene','bestDGene','bestJGene','bestVFamily','bestDFamily',
                   'bestJFamily','bestVHitScore','nSeqCDR3','aaSeqCDR3','descrR1']]
        df.columns = ['vgene', 'dgene', 'jgene', 'vgene_gene', 'dgene_gene', 'jgene_gene', 'vgene_family', 'dgene_family',
                       'jgene_family','v_score','junction','junction_aa', 'seqId']
        
        df['substring'] = df['junction_aa'].apply(get_substring)
        df['junction_length'] = df['junction'].apply(str).apply(len)
        df['junction_length_aa'] = df['junction_aa'].apply(str).apply(len)
        df['functional'] = 'productive'
        df['annotation_tool'] = 'MiXCR'
        
        sampleid = sample_db_cm.find({"mixcr_file_name":{'$regex': filename}},{'_id':1})
        
        ir_project_sample_id = [i['_id'] for i in sampleid][0]
        df['ir_project_sample_id']=ir_project_sample_id
        
        count_row = len(df.index)
        num_to_insert = 10000
        
        (runNumber,rest)= divmod(count_row,num_to_insert)
        
        for i in range(runNumber+1):
            df_insert = df.iloc[10000*i:10000*(i+1)]
            records = json.loads(df_insert.T.to_json()).values()
            sequence_db_cm.insert_many(records)
        
        ori_count = sample_db_cm.find_one({"mixcr_file_name":{'$regex': filename}},{"ir_sequence_count":1})["ir_sequence_count"]
        sample_db_cm.update({"mixcr_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":count_row+ori_count}}, multi=True)
        # sample_db_cm.update_one({"mixcr_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":count_row}})
        
        sequence_db_cm.create_index("ir_project_sample_id")
        
        sequence_db_cm.create_index("functional")
