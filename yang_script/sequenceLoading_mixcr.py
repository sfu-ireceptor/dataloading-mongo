#!/usr/bin/env python
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
import sys

def get_all_substrings(string):
    if type(string) == float:
        return
    else:
        length = len(string)
        for i in range(length):
            for j in range(i + 1, length + 1):
                yield(string[i:j])
def get_substring(string):
    strlist=[]
    for i in get_all_substrings(string):
        if len(i)>3:
            strlist.append(i)
    return strlist

def loaddata(fullfilename,filename):
    df_raw = pd.read_table(fullfilename, compression='gzip', header=0, sep='\t', quotechar='"')
    df = df_raw[['bestVHit','bestDHit','bestJHit','bestVGene','bestDGene','bestJGene','bestVFamily','bestDFamily',
               'bestJFamily','bestVHitScore','nSeqCDR3','aaSeqCDR3','descrR1']]
    df.columns = ['v_call', 'd_call', 'j_call', 'vgene_gene', 'dgene_gene', 'jgene_gene', 'vgene_family', 'dgene_family',
                   'jgene_family','v_score','junction','junction_aa', 'seqId']
    df['substring'] = df['junction_aa'].apply(get_substring)
    df['junction_length'] = df['junction'].apply(str).apply(len)
    df['junction_aa_length'] = df['junction_aa'].apply(str).apply(len)
    df['functional'] = 1
    df['functionality'] = 'productive'
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
    return



def main(mypath):
    for gzfilename in listdir(mypath):
        filename = gzfilename[:-3]
        fullfilename = mypath+"/"+gzfilename
        print ("loading data to mongodb from "+fullfilename)
        loaddata(fullfilename,filename)
            
if __name__ == "__main__":
    mng_client = pymongo.MongoClient('localhost', 27017)
    db_name = sys.argv[1]
    sequence_cname = sys.argv[2]
    sample_cname = sys.argv[3]
    mypath = sys.argv[4]
#     mypath = "/mnt/data/annotations"
    # Replace mongo db name
    mng_db = mng_client[db_name]
    #  Replace mongo db collection name
    sample_db_cm = mng_db[sample_cname]
    # sq_collection_name = 'sequenceDataNew' 
    sequence_db_cm = mng_db[sequence_cname]
    main(mypath)