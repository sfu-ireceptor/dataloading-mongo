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
from time import gmtime, strftime

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

def setGene(gene):
    gene_string = re.split(',| ', gene)
    gene_string = list(set(gene_string))
    if len(gene_string) == 1 or 0:
        return gene_string
    else:
        if '' in gene_string:
            gene_string.remove('')
        if 'or' in gene_string:
            gene_string.remove('or')
        if 'F' in gene_string:
            gene_string.remove('F')
        if 'P' in gene_string:
            gene_string.remove('P')
        if '[F]' in gene_string:
            gene_string.remove('[F]')
        if 'Homsap' in gene_string:
            gene_string.remove('Homsap')
        if '(see' in gene_string:
            gene_string.remove('(see')
        if 'comment)' in gene_string:
            gene_string.remove('comment)')
        return gene_string

def functional_boolean(functionality):
    if functionality.startswith("productive"):
        return 1
    else:
        return 0
    
def loadData(mypath,filename,sample_db_cm):    
    tar = tarfile.open(mypath+filename)
    tar.extractall()
    tar.close()
    outfile = open(filename+".csv", "w")
    print ("%s - parsing data " % strftime('%Y-%m-%d %H:%M:%S', gmtime()))
    Summary_1 = pd.read_table('1_Summary.txt')
    IMGT_gapped_nt_sequences_2 = pd.read_table('2_IMGT-gapped-nt-sequences.txt')
    Nt_sequences_3 = pd.read_table('3_Nt-sequences.txt')
    IMGT_gapped_AA_sequences_4 = pd.read_table('4_IMGT-gapped-AA-sequences.txt')
    AA_sequences_5 = pd.read_table('5_AA-sequences.txt')
    V_REGION_mutation_and_AA_change_table_7 = pd.read_table('7_V-REGION-mutation-and-AA-change-table.txt')
    Parameters_11 = pd.read_table('11_Parameters.txt',header =None)
    Para_dict = dict(zip(Parameters_11[0], Parameters_11[1])) 
    Summary_column_list = Summary_1.columns.values.tolist()
    if 'Functionality' in Summary_column_list:
        df_1 = Summary_1[['Sequence ID','V-GENE and allele', 'J-GENE and allele', 'D-GENE and allele', 'Functionality',
                          'V-REGION score', 'J-REGION score','V-REGION identity %',
                          'Functionality comment',
                          'V-REGION identity %']]
        df_1.columns = ['seq_name','v_string', 'j_string', 'd_string', 'functionality', 'v_score', 'j_score',
                        'vgene_probablity',                        
                        'functionality_comment',
                        'vgene_probability']
    elif 'V-DOMAIN Functionality' in Summary_column_list:
        df_1 = Summary_1[['Sequence ID','V-GENE and allele', 'J-GENE and allele', 'D-GENE and allele', 'V-DOMAIN Functionality',
                          'V-REGION score', 'J-REGION score','V-REGION identity %',
                          'V-DOMAIN Functionality comment',
                          'V-REGION identity %']]
        df_1.columns = ['seq_name','v_string', 'j_string', 'd_string', 'functionality', 'v_score', 'j_score',
                        'vgene_probablity',                        
                        'functionality_comment',
                        'vgene_probability']
    df_3 = Nt_sequences_3[['JUNCTION','V-REGION start','V-REGION end',
                           'J-REGION start','J-REGION end','D-REGION start','D-REGION end','JUNCTION start','JUNCTION end']]
    df_3.columns = ['junction_nt','v_start','v_end','j_start','j_end','d_start',
                    'd_end','junction_start',
                    'junction_end']
    df_5 = AA_sequences_5[['JUNCTION']]
    df_5.columns = ['junction_aa']
    df_concat = pd.concat([df_1, df_3, df_5], axis=1)
    df_concat['annotation_tool'] = "V-Quest"
    df_concat['annotation_date'] = Para_dict['Date']
    df_concat['tool_version'] = Para_dict['IMGT/V-QUEST programme version']
    df_concat['reference_version'] = Para_dict['IMGT/V-QUEST reference directory release']
    df_concat['species'] = Para_dict['Species']
    df_concat['receptor_type'] = Para_dict['Receptor type or locus']
    df_concat['reference_directory_set'] = Para_dict['IMGT/V-QUEST reference directory set']
    df_concat['search_insert_delete'] = Para_dict['Search for insertions and deletions']
    df_concat['no_nucleotide_to_add'] = Para_dict[ "Nb of nucleotides to add (or exclude) in 3' of the V-REGION for the evaluation of the alignment score"]
    df_concat['no_nucleotide_to_exclude'] = Para_dict[ "Nb of nucleotides to exclude in 5' of the V-REGION for the evaluation of the nb of mutations"]
    df_concat = df_concat.where((pd.notnull(df_concat)), "")
    sampleid = sample_db_cm.find({"imgt_file_name":{'$regex': filename}},{'_id':1})
    ir_project_sample_id = [i['_id'] for i in sampleid][0]
    df_concat['ir_project_sample_id']=ir_project_sample_id
    df_concat['substring'] = df_concat['junction_aa'].apply(get_substring)
    df_concat['v_call'] = df_concat['v_string'].apply(str).apply(setGene)
    df_concat['j_call'] = df_concat['j_string'].apply(str).apply(setGene)
    df_concat['d_call'] = df_concat['d_string'].apply(str).apply(setGene)
    df_concat['junction_length'] = df_concat['junction_nt'].apply(len)
    df_concat['junction_aa_length'] = df_concat['junction_aa'].apply(len)
    df_concat['functional'] = df_concat['functionality'].apply(functional_boolean)
    records = json.loads(df_concat.T.to_json()).values()
    print ("%s - loading data " % strftime('%Y-%m-%d %H:%M:%S', gmtime()))
    #sequence_db_cm.insert_many(records)
    #df_concat.to_csv(outfile, sep=',', encoding='utf-8', index=False)
    for sequence in records:
      outfile.write(json.dumps(sequence ))
      outfile.write('\n')
    outfile.close()  
    ir_sequence_count = len(records)
    print ("%s - loading complete " % strftime('%Y-%m-%d %H:%M:%S', gmtime()))
    print ("Loaded % sequences" % ir_sequence_count)
    ori_count = sample_db_cm.find_one({"imgt_file_name":{'$regex': filename}},{"ir_sequence_count":1})["ir_sequence_count"]
    #sample_db_cm.update({"imgt_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":ir_sequence_count+ori_count}}, multi=True)
    return


def main(mypath):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
    for filename in onlyfiles:
        print (filename)
        sampleid = sample_db_cm.find({"imgt_file_name":{'$regex': filename}},{'_id':1})
        idlist=[i['_id'] for i in sampleid]
        if len(idlist)>0:
            loadData(mypath,filename,sample_db_cm)
        else:
            print ("Warning! The filename %s does not match the one in sample data" %filename)
    filelist = [ f for f in os.listdir(".") if f.endswith(".txt") ]
    for f in filelist:
        os.remove(f)
            
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
