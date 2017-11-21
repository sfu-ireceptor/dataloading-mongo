import pandas as pd
import json
import pymongo

import zipfile

import tarfile

import sys
import re
import numpy as np

import os
from os.path import isfile, join

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

class IMGT:
    
    def __init__( self, context ):
        self.context = context
    
    def dataFolder(self):
        # First iteration (bad: hardcoded!)
        return self.context.library + "/imgt/imgt/"
    
    def getPath( self, filename ):
        return join( self.dataFolder(), filename )
    
    def readDf( self, filename, **kwargs ):
        return pd.read_table( self.getPath(filename), kwargs )

    def process(self):
        
        # Assuming that you have a zip file like 'imgt.zip'
        with zipfile.ZipFile( self.context.path, "r" ) as zip:
            # unzip to local directory for now
            zip.extractall( self.context.library )

        onlyfiles = [f for f in os.listdir( self.dataFolder() ) if isfile( self.getPath(f) )]
        
        # Process annotation files
        for f in onlyfiles:
            print (f)
            processAnnotationFile( self.getPath(f) )
                    
        # Create indices(?)       
        # self.context.sequences.create_index("functional")
        
    def processAnnotationFile( self, path ):
            
        tar = tarfile.open( path )
        
        tar.extractall()
        
        tar.close()
        
        Summary_1 = self.readDf('1_Summary.txt')
        
        IMGT_gapped_nt_sequences_2 = self.readDf('2_IMGT-gapped-nt-sequences.txt')
        
        Nt_sequences_3 = self.readDf('3_Nt-sequences.txt')
        
        IMGT_gapped_AA_sequences_4 = self.readDf('4_IMGT-gapped-AA-sequences.txt')
        
        AA_sequences_5 = self.readDf('5_AA-sequences.txt')
        
        V_REGION_mutation_and_AA_change_table_7 = self.readDf('7_V-REGION-mutation-and-AA-change-table.txt')

        Parameters_11 = self.readDf('11_Parameters.txt', header=None )

        Para_dict = dict(zip(Parameters_11[0], Parameters_11[1]))

        Summary_column_list = Summary_1.columns.values.tolist()
        
        if 'Functionality' in Summary_column_list:
            
            df_1 = Summary_1[['Sequence ID','V-GENE and allele', 'J-GENE and allele', 'D-GENE and allele', 'Functionality', 
                              'V-REGION score', 'J-REGION score','V-REGION identity %', 'D-REGION reading frame',
                              'CDR1-IMGT length', 'CDR2-IMGT length','CDR3-IMGT length', 'Functionality comment', 
                              'Orientation', 'V-REGION identity %']]
            
            df_1.columns = ['seq_name','v_string', 'j_string', 'd_string', 'functional', 'v_score', 'j_score',
                            'vgene_probablity',
                            'dregion_reading_frame', 'cdr1_length', 'cdr2_length', 'cdr3_length',
                            'functionality_comment',
                            'rev_comp', 'vgene_probability']
            
        elif 'V-DOMAIN Functionality' in Summary_column_list:
            
            df_1 = Summary_1[['Sequence ID','V-GENE and allele', 'J-GENE and allele', 'D-GENE and allele', 'V-DOMAIN Functionality',
                              'V-REGION score', 'J-REGION score','V-REGION identity %', 'D-REGION reading frame', 
                              'CDR1-IMGT length','CDR2-IMGT length', 'CDR3-IMGT length', 'V-DOMAIN Functionality comment', 
                              'Orientation','V-REGION identity %']]
            
            df_1.columns = ['seq_name','v_string', 'j_string', 'd_string', 'functional', 'v_score', 'j_score',
                            'vgene_probablity',
                            'dregion_reading_frame', 'cdr1_length', 'cdr2_length', 'cdr3_length',
                            'functionality_comment',
                            'rev_comp', 'vgene_probability']
            
        df_2 = IMGT_gapped_nt_sequences_2[['V-D-J-REGION','V-J-REGION','V-REGION','J-REGION','FR1-IMGT','FR2-IMGT','FR3-IMGT',
                                           'FR4-IMGT','CDR1-IMGT','CDR2-IMGT','CDR3-IMGT','JUNCTION']]
        
        df_2.columns = ['vdjregion_sequence_nt_gapped','vjregion_sequence_nt_gapped','vregion_sequence_nt_gapped',
                        'jregion_sequence_nt_gapped','fr1region_sequence_nt_gapped','fr2region_sequence_nt_gapped',
                        'fr3region_sequence_nt_gapped','fr4region_sequence_nt_gapped','cdr1region_sequence_nt_gapped',
                        'cdr2region_sequence_nt_gapped','cdr3region_sequence_nt_gapped','junction_sequence_nt_gapped']

        df_3 = Nt_sequences_3[['V-D-J-REGION','V-J-REGION','D-J-REGION','V-REGION','J-REGION','D-REGION','FR1-IMGT','FR2-IMGT',
                               'FR3-IMGT','FR4-IMGT','CDR1-IMGT','CDR2-IMGT','CDR3-IMGT','JUNCTION','V-D-J-REGION start',
                               'V-D-J-REGION end','V-J-REGION start','V-J-REGION end','V-REGION start','V-REGION end',
                               'J-REGION start','J-REGION end','D-REGION start','D-REGION end','FR1-IMGT start','FR1-IMGT end',
                               'FR2-IMGT start','FR2-IMGT end','FR3-IMGT start','FR3-IMGT end','FR4-IMGT start','FR4-IMGT end',
                               'CDR1-IMGT start','CDR1-IMGT end','CDR2-IMGT start','CDR2-IMGT end','CDR3-IMGT start',
                               'CDR3-IMGT end','JUNCTION start','JUNCTION end','D-J-REGION start','D-J-REGION end']]

        df_3.columns = ['vdjregion_sequence_nt','vjregion_sequence_nt','djregion_sequence_nt','vregion_sequence_nt',
                        'jregion_sequence_nt','dregion_sequence_nt','fr1region_sequence_nt','fr2region_sequence_nt',
                        'fr3region_sequence_nt','fr4region_sequence_nt','cdr1region_sequence_nt','cdr2region_sequence_nt',
                        'cdr3region_sequence_nt','junction_nt','vdjregion_start','vdjregion_end','vjregion_start',
                        'vjregion_end','v_start','v_end','j_start','j_end','d_start',
                        'd_end','fwr1_start','fwr1_end','fwr2_start','fwr2_end','fwr3_start',
                        'fwr3_end','fwr4_start','fwr4_end','cdr1_start','cdr1_end',
                        'cdr2_start','cdr2_end','cdr3_start','cdr3_end','junction_start',
                        'junction_end','djregion_start','djregion_end']

        df_4 = IMGT_gapped_AA_sequences_4[['V-D-J-REGION','V-J-REGION','V-REGION','J-REGION','FR1-IMGT','FR2-IMGT','FR3-IMGT',
                                           'FR4-IMGT','CDR1-IMGT','CDR2-IMGT','CDR3-IMGT','JUNCTION']]

        df_4.columns = ['vdjregion_sequence_aa_gapped','vjregion_sequence_aa_gapped','vregion_sequence_aa_gapped',
                        'jregion_sequence_aa_gapped','fr1region_sequence_aa_gapped','fr2region_sequence_aa_gapped',
                        'fr3region_sequence_aa_gapped','fr4region_sequence_aa_gapped','cdr1region_sequence_aa_gapped',
                        'cdr2region_sequence_aa_gapped','cdr3region_sequence_aa_gapped','junction_sequence_aa_gapped']

        df_5 = AA_sequences_5[['V-D-J-REGION','V-J-REGION','V-REGION','J-REGION','FR1-IMGT','FR2-IMGT','FR3-IMGT','FR4-IMGT',
                               'CDR1-IMGT','CDR2-IMGT','CDR3-IMGT','JUNCTION']]

        df_5.columns = ['vdjregion_sequence_aa','vjregion_sequence_aa','vregion_sequence_aa','jregion_sequence_aa',
                        'fr1region_sequence_aa','fr2region_sequence_aa','fr3region_sequence_aa','fr4region_sequence_aa',
                        'cdr1region_sequence_aa','cdr2region_sequence_aa','cdr3region_sequence_aa','junction_aa']

        df_7 = V_REGION_mutation_and_AA_change_table_7[['V-REGION','FR1-IMGT','FR2-IMGT','FR3-IMGT','CDR1-IMGT','CDR2-IMGT',
                                                        'CDR3-IMGT']]

        df_7.columns = ['vregion_mutation_string','fr1region_mutation_string','fr2region_mutation_string',
                        'fr3region_mutation_string','cdr1region_mutation_string','cdr2region_mutation_string',
                        'cdr3region_mutation_string']

        df_concat = pd.concat([df_1, df_2, df_3, df_4, df_5, df_7], axis=1)
        df_concat['annotation_date'] = Para_dict['Date']
        df_concat['tool_version'] = Para_dict['IMGT/V-QUEST programme version']
        df_concat['reference_version'] = Para_dict['IMGT/V-QUEST reference directory release']
        df_concat['species'] = Para_dict['Species']
        df_concat['receptor_type'] = Para_dict['Receptor type or locus']
        df_concat['reference_directory_set'] = Para_dict['IMGT/V-QUEST reference directory set']
        df_concat['search_insert_delete'] = Para_dict['Search for insertions and deletions']
        df_concat['no_nucleotide_to_add'] = Para_dict[
            "Nb of nucleotides to add (or exclude) in 3' of the V-REGION for the evaluation of the alignment score"]
        df_concat['no_nucleotide_to_exclude'] = Para_dict[
            "Nb of nucleotides to exclude in 5' of the V-REGION for the evaluation of the nb of mutations"]
        df_concat = df_concat.where((pd.notnull(df_concat)), "")
        df_concat['cdr1_length'] = df_concat['cdr1region_sequence_aa'].apply(len)
        df_concat['cdr2_length'] = df_concat['cdr2region_sequence_aa'].apply(len)
        df_concat['cdr3_length'] = df_concat['cdr3region_sequence_aa'].apply(len)

        sampleid = self.context.samples.find({"imgt_file_name":{'$regex': filename}},{'_id':1})

        ir_project_sample_id = [i['_id'] for i in sampleid][0]

        df_concat['ir_project_sample_id']=ir_project_sample_id
        df_concat['substring'] = df_concat['junction_aa'].apply(get_substring)
        #     df_concat['substring'] = df_concat['cdr3region_sequence_aa'].apply(get_substring)
        df_concat['v_call'] = df_concat['v_string'].apply(str).apply(setGene)
        df_concat['j_call'] = df_concat['j_string'].apply(str).apply(setGene)
        df_concat['d_call'] = df_concat['d_string'].apply(str).apply(setGene)
        df_concat['junction_length'] = df_concat['junction_nt'].apply(len)
        df_concat['junction_length_aa'] = df_concat['junction_aa'].apply(len)
        
        records = json.loads(df_concat.T.to_json()).values()
        
        # The climax: insert the records into the MongoDb collection!
        self.context.sequences.insert(records)
        
        ir_sequence_count = len(records)
        
        #     self.context.samples.update_one({"imgt_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":0}})
        ori_count = self.context.samples.find_one({"imgt_file_name":{'$regex': filename}},{"ir_sequence_count":1})["ir_sequence_count"]

        self.context.samples.update({"imgt_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":ir_sequence_count+ori_count}}, multi=True)

        #     self.context.samples.update_one({"imgt_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":ir_sequence_count+ori_count}})

        # Clean up annotation files
        filelist = [ f for f in os.listdir( self.dataFolder() ) if f.endswith(".txt") ]
        for f in filelist: os.remove(f)
        
        return True
    