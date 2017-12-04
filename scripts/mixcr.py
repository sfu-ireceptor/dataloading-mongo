# Script for loading MIXCR formatted annotation file 
# into an iReceptor data node MongoDb database
#

import pandas as pd
import json
import gzip

from parser import Parser

class MiXCR(Parser):
    
    def __init__( self, context ):
        Parser.__init__(self,context)

    def process(self):

        # This first iteration just reads one MiXCR file
        # at a time, given the full file (path) name
        # e.g. SRR4084213_aa_mixcr_annotation.txt
        # May also be gzip compressed file
        
        # Open, decompress then read(), if it is a gz archive
        if self.context.path.endswith(".gz"):
            with gzip.open(self.context.path, 'rb') as f:
                # read file directly from the file handle 
                # (Panda read_table call handles this...)
                self.processMiXcrFile(f)

        else: # read directly as a regular text file
            self.processMiXcrFile(self.context.path)

        # Rebuild indices 
        print("Rebuilding sequence indices:")      
        print("junction_aa_length...")
        self.context.sequences.create_index("junction_aa_length")
        print("functional...")
        self.context.sequences.create_index("functional")
        print("ir_project_sample_id...")
        self.context.sequences.create_index("ir_project_sample_id")
                
        return True

    def processMiXcrFile( self, path ):

        df_raw = pd.read_table(path)

        df = df_raw[['bestVHit','bestDHit','bestJHit','bestVGene','bestDGene','bestJGene','bestVFamily','bestDFamily',
                   'bestJFamily','bestVHitScore','nSeqCDR3','aaSeqCDR3','descrR1']]

        df.columns = ['vgene', 'dgene', 'jgene', 'vgene_gene', 'dgene_gene', 'jgene_gene', 'vgene_family', 'dgene_family',
                       'jgene_family','v_score','junction','junction_aa', 'seqId']

        df['substring'] = df['junction_aa'].apply(Parser.get_substring)
        df['junction_length'] = df['junction'].apply(str).apply(len)
        df['junction_aa_length'] = df['junction_aa'].apply(str).apply(len)

        df['functional'] = 'productive'
        df['annotation_tool'] = 'MiXCR'

        sampleid = self.context.samples.find({"mixcr_file_name":{'$regex': filename}},{'_id':1})

        ir_project_sample_id = [i['_id'] for i in sampleid][0]
        df['ir_project_sample_id']=ir_project_sample_id

        count_row = len(df.index)
        num_to_insert = 10000

        (runNumber,rest)= divmod(count_row,num_to_insert)

        for i in range(runNumber+1):
            df_insert = df.iloc[10000*i:10000*(i+1)]
            records = json.loads(df_insert.T.to_json()).values()
            self.context.sequences.insert_many(records)

        if self.context.counter == 'reset':
            ori_count = 0
        else:
            ori_count = self.context.samples.find_one({"mixcr_file_name":{'$regex': filename}},{"ir_sequence_count":1})["ir_sequence_count"]

        self.context.samples.update({"mixcr_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":count_row+ori_count}}, multi=True)
        # self.context.samples.update_one({"mixcr_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":count_row}})
