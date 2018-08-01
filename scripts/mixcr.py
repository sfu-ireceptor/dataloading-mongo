# Script for loading MIXCR formatted annotation file 
# into an iReceptor data node MongoDb database
#

import sys
import pandas as pd
import json
import gzip
import time

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
            print("Reading data gzip archive: "+self.context.path)
            with gzip.open(self.context.path, 'rb') as f:
                # read file directly from the file handle 
                # (Panda read_table call handles this...)
                self.processMiXcrFile(f)

        else: # read directly as a regular text file
            print("Reading text file: "+self.context.path)
            self.processMiXcrFile(self.context.path)

        return True

    def processMiXcrFile( self, path ):

        # Read in the MiXCR columns we care about from the MiXCR file
        print("Reading in table...", flush=True)
        mixcrColumns = ['bestVHit','bestDHit','bestJHit', 'bestVHitScore', 'bestDHitScore', 'bestJHitScore', 'nSeqCDR3','aaSeqCDR3', 'readSequence', 'descrR1' ]
        df = pd.read_table(path, usecols=mixcrColumns)

        print("Processing raw data frame...", flush=True)
        mongoColumns = ['v_call', 'd_call', 'j_call', 'v_score', 'd_score', 'j_score', 'junction','junction_aa', 'sequence', 'sequence_id']
        df.columns = mongoColumns

        # Build the substring array that allows index for fast searching of
        # Junction AA substrings.
        print("Retrieving junction amino acids...", flush=True)
        df['substring'] = df['junction_aa'].apply(Parser.get_substring)

        # MiXCR doesn't have junction length, we want it in our repository.
        print("Computing junction length...", flush=True)
        df['junction_length'] = df['junction'].apply(str).apply(len)

        print("Computing junction amino acids length...", flush=True)
        df['junction_aa_length'] = df['junction_aa'].apply(str).apply(len)

        # Build the v_call field, as an array if there is more than one gene
        # assignment made by the annotator.
        print("Constructing v_call array from v_call", flush=True)
        df['v_call'] = df['v_call'].apply(Parser.setGene)

        # Build the vgene_gene field (with no allele)
        print("Constructing vgene_gene from v_call", flush=True)
        df['vgene_gene'] = df['v_call'].apply(Parser.setGeneGene)

        # Build the vgene_family field (with no allele and no gene)
        print("Constructing vgene_family from v_call", flush=True)
        df['vgene_family'] = df['v_call'].apply(Parser.setGeneFamily)

        # Build the d_call field, as an array if there is more than one gene
        # assignment made by the annotator.
        print("Constructing d_call array from d_call", flush=True)
        df['d_call'] = df['d_call'].apply(Parser.setGene)

        # Build the dgene_gene field (with no allele)
        print("Constructing dgene_gene from d_call", flush=True)
        df['dgene_gene'] = df['d_call'].apply(Parser.setGeneGene)

        # Build the dgene_family field (with no allele and no gene)
        print("Constructing dgene_family from d_call", flush=True)
        df['dgene_family'] = df['d_call'].apply(Parser.setGeneFamily)

        # Build the j_call field, as an array if there is more than one gene
        # assignment made by the annotator.
        print("Constructing j_call array from j_call", flush=True)
        df['j_call'] = df['j_call'].apply(Parser.setGene)

        # Build the jgene_gene field (with no allele)
        print("Constructing jgene_gene from j_call", flush=True)
        df['jgene_gene'] = df['j_call'].apply(Parser.setGeneGene)

        # Build the jgene_family field (with no allele and no gene)
        print("Constructing jgene_family from j_call", flush=True)
        df['jgene_family'] = df['j_call'].apply(Parser.setGeneFamily)

        df['functional'] = 'productive'
        df['annotation_tool'] = 'MiXCR'

        # Get root filename: may need to strip off any gzip 'archive' file extension
        filename = self.context.filename.replace(".gz","")
        print("For MiXCR filename: "+filename, flush=True)

        print("Retrieving associated sample...", flush=True)
        sampleid = self.context.samples.find({"mixcr_file_name":{'$regex': filename}},{'_id':1})

        ir_project_sample_id = [i['_id'] for i in sampleid][0]
        df['ir_project_sample_id']=ir_project_sample_id

        count_row = len(df.index)
        num_to_insert = 100000
        (runNumber,rest)= divmod(count_row,num_to_insert)

        print("Inserting ", count_row, " records in MongoDb sequence collection")
        for i in range(runNumber+1):
            t_start = time.perf_counter()
            df_insert = df.iloc[num_to_insert*i:num_to_insert*(i+1)]
            records = json.loads(df_insert.T.to_json()).values()
            self.context.sequences.insert_many(records)
            t_end = time.perf_counter()
            print("Processing records", num_to_insert*i, "to", num_to_insert*(i+1), "took", (t_end - t_start), "seconds")
            sys.stdout.flush()

        print("Updating sequence count", flush=True)
        if self.context.counter == 'reset':
            ori_count = 0
        else:
            ori_count = self.context.samples.find_one({"mixcr_file_name":{'$regex': filename}},{"ir_sequence_count":1})["ir_sequence_count"]

        self.context.samples.update({"mixcr_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":count_row+ori_count}}, multi=True)
        # self.context.samples.update_one({"mixcr_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":count_row}})

        print("MiXCR data loading complete for file: "+filename, flush=True)
        
