# Script for loading MIXCR formatted annotation file 
# into an iReceptor data node MongoDb database
#

import sys
import os.path
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
        success = True

        # Check to see if the file exists and return if not.
        if not os.path.isfile(self.context.path):
            print("Could not open MiXCR file ", self.context.path)
            return False

        # Get root filename from the path, should be a file if the path is file, so not checking again 8-)
        filename = os.path.basename(self.context.path)

        if self.context.path.endswith(".gz"):
            print("Reading data gzip archive: "+self.context.path)
            with gzip.open(self.context.path, 'rb') as file_handle:
                # read file directly from the file handle 
                # (Panda read_table call handles this...)
                success = self.processMiXcrFile(file_handle, filename)

        else: # read directly as a regular text file
            print("Reading text file: "+self.context.path)
            file_handle = open(self.context.path, "r")
            success = self.processMiXcrFile(file_handle, filename)

        return success

    def processMiXcrFile( self, file_handle, filename ):

        # Define the number of records to iterate over
        chunk_size = 100000

        # Define the columns from the MiXCR file we want
        mixcrColumns = ['bestVHit','bestDHit','bestJHit',
                        'bestVHitScore', 'bestDHitScore', 'bestJHitScore',
                        'nSeqCDR3','aaSeqCDR3', 'readSequence', 'descrR1' ]

        # Define the mapping of MiXCR columns to Mongo repository columns
        mongoColumns = ['v_call', 'd_call', 'j_call',
                        'v_score', 'd_score', 'j_score',
                        'junction','junction_aa', 'sequence', 'sequence_id']

        # Query for the sample and create an array of sample IDs
        print("Retrieving associated sample for file", filename)
        samples_cursor = self.context.samples.find({"mixcr_file_name":{'$regex': filename}},{'_id':1})
        idarray = [sample['_id'] for sample in samples_cursor]

        # Check to see that we found it and that we only found one. Fail if not.
        num_samples = len(idarray)
        if num_samples == 0:
            print("Could not find annotation file", filename)
            print("No sample could be associated with this annotation file.")
            return False
        elif num_samples > 1:
            print("Annotation file can not be associated with a unique sample, found", num_samples)
            print("Unique assignment of annotations to a single sample are required.")
            return False

        # Get the sample ID and assign it to sample ID field
        ir_project_sample_id = idarray[0]

	# Get a Pandas reader iterator for the file.
        print("Preparing the file reader...", flush=True)
        df_reader = pd.read_table(file_handle, usecols=mixcrColumns, chunksize=chunk_size)

        # Iterate over the file a chunk at a time. Each chunk is a data frame.
        total_records = 0
        for df_chunk in df_reader:

            print("Processing raw data frame...", flush=True)
            df_chunk.columns = mongoColumns

            # Build the substring array that allows index for fast searching of
            # Junction AA substrings. Also calculate junction AA length
            if 'junction_aa' in df_chunk:
                print("Computing junction amino acids substrings...", flush=True)
                df_chunk['substring'] = df_chunk['junction_aa'].apply(Parser.get_substring)
                print("Computing junction amino acids length...", flush=True)
                df_chunk['junction_aa_length'] = df_chunk['junction_aa'].apply(str).apply(len)

            # MiXCR doesn't have junction length, we want it in our repository.
            if 'junction' in df_chunk:
                print("Computing junction length...", flush=True)
                df_chunk['junction_length'] = df_chunk['junction'].apply(str).apply(len)


            # Build the v_call field, as an array if there is more than one gene
            # assignment made by the annotator.
            if 'v_call' in df_chunk:
                print("Constructing v_call array from v_call", flush=True)
                df_chunk['v_call'] = df_chunk['v_call'].apply(Parser.setGene)

                # Build the vgene_gene field (with no allele)
                print("Constructing vgene_gene from v_call", flush=True)
                df_chunk['vgene_gene'] = df_chunk['v_call'].apply(Parser.setGeneGene)

                # Build the vgene_family field (with no allele and no gene)
                print("Constructing vgene_family from v_call", flush=True)
                df_chunk['vgene_family'] = df_chunk['v_call'].apply(Parser.setGeneFamily)

            # Build the d_call field, as an array if there is more than one gene
            # assignment made by the annotator.
            if 'd_call' in df_chunk:
                print("Constructing d_call array from d_call", flush=True)
                df_chunk['d_call'] = df_chunk['d_call'].apply(Parser.setGene)

                # Build the dgene_gene field (with no allele)
                print("Constructing dgene_gene from d_call", flush=True)
                df_chunk['dgene_gene'] = df_chunk['d_call'].apply(Parser.setGeneGene)

                # Build the dgene_family field (with no allele and no gene)
                print("Constructing dgene_family from d_call", flush=True)
                df_chunk['dgene_family'] = df_chunk['d_call'].apply(Parser.setGeneFamily)

            # Build the j_call field, as an array if there is more than one gene
            # assignment made by the annotator.
            if 'j_call' in df_chunk:
                print("Constructing j_call array from j_call", flush=True)
                df_chunk['j_call'] = df_chunk['j_call'].apply(Parser.setGene)

                # Build the jgene_gene field (with no allele)
                print("Constructing jgene_gene from j_call", flush=True)
                df_chunk['jgene_gene'] = df_chunk['j_call'].apply(Parser.setGeneGene)

                # Build the jgene_family field (with no allele and no gene)
                print("Constructing jgene_family from j_call", flush=True)
                df_chunk['jgene_family'] = df_chunk['j_call'].apply(Parser.setGeneFamily)

            # Assign each record the constant fields for all records in the chunk
            df_chunk['functional'] = 'productive'
            df_chunk['annotation_tool'] = 'MiXCR'
            df_chunk['ir_project_sample_id'] = ir_project_sample_id

            # Insert the chunk of records into Mongo.
            num_records = len(df_chunk)
            print("Inserting", num_records, "into Mongo...")
            t_start = time.perf_counter()
            records = json.loads(df_chunk.T.to_json()).values()
            self.context.sequences.insert_many(records)
            t_end = time.perf_counter()
            print("Inserted records, time =", (t_end - t_start), "seconds", flush=True)

            # Keep track of the total number of records processed.
            total_records = total_records + num_records

        print("Updating sequence count", flush=True)
        if self.context.counter == 'reset':
            original_count = 0
        else:
            original_count = self.context.samples.find_one({"mixcr_file_name":{'$regex': filename}},{"ir_sequence_count":1})["ir_sequence_count"]

        self.context.samples.update({"mixcr_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":total_records+original_count}}, multi=True)

        print("MiXCR data loading complete for file: "+filename, flush=True)
        return True
        
