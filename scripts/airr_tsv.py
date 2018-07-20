# Script for loading MIXCR formatted annotation file 
# into an iReceptor data node MongoDb database
#

import pandas as pd
import json
import gzip
import airr

from parser import Parser

class AIRR_TSV(Parser):
    
    def __init__( self, context ):
        Parser.__init__(self,context)

    def process(self):

        # This first iteration just reads one AIRR TSV file
        # at a time, given the full file (path) name
        # e.g. SRR4084213_aa_AIRR_annotation.tsv
        # May also be gzip compressed file
        
        # Open, decompress then read(), if it is a gz archive
        if self.context.path.endswith(".gz"):
            print("Reading data gzip archive: "+self.context.path)
            with gzip.open(self.context.path, 'rb') as f:
                # read file directly from the file handle 
                # (Panda read_table call handles this...)
                success = self.processAIRRTSVFile(f)

        else: # read directly as a regular text file
            print("Reading text file: "+self.context.path)
            success = self.processAIRRTSVFile(self.context.path)

        return success

    def processAIRRTSVFile( self, path ):

        print("Reading in AIRR TSV file...")
        # Open the file and get a rearrangment object iterator
        file_handle = open(path, 'r')
        airr_data = airr.read_rearrangement(file_handle)
        # Print out the fields in the AIRR TSV file
        print(airr_data.fields)
        print(airr_data.external_fields)
        # Validate the AIRR TSV file and confirm if loading is desired if 
        # the file is not valid. Note, the validate method uses the
        # iterator to iterate to the end of the file. So once validated
        # the iterator can not be used again and the file is at the end
        # of the file. We need to open a new handle and create a new iterator
        # to procee the file.
        if airr_data.validate():
                print("File", path, "is a valid AIRR TSV file")
        else:
                print("### WARNING: File", path, "is NOT a valid AIRR TSV file")
                while True:
                        decision = input("### Are you sure you want to load this data into the repository? (Yes/No):")
                        if decision.upper().startswith('N'):
                                return False
                        elif decision.upper().startswith('Y'):
                                break

        # Get a new file handle and iterartor to process the file.
        file_handle = open(path, 'r')
        print("Processing raw data frame...")
        airr_df = airr.load_rearrangement(file_handle)

        # Build the substring index for fast searching of Junction AA substrings.
        print("Retrieving junction amino acids and building substrings...")
        airr_df['substring'] = airr_df['junction_aa'].apply(Parser.get_substring)

        # The AIRR TSV format doesn't have AA length, we want it in our repository.
        print("Computing junction amino acids length...")
        airr_df['junction_aa_length'] = airr_df['junction_aa'].apply(str).apply(len)

        # For now we assume that an AIRR TSV file, when loaded into iReceptor, has
        # been produced by igblast. This in general is not the case, but as a loader
        # script we assume this to be the case.
        print("Setting annotation tool to be igblast...")
        airr_df['annotation_tool'] = 'igblast'

        print (airr_df.columns)

        # Get root filename: may need to strip off any gzip 'archive' file extension
        filename = self.context.filename.replace(".gz","")
        print("For igblast filename: "+filename)

        print("Retrieving associated sample...")
        sampleid = self.context.samples.find({"igblast_file_name":{'$regex': filename}},{'_id':1})

        ir_project_sample_id = [i['_id'] for i in sampleid][0]
        airr_df['ir_project_sample_id']=ir_project_sample_id
        print("ir_project_sample_id = ", ir_project_sample_id)

        count_row = len(airr_df.index)
        num_to_insert = 10000
        (runNumber,rest)= divmod(count_row,num_to_insert)
        print("count_row = ", count_row, ", runNumber = ", runNumber, ", rest = ", rest)
        print("Inserting ",runNumber+1," batches in MongoDb sequence collection")
        for i in range(runNumber+1):
            print("Inserting batch ", i)
            df_insert = airr_df.iloc[10000*i:10000*(i+1)]
            records = json.loads(df_insert.T.to_json()).values()
            self.context.sequences.insert_many(records)

        print("Updating sequence count")
        if self.context.counter == 'reset':
            ori_count = 0
        else:
            ori_count = self.context.samples.find_one({"igblast_file_name":{'$regex': filename}},{"ir_sequence_count":1})["ir_sequence_count"]

        self.context.samples.update({"igblast_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":count_row+ori_count}}, multi=True)
        # self.context.samples.update_one({"mixcr_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":count_row}})

        print("igblast data loading complete for file: "+filename)
        file_handle.close()
        #print("AIRR TSV data not yet written to Mongo")
        return True;
