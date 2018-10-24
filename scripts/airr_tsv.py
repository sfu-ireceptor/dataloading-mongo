# Script for loading MIXCR formatted annotation file 
# into an iReceptor data node MongoDb database
#

from os.path import isfile

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
        # Open the file (it it exists) and get a rearrangment object iterator
        if not isfile(path):
            print("Could not open AIRR TSV file ", path)
            return False
        file_handle = open(path, 'r')
        airr_data = airr.read_rearrangement(file_handle)
        # Print out the fields in the AIRR TSV file
        # print(airr_data.fields)
        # print(airr_data.external_fields)
        # Validate the AIRR TSV file and confirm if loading is desired if 
        # the file is not valid. Note, the validate method uses the
        # iterator to iterate to the end of the file. So once validated
        # the iterator can not be used again and the file is at the end
        # of the file. We need to open a new handle and create a new iterator
        # to procee the file.
        if airr_data.validate():
                if self.context.verbose:
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
        if self.context.verbose:
            print("Processing raw data frame...")
        airr_df = airr.load_rearrangement(file_handle)

        # Build the substring array that allows index for fast searching of
        # Junction AA substrings.
        if 'junction_aa' in airr_df:
            if self.context.verbose:
                print("Retrieving junction amino acids and building substrings...")
            airr_df['substring'] = airr_df['junction_aa'].apply(Parser.get_substring)

            # The AIRR TSV format doesn't have AA length, we want it in our repository.
            if not ('junction_aa_length' in airr_df):
                if self.context.verbose:
                    print("Computing junction amino acids length...")
                airr_df['junction_aa_length'] = airr_df['junction_aa'].apply(str).apply(len)

        # Build the v_call field, as an array if there is more than one gene
        # assignment made by the annotator.
        Parser.processGene(self.context, airr_df, "v_call", "v_call", "vgene_gene", "vgene_family")
        Parser.processGene(self.context, airr_df, "j_call", "j_call", "jgene_gene", "jgene_family")
        Parser.processGene(self.context, airr_df, "d_call", "d_call", "dgene_gene", "dgene_family")

        # For now we assume that an AIRR TSV file, when loaded into iReceptor, has
        # been produced by igblast. This in general is not the case, but as a loader
        # script we assume this to be the case.
        if self.context.verbose:
            print("Setting annotation tool to be igblast...")
        airr_df['ir_annotation_tool'] = 'igblast'

        # Get root filename: may need to strip off any gzip 'archive' file extension
        filename = self.context.filename.replace(".gz","")
        if self.context.verbose:
            print("For igblast filename: "+filename)

        # Query for the sample and create an array of sample IDs
        if self.context.verbose:
            print("Retrieving associated sample...")
        samples_cursor = self.context.samples.find({"igblast_file_name":{'$regex': filename}},{'_id':1})
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
        airr_df['ir_project_sample_id']=ir_project_sample_id
        if self.context.verbose:
            print("Inserting into sample ID", ir_project_sample_id)

        # Get the number of sequences we want to insert
        count_row = len(airr_df.index)
        # Assign a block size of number of records to insert at a time
        num_to_insert = 10000
        # Calculate how many iterations and the remainder
        (runNumber,rest)= divmod(count_row,num_to_insert)
        if self.context.verbose:
            print("count_row = ", count_row, ", runNumber = ", runNumber, ", rest = ", rest)
        print("Inserting ",runNumber+1," batches in MongoDb sequence collection")
        for i in range(runNumber+1):
            print("Inserting records", num_to_insert*i, "to", num_to_insert*(i+1))
            df_insert = airr_df.iloc[num_to_insert*i:num_to_insert*(i+1)]
            records = json.loads(df_insert.T.to_json()).values()
            self.context.sequences.insert_many(records)
 
        if self.context.verbose:
            print("Updating sequence count")
        if self.context.counter == 'reset':
            ori_count = 0
        else:
            ori_count = self.context.samples.find_one({"igblast_file_name":{'$regex': filename}},{"ir_sequence_count":1})["ir_sequence_count"]

        self.context.samples.update({"igblast_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":count_row+ori_count}}, multi=True)
        # self.context.samples.update_one({"mixcr_file_name":{'$regex': filename}},{"$set" : {"ir_sequence_count":count_row}})

        print("igblast data loading complete for file: "+filename)
        file_handle.close()
        return True;
