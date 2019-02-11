# Script for loading MIXCR formatted annotation file 
# into an iReceptor data node MongoDb database
#

from os.path import isfile

import pandas as pd
import time
import json
import gzip
import airr

from parser import Parser

class AIRR_TSV(Parser):
    
    def __init__( self, context ):
        Parser.__init__(self,context)

    # We nedd to convert the AIRR TSV T/F to our mechanism for storing functionality
    # which is 1/0 
    def functional_boolean(self, functionality):
        if functionality or functionality == "T":
            return 1
        else:
            return 0

    def process(self):

        # This first iteration just reads one AIRR TSV file
        # at a time, given the full file (path) name
        # e.g. SRR4084213_aa_AIRR_annotation.tsv
        # May also be gzip compressed file
        
        # Open, decompress then read(), if it is a gz archive
        if self.context.path.endswith(".gz"):
            if self.context.verbose:
                print("Info: Reading data gzip archive: "+self.context.path)
            with gzip.open(self.context.path, 'rb') as f:
                # read file directly from the file handle 
                # (Panda read_table call handles this...)
                success = self.processAIRRTSVFile(f)

        else: # read directly as a regular text file
            if self.context.verbose:
                print("Info: Reading text file: "+self.context.path)
            success = self.processAIRRTSVFile(self.context.path)

        return success

    def processAIRRTSVFile( self, path ):
        # Set the tag for the repository that we are using. Note this should
        # be refactored so that it is a parameter provided so that we can use
        # multiple repositories.
        repository_tag = self.context.repository_tag

        # Set the size of each chunk of data that is inserted.
        chunk_size = self.context.repository_chunk

        # Validate the AIRR TSV file and confirm if loading is desired if 
        # the file is not valid. Note that this function processes the entire
        # file and validates all of the data. For a large file this could be
        # expensive. 
        if airr.validate_rearrangement(path):
                print("Info: File", path, "is a valid AIRR TSV file")
        else:
                print("Warning: File", path, "is NOT a valid AIRR TSV file")

        # Extract the fields that are of interest for this file. Essentiall all non null mixcr fields
        field_of_interest = self.context.airr_map.airr_rearrangement_map['igblast'].notnull()

        # We select the rows in the mapping that contain fields of interest for MiXCR.
        # At this point, file_fields contains N columns that contain our mappings for the
        # the specific formats (e.g. ir_id, airr, vquest). The rows are limited to have
        # only data that is relevant to igblast
        file_fields = self.context.airr_map.airr_rearrangement_map.loc[field_of_interest]

        # We need to build the set of fields that the repository can store. We don't
        # want to extract fields that the repository doesn't want.
        igblastColumns = []
        columnMapping = {}
        if self.context.verbose:
            print("Info: Dumping AIRR mapping")
        for index, row in file_fields.iterrows():
            if self.context.verbose:
                print("Info:    " + str(row['igblast']) + " -> " + str(row[repository_tag]))
            # If the repository column has a value for the igblast field, track the field
            # from both the igblast and repository side.
            if not pd.isnull(row[repository_tag]):
                igblastColumns.append(row['igblast'])
                columnMapping[row['igblast']] = row[repository_tag]
            else:
                print("Info:     Repository does not support " +
                      str(row['igblast']) + ", not inserting into repository")

        # Get a new rearrangement reader so we can get the field names in the file.
        if self.context.verbose:
            print("Info: Processing raw data frame...")
        airr_reader = airr.read_rearrangement(path)

        # Determing the mapping from the file input to the repository.
        finalMapping = {}
        for airr_field in airr_reader.fields:
            if airr_field in columnMapping:
                if self.context.verbose:
                    print("Info: AIRR field in file: " + airr_field + " -> " + columnMapping[airr_field])
                finalMapping[airr_field] = columnMapping[airr_field]
            else:
                if self.context.verbose:
                    print("Info: No mapping for input AIRR TSV field " + airr_field +
                          ", adding to repository without mapping.")

        # Determine if we are missing any repository columns from the input data.
        for igblast_column, mongo_column in columnMapping.items():
            if not igblast_column in airr_reader.fields:
                if self.context.verbose:
                    print("Info: Missing data in input AIRR file for " + igblast_column)

        # Get root filename: may need to strip off any gzip 'archive' file extension
        filename = self.context.filename.replace(".gz","")

        # Get the sample ID of the data we are processing. We use the IMGT file name for
        # this at the moment, but this may not be the most robust method.
        value = self.context.airr_map.getMapping("ir_rearrangement_file_name", "ir_id", repository_tag)
        idarray = []
        if value is None:
            print("ERROR: Could not find ir_rearrangement_file_name in repository " + repository_tag)
            return False
        else:
            if self.context.verbose:
                print("Info: Retrieving associated sample for file " + filename + " from repository field " + value)
            idarray = Parser.getSampleIDs(self.context, value, filename)

        # Check to see that we found it and that we only found one. Fail if not.
        num_samples = len(idarray)
        if num_samples == 0:
            print("ERROR: Could not find annotation file", filename)
            print("ERROR: No sample could be associated with this annotation file.")
            return False
        elif num_samples > 1:
            print("ERROR: Annotation file can not be associated with a unique sample, found", num_samples)
            print("ERROR: Unique assignment of annotations to a single sample are required.")
            return False
            
        # We found a unique sample, keep track of it for later. 
        ir_project_sample_id = idarray[0]

        # Create a reader for the data frame with step size "chunk_size"
        airr_df_reader = pd.read_table(path, chunksize=chunk_size)

        # Iterate over the file with data frames of size "chunk_size"
        total_records = 0
        for airr_df in airr_df_reader:
            # Remap the column names. We need to remap because the columns may be in a differnt
            # order in the file than in the column mapping.
            airr_df.rename(finalMapping, axis='columns', inplace=True)

            # Build the substring array that allows index for fast searching of
            # Junction AA substrings.
            if 'junction_aa' in airr_df:
                if self.context.verbose:
                    print("Info: Retrieving junction amino acids and building substrings...", flush=True)
                airr_df['substring'] = airr_df['junction_aa'].apply(Parser.get_substring)

                # The AIRR TSV format doesn't have AA length, we want it in our repository.
                if not ('junction_aa_length' in airr_df):
                    if self.context.verbose:
                        print("Info: Computing junction amino acids length...", flush=True)
                    airr_df['junction_aa_length'] = airr_df['junction_aa'].apply(str).apply(len)

            # Check to see if we have a productive field (later versions of AIRR TSV). If
            # so conver to our repositories boolean storage mechanism. Similarly if the
            # older AIRR TSV version of the functional field is present, handle that as well.
            if 'productive' in airr_df:
                airr_df['functional'] = airr_df['productive'].apply(self.functional_boolean)
            elif 'functional' in airr_df:
                airr_df['functional'] = airr_df['functional'].apply(self.functional_boolean)

            # Build the v_call field, as an array if there is more than one gene
            # assignment made by the annotator.
            Parser.processGene(self.context, airr_df, "v_call", "v_call", "vgene_gene", "vgene_family")
            Parser.processGene(self.context, airr_df, "j_call", "j_call", "jgene_gene", "jgene_family")
            Parser.processGene(self.context, airr_df, "d_call", "d_call", "dgene_gene", "dgene_family")
            # If we don't already have a locus (that is the data file didn't provide one) then
            # calculate the locus based on the v_call array.
            if not 'locus' in airr_df:
                airr_df['locus'] = airr_df['v_call'].apply(Parser.getLocus)

            # For now we assume that an AIRR TSV file, when loaded into iReceptor, has
            # been produced by igblast. This in general is not the case, but as a loader
            # script we assume this to be the case.
            if self.context.verbose:
                print("Info: Setting annotation tool to be igblast...", flush=True)
            airr_df['ir_annotation_tool'] = 'igblast'

            # Keep track of the sample id so can link each rearrangement to a repertoire
            airr_df['ir_project_sample_id']=ir_project_sample_id

            # Create the created and update values for this block of records. Note that this
            # means that each block of inserts will have the same date.
            now_str = Parser.getDateTimeNowUTC()
            airr_df["ir_created_at"] = now_str
            airr_df["ir_updated_at"] = now_str

            # Insert the chunk of records into Mongo.
            num_records = len(airr_df)
            print("Info: Inserting", num_records, "records into Mongo...", flush=True)
            t_start = time.perf_counter()
            records = json.loads(airr_df.T.to_json()).values()
            self.context.sequences.insert_many(records)
            t_end = time.perf_counter()
            print("Info: Inserted records, time =", (t_end - t_start), "seconds", flush=True)

            # Keep track of the total number of records processed.
            total_records = total_records + num_records

 
        # Get the number of annotations for this repertoire (as defined by the ir_project_sample_id)
        if self.context.verbose:
            print("Info: Getting the number of annotations for this repertoire")
        annotation_count = self.context.sequences.find(
                {"ir_project_sample_id":{'$eq':ir_project_sample_id}}
            ).count()
        if self.context.verbose:
            print("Annotation count = %d" % (annotation_count), flush=True)

        # Set the cached ir_sequeunce_count field for the repertoire/sample.
        self.context.samples.update(
            {"_id":ir_project_sample_id}, {"$set": {"ir_sequence_count":annotation_count}}
        )

        # Inform on what we added and the total count for the this record.
        print("Info: Inserted %d records, total annotation count = %d" % (total_records, annotation_count))

        return True;
