# Script for loading MIXCR formatted annotation file 
# into an iReceptor data node MongoDb database

import sys
import os.path
import pandas as pd
import json
import gzip
import time

from rearrangement import Rearrangement

class MiXCR(Rearrangement):
    
    def __init__( self, verbose, repository_tag, repository_chunk, airr_map, repository):
        Rearrangement.__init__(self, verbose, repository_tag, repository_chunk, airr_map, repository)
        # The annotation tool used for the MiXCR parser is of course MiXCR
        self.setAnnotationTool("MiXCR")
        # The default column in the AIRR Mapping file is mixcr. This can be 
        # overrideen by the user should they choose to use a differnt set of 
        # columns from the file.
        self.setFileMapping("mixcr")

    def process(self, filewithpath):

        # This reads one MiXCR file at a time, given the full file (path) name
        # May also be gzip compressed file
        
        # Open, decompress then read(), if it is a gz archive
        success = True

        # Check to see if the file exists and return if not.
        if not os.path.isfile(filewithpath):
            print("ERROR: Could not open MiXCR file ", filewithpath)
            return False

        # Get root filename from the path, should be a file if the path is file, so not checking again 8-)
        filename = os.path.basename(filewithpath)

        if filewithpath.endswith(".gz"):
            if self.verbose():
                print("Info: Reading data gzip archive: "+filewithpath)
            with gzip.open(filewithpath, 'rb') as file_handle:
                # read file directly from the file handle 
                # (Pandas read_csv call handles this...)
                success = self.processMiXcrFile(file_handle, filename)

        else: # read directly as a regular text file
            if self.verbose():
                print("Info: Reading text file: "+filewithpath)
            file_handle = open(filewithpath, "r")
            success = self.processMiXcrFile(file_handle, filename)

        return success

    def processMiXcrFile( self, file_handle, filename ):

        # Get the AIRR Map object for this class (for convenience).
        airr_map = self.getAIRRMap()

        # Set the tag for the repository that we are using. Note this should
        # be refactored so that it is a parameter provided so that we can use
        # multiple repositories.
        repository_tag = self.getRepositoryTag()

        # Set the tag for the file mapping that we are using. Ths is essentially the
        # look up into the columns of the AIRR Mapping that we are using. 
        filemap_tag = self.getFileMapping()

        # Define the number of records to iterate over
        chunk_size = self.getRepositoryChunkSize()

        # Query for the sample and create an array of sample IDs
        filename = filename.replace(".gz", "")

        # Get the sample ID of the data we are processing. We use the IMGT file name for
        # this at the moment, but this may not be the most robust method.
        value = airr_map.getMapping("ir_rearrangement_file_name", "ir_id", repository_tag)
        idarray = []
        if value is None:
            print("ERROR: Could not find ir_rearrangement_file_name in repository " + repository_tag)
            return False
        else:
            if self.verbose():
                print("Info: Retrieving associated repertoire for file " + filename + " from repository field " + value)
            idarray = self.repositoryGetRepertoireIDs(value, filename)


        # Check to see that we found it and that we only found one. Fail if not.
        num_samples = len(idarray)
        if num_samples == 0:
            print("ERROR: Could not find sample in repository with annotation file", filename)
            print("ERROR: No sample could be associated with this annotation file.")
            return False
        elif num_samples > 1:
            print("ERROR: Annotation file can not be associated with a unique sample in the repository, found", num_samples)
            print("ERROR: Unique assignment of annotations to a single sample are required.")
            return False

        # Get the sample ID and assign it to sample ID field
        ir_project_sample_id = idarray[0]

        # Extract the fields that are of interest for this file. Essentiall all non null fields
        # in the file. This is a boolean array that is T everywhere there is a notnull field in
        # the column of interest.
        map_column = airr_map.getRearrangementMapColumn(filemap_tag)
        fields_of_interest = map_column.notnull()

        # We select the rows in the mapping that contain fields of interest for MiXCR.
        # At this point, file_fields contains N columns that contain our mappings for the
        # the specific formats (e.g. ir_id, airr, vquest). The rows are limited to have
        # only data that is relevant to MiXCR
        file_fields = airr_map.getRearrangementRows(fields_of_interest)

        # We need to build the set of fields that the repository can store. We don't
        # want to extract fields that the repository doesn't want.
        mixcrColumns = []
        columnMapping = {}
        if self.verbose():
            print("Info: Dumping expected " + self.getAnnotationTool() + "(" + filemap_tag +
                  ") to repository mapping")
        for index, row in file_fields.iterrows():
            if self.verbose():
                print("Info:    " + str(row[filemap_tag]) + " -> " + str(row[repository_tag]))
            # If the repository column has a value for the IMGT field, track the field
            # from both the IMGT and repository side.
            if not pd.isnull(row[repository_tag]):
                mixcrColumns.append(row[filemap_tag])
                columnMapping[row[filemap_tag]] = row[repository_tag]
            else:
                if self.verbose():
                    print("Info:    Repository does not support " +
                          str(row[filemap_tag]) + ", not inserting into repository")

	# Get a Pandas reader iterator for the file. When reading the file we only want to
        # read in the mixcrColumns we care about. We want to read in only a fixed number of 
        # records so we don't have any memory contraints reading really large files. And
        # we don't want to map empty strings to Pandas NaN values. This causes an issue as
        # missing strings get read as a NaN value, which is interpreted as a string. One can
        # then not tell the difference between a "nan" string and a "NAN" Junction sequence.
        if self.verbose():
            print("Info: Preparing the file reader...", flush=True)
        df_reader = pd.read_csv(file_handle, sep='\t', chunksize=chunk_size, na_filter=False)

        # Iterate over the file a chunk at a time. Each chunk is a data frame.
        total_records = 0
        for df_chunk in df_reader:

            if self.verbose():
                print("Info: Processing raw data frame...", flush=True)
            # Remap the column names. We need to remap because the columns may be in a differnt
            # order in the file than in the column mapping. We leave any non-mapped columns in the
            # data frame as we don't want to discard data.
            for mixcr_column in df_chunk.columns:
                if mixcr_column in columnMapping:
                    mongo_column = columnMapping[mixcr_column]
                    if self.verbose():
                        print("Info: Mapping " + self.getAnnotationTool() + " field in file: " +
                              mixcr_column + " -> " + mongo_column)
                    df_chunk.rename({mixcr_column:mongo_column}, axis='columns', inplace=True)
                else:
                    if self.verbose():
                        print("Info: No mapping for " + self.getAnnotationTool() + " input file column " +
                              mixcr_column + ", storing in repository as is")
            # Check to see which desired MiXCR mappings we don't have...
            for mixcr_column, mongo_column in columnMapping.items():
                if not mongo_column in df_chunk.columns:
                    if self.verbose():
                        print("Info: Missing data in input " + self.getAnnotationTool() + 
                              " file for " + mixcr_column)
            
            # Build the substring array that allows index for fast searching of
            # Junction AA substrings. Also calculate junction AA length
            junction_aa = airr_map.getMapping("junction_aa", "ir_id", repository_tag)
            ir_substring = airr_map.getMapping("ir_substring", "ir_id", repository_tag)
            ir_junction_aa_length = airr_map.getMapping("ir_junction_aa_length", "ir_id", repository_tag)
            if junction_aa in df_chunk:
                if self.verbose():
                    print("Info: Computing junction amino acids substrings...", flush=True)
                df_chunk[ir_substring] = df_chunk[junction_aa].apply(Rearrangement.get_substring)
                if self.verbose():
                    print("Info: Computing junction amino acids length...", flush=True)
                df_chunk[ir_junction_aa_length] = df_chunk[junction_aa].apply(str).apply(len)

            # MiXCR doesn't have junction nucleotide length, we want it in our repository.
            junction = airr_map.getMapping("junction", "ir_id", repository_tag)
            junction_length = airr_map.getMapping("junction_length", "ir_id", repository_tag)
            if junction in df_chunk:
                if self.verbose():
                    print("Info: Computing junction length...", flush=True)
                df_chunk[junction_length] = df_chunk[junction].apply(str).apply(len)


            # We need to look up the "known parameter" from an iReceptor perspective (the field
            # name in the "ir_id" column mapping and map that to the correct field name for the
            # repository we are writing to.
            v_call = airr_map.getMapping("v_call", "ir_id", repository_tag)
            d_call = airr_map.getMapping("d_call", "ir_id", repository_tag)
            j_call = airr_map.getMapping("j_call", "ir_id", repository_tag)
            ir_vgene_gene = airr_map.getMapping("ir_vgene_gene", "ir_id", repository_tag)
            ir_dgene_gene = airr_map.getMapping("ir_dgene_gene", "ir_id", repository_tag)
            ir_jgene_gene = airr_map.getMapping("ir_jgene_gene", "ir_id", repository_tag)
            ir_vgene_family = airr_map.getMapping("ir_vgene_family", "ir_id", repository_tag)
            ir_dgene_family = airr_map.getMapping("ir_dgene_family", "ir_id", repository_tag)
            ir_jgene_family = airr_map.getMapping("ir_jgene_family", "ir_id", repository_tag)

            # Build the v_call field, as an array if there is more than one gene
            # assignment made by the annotator.
            self.processGene(df_chunk, v_call, v_call, ir_vgene_gene, ir_vgene_family)
            self.processGene(df_chunk, j_call, j_call, ir_jgene_gene, ir_jgene_family)
            self.processGene(df_chunk, d_call, d_call, ir_dgene_gene, ir_dgene_family)
            # If we don't already have a locus (that is the data file didn't provide one) then
            # calculate the locus based on the v_call array.
            locus = airr_map.getMapping("locus", "ir_id", repository_tag)
            if not locus in df_chunk:
                df_chunk[locus] = df_chunk[v_call].apply(Rearrangement.getLocus)

            # Assign each record the constant fields for all records in the chunk
            productive = airr_map.getMapping("productive", "ir_id", repository_tag)
            df_chunk[productive] = 1
            # Assign any iReceptor specific custom fields for the records in the chunk
            ir_annotation_tool = airr_map.getMapping("ir_annotation_tool", "ir_id", repository_tag)
            df_chunk[ir_annotation_tool] = self.getAnnotationTool()
            ir_project_sample_id_field = airr_map.getMapping("ir_project_sample_id", "ir_id", repository_tag)
            df_chunk[ir_project_sample_id_field] = ir_project_sample_id
            # Create the created and update values for this block of records. Note that this
            # means that each block of inserts will have the same date.
            now_str = Rearrangement.getDateTimeNowUTC()
            ir_created_at = airr_map.getMapping("ir_created_at", "ir_id", repository_tag)
            ir_updated_at = airr_map.getMapping("ir_updated_at", "ir_id", repository_tag)
            df_chunk[ir_created_at] = now_str
            df_chunk[ir_updated_at] = now_str

            # Insert the chunk of records into Mongo.
            num_records = len(df_chunk)
            print("Info: Inserting", num_records, "records into Mongo...", flush=True)
            t_start = time.perf_counter()
            records = json.loads(df_chunk.T.to_json()).values()
            self.repositoryInsertRearrangements(records)
            t_end = time.perf_counter()
            print("Info: Inserted records, time =", (t_end - t_start), "seconds", flush=True)

            # Keep track of the total number of records processed.
            total_records = total_records + num_records
            print("Info: Total records so far =", total_records, flush=True)

        # Get the number of annotations for this repertoire (as defined by the ir_project_sample_id)
        if self.verbose():
            print("Info: Getting the number of annotations for this repertoire")
        annotation_count = self.repositoryCountRearrangements(ir_project_sample_id)

        # Set the cached ir_sequeunce_count field for the repertoire/sample.
        self.repositoryUpdateCount(ir_project_sample_id, annotation_count)

        # Inform on what we added and the total count for the this record.
        print("Info: Inserted %d records, total annotation count = %d" % (total_records, annotation_count))
 
        return True
        
