# Script for loading MIXCR formatted annotation file 
# into an iReceptor data node MongoDb database

import sys
import os.path
import pandas as pd
import numpy as np 
import json
import gzip
import time

from rearrangement import Rearrangement
from parser import Parser

class Adaptive(Rearrangement):
    
    # Static method do map Adaptive missing data values to AIRR 
    # null values.
    @staticmethod
    def mapMissingDatatoEmptyString(field_value):
        # Adaptive has four different values that mean data is missing, 
        # we need to map these to null.
        if field_value in ["na","unknown","no data","unresolved"]:
            return ""
        else:
            return field_value

    # Static method do map Adaptive gene data values to AIRR values.
    @staticmethod
    def mapAdaptiveGene(resolved, allele_ties, gene_ties, family_ties):
        new_resolved = resolved
        if not (allele_ties == "no data" or allele_ties==""):
            # If there is an allele tie, then resolved is correct, and we
            # need to split the resolved into the resolved gene and add the alleles.
            # e.g. v_resolved = TCRBV02-01, v_allele_ties = 01,03
            # becomes: TCRBV02-01*01, TCRBV02-01*03
            new_resolved = ""
            # Create an array of alleles
            alleles = allele_ties.split(",")
            count = 0
            # Build a new resolved string comma separated, with alleles
            for allele in alleles:
                # Comma separated for all but the first.
                if count > 0:
                    new_resolved = new_resolved + ","
                # Concatenate the resolved gene and the allele
                new_resolved = new_resolved + resolved + "*" + allele.strip()
                count = count + 1
        elif not (gene_ties == "no data" or gene_ties =="") and not (resolved == "unknown" or resolved=="") :
            # If there is a gene_tie then we need to handle the case where one of
            # the genes has a / in it. As far as I can tell this is redundant data
            # in the examples that I have seen so we just throw away the stuff after
            # the /. 
            # E.g. v_resolved = TCRBV12, v_gene_ties = TCRBV12-03/12-04,TCRBV12-04
            # becomes TCRBV12-03,TCRBV12-04
            new_resolved = ""
            # Split the gene ties on comma
            genes = gene_ties.split(",")
            count = 0
            # For each gene...
            for gene in genes:
                # Comma separated except for the first.
                if count > 0:
                    new_resolved = new_resolved + ","
                # If there is a /, fix it by throwing away everthing after it.
                gene_fixed = gene
                if "/" in gene:
                    gene_list = gene.split("/")
                    gene_fixed = gene_list[0]
                # Add the fixed gene
                new_resolved = new_resolved + gene_fixed.strip()
                count = count + 1
        elif not (gene_ties == "no data" or gene_ties == "") and (resolved == "unknown" or resolved == "") :
            # Handle no v_resolved and gene_ties.
            # E.g. d_resolved = unknown, d_gene_ties = TCRBD01-01,TCRBD02-01
            # becomes TCRBD01-01,TCRBD02-01
            new_resolved = gene_ties
        elif "/" in resolved:
            # Handle a / in resolved with none of the other cases occuring
            # E.g. TCRBV12-03/12-04*01
            # becomes TCRBV12-03*01, TCRBV12-04*01
            new_resolved = ""
            # Get the allele to be used for all genes (last three characters)
            allele = resolved[len(resolved)-3:]
            # The stuff after the / doesn't have a locus so we need it. Remember that
            # Adaptive loci are 5 chars (e.g. TCRBV)
            locus = resolved[0:5]
            # Get rid of the allele
            base_string = resolved.replace(allele, "")
            # Get rid of the locus
            base_string = base_string.replace(locus, "")
            # Now we are left with a / separated list of gene numbers.
            resolve_list = base_string.split("/")
            count = 0
            for gene in resolve_list:
                # If it is not the first then we have to add the locus part,
                # otherwise we just add the allele value.
                if count > 0:
                    new_resolved = new_resolved + "," + locus.strip() + gene + allele.strip()
                else:
                    new_resolved = locus.strip() + gene + allele.strip()
                count = count + 1
            
        # Return the newly resolved value.
        return new_resolved

    # Static method to convert an Adaptive frame_type field to a
    # true/false productive value.
    @staticmethod
    def mapProductive(frame_type):
        # Only "In" for "in frame" are productive
        if frame_type == "In":
            return True
        else:
            return False

    # Static method to convert an Adaptive frame_type field to a
    # true/false stop_codon value
    @staticmethod
    def mapStopCodon(frame_type):
        # If frame_type contains "Stop" it contains a stop_codon.
        # If it is in frame, then there is no stop codon. 
        # Otherwise return None as we don't know.
        if frame_type == "Stop":
            return True
        elif frame_type == "In":
            return False
        else:
            return None

    # Static method to convert an Adaptive frame_type field to a
    # true/false vj_in_frame value
    @staticmethod
    def mapInFrame(frame_type):
        # If frame_type contains "In" then vj_in_frame is True
        # If it is "Out" then vj_in_frame is False
        # Otherwise we don't know what it is so return Null
        if frame_type == "In":
            return True
        elif frame_type == "Out":
            return False
        else:
            return None

    # Static method to convert an Adaptive gene call to something that is
    # consistent with IMGT nomenclature. Sheesh, this is UGLY!!!
    @staticmethod
    def convertGeneCall(gene_call):
        # Change the TCR with TR as per IMGT nomenclature
        gene_call = gene_call.replace("TCR", "TR")
        # Handle the incorrect mapping of orphon gene names
        gene_call = gene_call.replace("-or", "/OR")
        # Handle the leading 0 in orphon gene names
        gene_call = gene_call.replace("/OR0", "/OR")
        # Handle the use of _ rather than - in orphon gene names
        gene_call = gene_call.replace("_", "-")
        # Get rid of the 0 prefix in the gene if necessary. Note this has to 
        # be done after the previous step or we miss the 0s that are with orphons
        gene_call = gene_call.replace("-0", "-")
        # Get rid of the 0 prefix on the gene family if necessary
        gene_call = gene_call.replace("TRBV0", "TRBV")
        gene_call = gene_call.replace("TRAV0", "TRAV")
        gene_call = gene_call.replace("TRBD0", "TRBD")
        gene_call = gene_call.replace("TRAD0", "TRAD")
        gene_call = gene_call.replace("TRBJ0", "TRBJ")
        gene_call = gene_call.replace("TRAJ0", "TRAJ")

        gene_call = gene_call.replace("IGHV0", "IGHV")
        gene_call = gene_call.replace("IGLV0", "IGLV")
        gene_call = gene_call.replace("IGKV0", "IGKV")
        gene_call = gene_call.replace("IGHD0", "IGHD")
        gene_call = gene_call.replace("IGLD0", "IGLD")
        gene_call = gene_call.replace("IGKD0", "IGKD")
        gene_call = gene_call.replace("IGHJ0", "IGHJ")
        gene_call = gene_call.replace("IGLJ0", "IGLJ")
        gene_call = gene_call.replace("IGKJ0", "IGKJ")

        return gene_call
        

    def __init__( self, verbose, repository_tag, repository_chunk, airr_map, repository):
        Rearrangement.__init__(self, verbose, repository_tag, repository_chunk, airr_map, repository)
        # The annotation tool used for the Adaptive parser is of course Adaptive
        self.setAnnotationTool("Adaptive")
        # The default column in the AIRR Mapping file is mixcr. This can be 
        # overrideen by the user should they choose to use a differnt set of 
        # columns from the file.
        self.setFileMapping("adaptive")

    def process(self, filewithpath):

        # This reads one Adaptive file at a time, given the full file (path) name
        # May also be gzip compressed file
        
        # Open, decompress then read(), if it is a gz archive
        success = True

        # Check to see if the file exists and return if not.
        if not os.path.isfile(filewithpath):
            print("ERROR: Could not open Adaptive file ", filewithpath)
            return False

        # Get root filename from the path, should be a file if the path is file,
        # so not checking again 8-)
        filename = os.path.basename(filewithpath)

        if filewithpath.endswith(".gz"):
            if self.verbose():
                print("Info: Reading data gzip archive: "+filewithpath)
            with gzip.open(filewithpath, 'rb') as file_handle:
                # read file directly from the file handle 
                # (Pandas read_csv call handles this...)
                success = self.processAdaptiveFile(file_handle, filename)

        else: # read directly as a regular text file
            if self.verbose():
                print("Info: Reading text file: "+filewithpath)
            file_handle = open(filewithpath, "r")
            success = self.processAdaptiveFile(file_handle, filename)

        return success

    def processAdaptiveFile( self, file_handle, filename ):

        # Start a timer for performance reasons.
        t_start_full = time.perf_counter()

        # Get the AIRR Map object for this class (for convenience).
        airr_map = self.getAIRRMap()

        # Set the tag for the repository that we are using.
        repository_tag = self.getRepositoryTag()
        # Get the tag to use for iReceptor specific mappings
        ireceptor_tag = self.getiReceptorTag()
        # Set the tag for the AIRR column
        airr_tag = self.getAIRRTag()

        # Get the fields to use for finding repertoire IDs, either using those IDs
        # directly or by looking for a repertoire ID based on a rearrangement file
        # name.
        repertoire_link_field = self.getRepertoireLinkIDField()
        rearrangement_link_field = self.getAnnotationLinkIDField()

        # Set the tag for the file mapping that we are using. Ths is essentially the
        # look up into the columns of the AIRR Mapping that we are using. 
        filemap_tag = self.getFileMapping()

        # Define the number of records to iterate over
        chunk_size = self.getRepositoryChunkSize()

        # Get the single, unique repertoire link id for the filename we are loading. If
        # we can't find one, this is an error and we return failure.
        repertoire_link_id = self.getRepertoireInfo(filename)
        if repertoire_link_id is None:
            print("ERROR: Could not link file %s to a valid repertoire"%(filename))
            return False

        # Get the column of values from the AIRR tag. We only want the
        # Rearrangement related fields.
        map_column = self.getAIRRMap().getIRRearrangementMapColumn(airr_tag)
        # Get a boolean column that flags columns of interest. Exclude nulls.
        fields_of_interest = map_column.notnull()
        # Afer the following airr_fields contains N columns (e.g. iReceptor, AIRR)
        # that contain the AIRR Repertoire mappings.
        airr_fields = self.getAIRRMap().getIRRearrangementRows(fields_of_interest)

        # Extract the fields that are of interest for this file. Essentially all non
        # null fields in the file. This is a boolean array that is T everywhere there
        # is a notnull field in the column of interest.
        map_column = airr_map.getIRRearrangementMapColumn(filemap_tag)
        fields_of_interest = map_column.notnull()

        # We select the rows in the mapping that contain fields of interest for Adaptive.
        # At this point, file_fields contains N columns that contain our mappings for
        # the specific formats (e.g. ireceptor, airr, vquest). The rows are limited to 
        # only data that is relevant to Adaptive
        file_fields = airr_map.getIRRearrangementRows(fields_of_interest)

        # We need to build the set of fields that the repository can store. We don't
        # want to extract fields that the repository doesn't want.
        columnMapping = {}
        if self.verbose():
            print("Info: Dumping expected %s (%s) to repository mapping"
                  %(self.getAnnotationTool(),filemap_tag))
        for index, row in file_fields.iterrows():
            if self.verbose():
                print("Info:    %s -> %s"
                      %(str(row[filemap_tag]), str(row[repository_tag])))
            # If the repository column has a value for the field, track the field
            if not pd.isnull(row[repository_tag]):
                columnMapping[row[filemap_tag]] = row[repository_tag]
            else:
                if self.verbose():
                    print("Info:    Repository does not support " +
                          str(row[filemap_tag]) + ", not inserting into repository")

	# Get a Pandas iterator for the file. When reading the file we only want to
        # read in the columns we care about. We want to read in only a fixed number of 
        # of records so we don't have any memory contraints reading really large files. 
        # And we don't want to map empty strings to Pandas NaN values. This causes an
        # issue as missing strings get read as a NaN value, which is interpreted as
        # a string. One can then not tell the difference between a "nan" string and
        # a "NAN" Junction sequence.
        if self.verbose():
            print("Info: Preparing the file reader...", flush=True)
        df_reader = pd.read_csv(file_handle, sep='\t', chunksize=chunk_size,
                                na_filter=False)

        # Iterate over the file a chunk at a time. Each chunk is a data frame.
        total_records = 0
        for df_chunk in df_reader:

            if self.verbose():
                print("Info: Processing raw data frame...", flush=True)
            # Remap the column names. We need to remap because the columns may be in 
            # a different order in the file than in the column mapping. We leave any
            # non-mapped columns in the data frame as we don't want to discard data.
            for file_column in df_chunk.columns:
                if file_column in columnMapping:
                    mongo_column = columnMapping[file_column]
                    if self.verbose():
                        print("Info: Mapping %s field in file: %s -> %s"
                              %(self.getAnnotationTool(), file_column, mongo_column))
                    df_chunk.rename({file_column:mongo_column},
                                    axis='columns', inplace=True)
                else:
                    new_column = "ad_" + file_column
                    if self.verbose():
                        print("Info: No mapping for %s column %s, storing as %s"
                              %(self.getAnnotationTool(), file_column, new_column))
                    df_chunk.rename({file_column:new_column},axis='columns',inplace=True)

            # Check to see which desired file mappings we don't have...
            for file_column, mongo_column in columnMapping.items():
                
                if mongo_column in df_chunk.columns:
                    df_chunk[mongo_column] = df_chunk[mongo_column].apply(
                                                 Adaptive.mapMissingDatatoEmptyString)
                else:
                    if self.verbose():
                        print("Info: Missing data in input %s file for %s"
                              %(self.getAnnotationTool(), file_column))
            
            # Build the substring array that allows index for fast searching of
            # Junction AA substrings. Also calculate junction AA length
            junction_aa = airr_map.getMapping("junction_aa",
                                              ireceptor_tag, repository_tag)
            ir_substring = airr_map.getMapping("ir_substring",
                                               ireceptor_tag, repository_tag)
            ir_junc_aa_len = airr_map.getMapping("ir_junction_aa_length",
                                               ireceptor_tag, repository_tag)
            if junction_aa in df_chunk:
                if self.verbose():
                    print("Info: Computing junction amino acids substrings...",
                          flush=True)
                # We want to process the junction to get rid of missing data. Adaptive
                # uses na in its junction column to indicate no junction we want this
                # to be an empty string.
                df_chunk[ir_substring] = df_chunk[junction_aa].apply(
                                                 Rearrangement.get_substring)
                if self.verbose():
                    print("Info: Computing junction amino acids length...", flush=True)
                df_chunk[ir_junc_aa_len] = df_chunk[junction_aa].apply(
                                                         Parser.len_null_to_null)

            # Adaptive doesn't have junction nucleotide length, we want it in our
            # repository.
            junction = airr_map.getMapping("junction", ireceptor_tag, repository_tag)
            junction_length = airr_map.getMapping("junction_length",
                                                  ireceptor_tag, repository_tag)
            if junction in df_chunk:
                if self.verbose():
                    print("Info: Computing junction length...", flush=True)
                df_chunk[junction_length] = df_chunk[junction].apply(str).apply(len)

            # We need to look up the field from an iReceptor perspective. We want the 
            # field name in the iReceptor column mapping and map that to the correct
            # field name for the repository we are writing to.
            v_call = airr_map.getMapping("v_call", ireceptor_tag, repository_tag)
            d_call = airr_map.getMapping("d_call", ireceptor_tag, repository_tag)
            j_call = airr_map.getMapping("j_call", ireceptor_tag, repository_tag)
            ir_vgene_gene = airr_map.getMapping("ir_vgene_gene",
                                                ireceptor_tag, repository_tag)
            ir_dgene_gene = airr_map.getMapping("ir_dgene_gene", 
                                                ireceptor_tag, repository_tag)
            ir_jgene_gene = airr_map.getMapping("ir_jgene_gene", 
                                                ireceptor_tag, repository_tag)
            ir_vgene_family = airr_map.getMapping("ir_vgene_family", 
                                                ireceptor_tag, repository_tag)
            ir_dgene_family = airr_map.getMapping("ir_dgene_family", 
                                                ireceptor_tag, repository_tag)
            ir_jgene_family = airr_map.getMapping("ir_jgene_family", 
                                                ireceptor_tag, repository_tag)

            # Process the v/d/j_call conversion. Adaptive does not use the IMGT 
            # nomenclature so we need to conver their v/d/j_call values to something
            # that is AIRR compatible.
            gene_df_chunk = df_chunk[[v_call,"ad_v_allele_ties",
                                     "ad_v_gene_ties","ad_v_family_ties"]]
            df_chunk[v_call] = gene_df_chunk.apply(
                              lambda x : Adaptive.mapAdaptiveGene(
                                             x[0], x[1], x[2], x[3]), axis=1)

            gene_df_chunk = df_chunk[[d_call,"ad_d_allele_ties",
                                     "ad_d_gene_ties","ad_d_family_ties"]]
            df_chunk[d_call] = gene_df_chunk.apply(
                              lambda x : Adaptive.mapAdaptiveGene(
                                             x[0], x[1], x[2], x[3]), axis=1)

            gene_df_chunk = df_chunk[[j_call,"ad_j_allele_ties",
                                     "ad_j_gene_ties","ad_j_family_ties"]]
            df_chunk[j_call] = gene_df_chunk.apply(
                              lambda x : Adaptive.mapAdaptiveGene(
                                             x[0], x[1], x[2], x[3]), axis=1)

            df_chunk[v_call] = df_chunk[v_call].apply(Adaptive.convertGeneCall)
            df_chunk[d_call] = df_chunk[d_call].apply(Adaptive.convertGeneCall)
            df_chunk[j_call] = df_chunk[j_call].apply(Adaptive.convertGeneCall)
            # Build the v_call field, as an array if there is more than one gene
            # assignment made by the annotator.
            self.processGene(df_chunk, v_call, v_call, ir_vgene_gene, ir_vgene_family)
            self.processGene(df_chunk, j_call, j_call, ir_jgene_gene, ir_jgene_family)
            self.processGene(df_chunk, d_call, d_call, ir_dgene_gene, ir_dgene_family)
            # If we don't already have a locus (that is the data file didn't provide
            # one) then calculate the locus based on the v_call array.
            locus = airr_map.getMapping("locus", ireceptor_tag, repository_tag)
            if not locus in df_chunk and v_call in df_chunk:
                df_chunk[locus] = df_chunk[v_call].apply(Rearrangement.getLocus)

            # Assign each record the constant fields for all records in the chunk
            # For Adaptive productive, stop_codon, and vj_in_frame can be calculated
            # from the "frame_type" field which is mapped to productive in the mapping.
            productive = airr_map.getMapping("productive",
                                             ireceptor_tag, repository_tag)
            vj_in_frame = airr_map.getMapping("vj_in_frame",
                                             ireceptor_tag, repository_tag)
            stop_codon = airr_map.getMapping("stop_codon",
                                             ireceptor_tag, repository_tag)
            if not stop_codon is None:
                df_chunk[stop_codon] = df_chunk[productive].apply(Adaptive.mapStopCodon)
            if not vj_in_frame is None:
                df_chunk[vj_in_frame] = df_chunk[productive].apply(Adaptive.mapInFrame)
            df_chunk[productive] = df_chunk[productive].apply(Adaptive.mapProductive)

            rep_rearrangement_link_field = airr_map.getMapping(
                                             rearrangement_link_field,
                                             ireceptor_tag, repository_tag)
            if not rep_rearrangement_link_field is None:
                df_chunk[rep_rearrangement_link_field] = repertoire_link_id
            else:
                print("ERROR: Could not get repertoire link field from AIRR mapping.")
                return False

            # Set the relevant IDs for the record being inserted. If it fails, don't 
            # load any data.
            if not self.checkIDFields(df_chunk, repertoire_link_id):
                return False

            # Check to make sure all AIRR required columns exist
            if not self.checkAIRRRequired(df_chunk, airr_fields):
                return False

            # Create the created and update values for this block of records. Note that
            # this means that each block of inserts will have the same date.
            now_str = Rearrangement.getDateTimeNowUTC()
            ir_created_at = airr_map.getMapping("ir_created_at_rearrangement", 
                                                ireceptor_tag, repository_tag)
            ir_updated_at = airr_map.getMapping("ir_updated_at_rearrangement",
                                                ireceptor_tag, repository_tag)
            df_chunk[ir_created_at] = now_str
            df_chunk[ir_updated_at] = now_str

            # Transform the data frame so that it meets the repository type requirements
            if not self.mapToRepositoryType(df_chunk,
                                            airr_map.getRearrangementClass(),
                                            airr_map.getIRRearrangementClass()):
                print("ERROR: Unable to map data to the repository")
                return False

            # Insert the chunk of records into Mongo.
            num_records = len(df_chunk)
            print("Info: Inserting", num_records, "records into Mongo...", flush=True)
            t_start = time.perf_counter()
            records = json.loads(df_chunk.T.to_json()).values()
            self.repositoryInsertRecords(records)
            t_end = time.perf_counter()
            print("Info: Inserted records, time =", (t_end - t_start),
                  "seconds", flush=True)

            # Keep track of the total number of records processed.
            total_records = total_records + num_records
            print("Info: Total records so far =", total_records, flush=True)

        # Get the number of annotations for this repertoire 
        if self.verbose():
            print("Info: Getting the number of annotations for this repertoire")
        annotation_count = self.repositoryCountRecords(repertoire_link_id)
        if annotation_count == -1:
            print("ERROR: invalid annotation count (%d), write failed." %
                  (annotation_count))
            return False

        # Set the cached ir_sequeunce_count field for the repertoire/sample.
        self.repositoryUpdateCount(repertoire_link_id, annotation_count)

        # Inform on what we added and the total count for the this record.
        t_end_full = time.perf_counter()
        print("Info: Inserted %d records, annotation count = %d, %f s, %f insertions/s" %
              (total_records, annotation_count, t_end_full - t_start_full,
              total_records/(t_end_full - t_start_full)), flush=True)

        return True
        
