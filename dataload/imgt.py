# Script for taking IMGT formatted annotation files
# extracted from a zip archive and loaded
# into an iReceptor data node MongoDb database
#
# Original script by Yang Zhou
# Adapted and revised by Richard Bruskiewich
#

import os
from os.path import isfile
from shutil import rmtree
import time
import re
import zipfile
import tarfile
import json
import pandas as pd

from parser import Parser

# IMGT has a "functionality" field which has a text string that indcates
# a functional annotation with the string "productive". Note that the 
# string sometimes contains "productinge (see comment..." so we need to
# check to ensure that the string starts with "productive".
def functional_boolean(functionality):
    if functionality.startswith("productive"):
        return 1
    else:
        return 0

class IMGT(Parser):
    def __init__(self, context):
        Parser.__init__(self, context)

    def process(self, filewithpath):

        # Check to see if the file exists.
        if not isfile(filewithpath):
            print("ERROR: Could not find IMGT compressed archive ", filewithpath)
            return False

        # Process the file...
        return self.processImgtArchive(filewithpath)

    def processImgtArchive(self, filewithpath):
        # Set the tag for the repository that we are using. Note this should
        # be refactored so that it is a parameter provided so that we can use
        # multiple repositories.
        repository_tag = self.context.repository_tag 

        # Get root filename from the path, should be a file if the path
        # is file, so not checking again 8-)
        fileName = os.path.basename(filewithpath)
        # Set the scratch folder based on the file name. This computes a
        # unique termporary folder in which we can uncompress and process
        # data in a safe way.
        self.setScratchFolder(filewithpath, fileName)

        if self.context.verbose:
            print("Info: Extracting IMGT file: ", fileName)
            print("Info: Path: ", filewithpath)
            print("Info: Scratch folder: ", self.getScratchFolder())

        # Get the sample ID of the data we are processing. We use the IMGT file name for
        # this at the moment, but this may not be the most robust method.
        value = self.context.airr_map.getMapping("ir_rearrangement_file_name", "ir_id", repository_tag)
        idarray = []
        if value is None:
            print("ERROR: Could not find ir_rearrangement_file_name in repository " + repository_tag)
            return False
        else:
            if self.context.verbose:
                print("Info: Retrieving associated sample for file " + fileName + " from repository field " + value)
            idarray = Parser.getSampleIDs(self.context, value, fileName)

        # Check to see that we found it and that we only found one. Fail if not.
        num_samples = len(idarray)
        if num_samples == 0:
            print("ERROR: Could not find annotation file " + fileName + " in the repository samples", flush=True)
            print("ERROR: No sample could be associated with this annotation file.", flush=True)
            return False
        elif num_samples > 1:
            print("ERROR: Annotation file can not be associated with a unique sample, found", num_samples, flush=True)
            print("ERROR: Unique assignment of annotations to a single sample are required.", flush=True)
            return False

        # Get the sample ID and assign it to sample ID field
        ir_project_sample_id = idarray[0]

        # Open the tar file, extract the data, and close the tarfile. 
        # This leaves us with a folder with all of the individual vQUest
        # files extracted in this location.
        tar = tarfile.open(filewithpath)
        tar.extractall(self.getScratchFolder())
        tar.close()

        # Get the list of relevant vQuest files. Choose the vquest_file column,
        # drop the NAs, and grab the unique members that remain. This gives us
        # the list of relevant vQuest file names from the configuration file
        # that we should be considering.
        vquest_file_map = self.context.airr_map.airr_rearrangement_map['vquest_file']
        vquest_files = vquest_file_map.dropna().unique()

        # Create a dictionary that stores an array of fields to process
        # for each IMGT file that we need to process.
        filedict = {}
        first_dataframe = True
        for vquest_file in vquest_files:
            if self.context.verbose:
                print("Info: Processing file ", vquest_file, flush=True)
            # Read in the data frame for the file.
            vquest_dataframe = self.readScratchDf(vquest_file, sep='\t')
            # Extract the fields that are of interest for this file.
            field_of_interest = self.context.airr_map.airr_rearrangement_map['vquest_file'].isin([vquest_file])
            # We select the rows in the mapping that contain fields of interest for this file.
            # At this point, file_fields contains N columns that contain our mappings for the
            # the specific formats (e.g. ir_id, airr, vquest). The rows are limited to have
            # only data that is relevant to this specific vquest file.
            file_fields = self.context.airr_map.airr_rearrangement_map.loc[field_of_interest]

            # We need to build the set of fields that the repository can store. We don't
            # want to extract fields that the repository doesn't want.
            vquest_fields = []
            mongo_fields = []
            for index, row in file_fields.iterrows():
                # If the repository column has a value for the IMGT field, track the field
                # from both the IMGT and repository side.
                if not pd.isnull(row[repository_tag]):
                    if self.context.verbose:
                        print("Info:    " + str(row['vquest']) + " -> " + str(row[repository_tag]), flush=True)
                    vquest_fields.append(row['vquest'])
                    mongo_fields.append(row[repository_tag])
                else:
                    if self.context.verbose:
                        print("Info:    Repository does not support " + vquest_file + "/" + 
                              str(row['vquest']) + ", not inserting into repository", flush=True)
            # Use the vquest column in our mapping to select the columns we want from the 
            # possibly quite large vquest data frame.
            mongo_dataframe = vquest_dataframe[vquest_fields]
            # We now have a data frame that has only the vquest data we want from this file. 
            # We now replace the vquest column names with the repository column names from
            # the map
            mongo_dataframe.columns = mongo_fields
            # We now have a data frame with vquest data in it with AIRR compliant column names.
            # Store all of this in a dictionay based on the file name so we can use it later.
            filedict[vquest_file] = {'vquest': file_fields['vquest'],
                                     repository_tag: file_fields[repository_tag],
                                     'vquest_dataframe': vquest_dataframe,
                                     'mongo_dataframe': mongo_dataframe}
            # Manage the data frames that we extract from each file. Essentially we 
            # just concatentate the data frames from each file into a single large
            # data frame.
            if first_dataframe:
                mongo_concat = mongo_dataframe
                vquest_concat = vquest_dataframe
                first_dataframe = False
            else:
                mongo_concat = pd.concat([mongo_concat, mongo_dataframe], axis=1)
                vquest_concat = pd.concat([vquest_concat, vquest_dataframe], axis=1)
                
        # We now have the data in a data frame with the correct headers mapped from the
        # IMGT data fields to the correct repository field names. Now we have to perform
        # any specific mappings that are specific to IMGT.
        if self.context.verbose:
            print("Info: Done building the initial data frame", flush=True) 

        # First, we want to keep track of some of the data from the IMGT Parameters file.
        # Create a dictionary with keys the first column of the parameter file and the values
        # the second column in the parameter file.
        Parameters_11 = self.readScratchDfNoHeader('11_Parameters.txt', sep='\t')
        parameter_dictionary = dict(zip(Parameters_11[0], Parameters_11[1]))

        # Need to grab some data out of the parameters dictionary.
        mongo_concat['annotation_date'] = parameter_dictionary['Date']
        mongo_concat['tool_version'] = parameter_dictionary['IMGT/V-QUEST programme version']
        mongo_concat['reference_version'] = parameter_dictionary[
            'IMGT/V-QUEST reference directory release']
        mongo_concat['species'] = parameter_dictionary['Species']
        mongo_concat['receptor_type'] = parameter_dictionary['Receptor type or locus']
        mongo_concat['reference_directory_set'] = parameter_dictionary[
            'IMGT/V-QUEST reference directory set']
        mongo_concat['search_insert_delete'] = parameter_dictionary[
            'Search for insertions and deletions']
        mongo_concat['no_nucleotide_to_add'] = parameter_dictionary[
            "Nb of nucleotides to add (or exclude) in 3' of the V-REGION for the evaluation of the alignment score"]
        mongo_concat['no_nucleotide_to_exclude'] = parameter_dictionary[
            "Nb of nucleotides to exclude in 5' of the V-REGION for the evaluation of the nb of mutations"]
        if self.context.verbose:
            print("Info: Done processing IMGT Parameter file", flush=True) 

        # Get rid of columns where the column is null.
        if self.context.verbose:
            print("Info: Cleaning up NULL columns", flush=True) 
        mongo_concat = mongo_concat.where((pd.notnull(mongo_concat)), "")

        # Critical iReceptor specific fields that need to be built from existing IMGT
        # generated fields.
        if self.context.verbose:
            print("Info: Setting up iReceptor specific fields", flush=True) 

        # IMGT annotates a rearrangement's functionality  with a string. We have a function
        # that takes the string and changes it to an integer 1/0 which the repository
        # expects. We want to keep the original data in case we need further interpretation,
        productive = self.context.airr_map.getMapping("productive", "ir_id", repository_tag)
        ir_productive = self.context.airr_map.getMapping("ir_productive", "ir_id", repository_tag)
        if productive in mongo_concat:
            mongo_concat[ir_productive] = mongo_concat[productive]
            mongo_concat[productive] = mongo_concat[productive].apply(functional_boolean)

        # The internal Mongo sample ID that links the sample to each sequence, constant
        # for all sequences in this file.
        ir_project_sample_id_field = self.context.airr_map.getMapping("ir_project_sample_id", "ir_id", repository_tag)
        mongo_concat[ir_project_sample_id_field] = ir_project_sample_id

        # The annotation tool used
        ir_annotation_tool = self.context.airr_map.getMapping("ir_annotation_tool", "ir_id", repository_tag)
        mongo_concat[ir_annotation_tool] = "V-Quest"

        # Generate the substring field, which we use to heavily optmiize junction AA
        # searches. Technically, this should probably be an ir_ field, but because
        # it is fundamental to the indexes that already exist, we won't change it for
        # now.
        if self.context.verbose:
            print("Info: Computing substring from junction", flush=True) 
        junction_aa = self.context.airr_map.getMapping("junction_aa", "ir_id", repository_tag)
        ir_substring = self.context.airr_map.getMapping("ir_substring", "ir_id", repository_tag)
        
        if junction_aa in mongo_concat:
            mongo_concat[ir_substring] = mongo_concat[junction_aa].apply(Parser.get_substring)

        # We want to keep the original vQuest vdj_string data, so we capture that in the
        # ir_vdjgene_string variables.
        # We need to look up the "known parameter" from an iReceptor perspective (the field
        # name in the "ir_id" column mapping and map that to the correct field name for the
        # repository we are writing to.
        v_call = self.context.airr_map.getMapping("v_call", "ir_id", repository_tag)
        d_call = self.context.airr_map.getMapping("d_call", "ir_id", repository_tag)
        j_call = self.context.airr_map.getMapping("j_call", "ir_id", repository_tag)
        ir_vgene_gene = self.context.airr_map.getMapping("ir_vgene_gene", "ir_id", repository_tag)
        ir_dgene_gene = self.context.airr_map.getMapping("ir_dgene_gene", "ir_id", repository_tag)
        ir_jgene_gene = self.context.airr_map.getMapping("ir_jgene_gene", "ir_id", repository_tag)
        ir_vgene_family = self.context.airr_map.getMapping("ir_vgene_family", "ir_id", repository_tag)
        ir_dgene_family = self.context.airr_map.getMapping("ir_dgene_family", "ir_id", repository_tag)
        ir_jgene_family = self.context.airr_map.getMapping("ir_jgene_family", "ir_id", repository_tag)
        ir_vgene_string = self.context.airr_map.getMapping("ir_vgene_string", "ir_id", repository_tag)
        ir_dgene_string = self.context.airr_map.getMapping("ir_dgene_string", "ir_id", repository_tag)
        ir_jgene_string = self.context.airr_map.getMapping("ir_jgene_string", "ir_id", repository_tag)
        mongo_concat[ir_vgene_string] = mongo_concat[v_call]
        mongo_concat[ir_jgene_string] = mongo_concat[j_call]
        mongo_concat[ir_dgene_string] = mongo_concat[d_call]
        # Process the IMGT VQuest v/d/j strings and generate the required columns the repository
        # needs, which are [vdj]_call, ir_[vdj]gene_gene, ir_[vdj]gene_family
        Parser.processGene(self.context, mongo_concat, ir_vgene_string, v_call, ir_vgene_gene, ir_vgene_family)
        Parser.processGene(self.context, mongo_concat, ir_jgene_string, j_call, ir_jgene_gene, ir_jgene_family)
        Parser.processGene(self.context, mongo_concat, ir_dgene_string, d_call, ir_dgene_gene, ir_dgene_family)
        # If we don't already have a locus (that is the data file didn't provide one) then
        # calculate the locus based on the v_call array.
        locus = self.context.airr_map.getMapping("locus", "ir_id", repository_tag)
        if not locus in mongo_concat:
            if self.context.verbose:
                print("Info: Computing locus from v_call", flush=True) 
            mongo_concat[locus] = mongo_concat[v_call].apply(Parser.getLocus)

        # Generate the junction length values as required.
        if self.context.verbose:
            print("Info: Computing junction lengths", flush=True) 
        junction = self.context.airr_map.getMapping("junction", "ir_id", repository_tag)
        junction_length = self.context.airr_map.getMapping("junction_length", "ir_id", repository_tag)
        if junction in mongo_concat and not junction_length in mongo_concat:
            mongo_concat[junction_length] = mongo_concat[junction].apply(len)
        # Special case for junction_aa_length. This does not exist in the AIRR standard,
        # so we have to check to see if the mapping returned None as well. 
        junction_aa = self.context.airr_map.getMapping("junction_aa", "ir_id", repository_tag)
        ir_junction_aa_length = self.context.airr_map.getMapping("ir_junction_aa_length", "ir_id", repository_tag)
        if junction_aa in mongo_concat and (ir_junction_aa_length is None or not ir_junction_aa_length in mongo_concat):
            mongo_concat[ir_junction_aa_length] = mongo_concat[junction_aa].apply(len)

        # Create the created and update values for this block of records. Note that this
        # means that each block of inserts will have the same date.
        now_str = Parser.getDateTimeNowUTC()
        ir_created_at = self.context.airr_map.getMapping("ir_created_at", "ir_id", repository_tag)
        ir_updated_at = self.context.airr_map.getMapping("ir_updated_at", "ir_id", repository_tag)
        mongo_concat[ir_created_at] = now_str
        mongo_concat[ir_updated_at] = now_str

        # Convert the mongo data frame dats int JSON.
        if self.context.verbose:
            print("Info: Creating JSON from Dataframe", flush=True) 
        t_start = time.perf_counter()
        records = json.loads(mongo_concat.T.to_json()).values()
        t_end = time.perf_counter()
        if self.context.verbose:
            print("Info: JSON created, time = %f seconds (%f records/s)" %((t_end - t_start),len(records)/(t_end - t_start)), flush=True)

        # The climax: insert the records into the MongoDb collection!
        if self.context.verbose:
            print("Info: Inserting %d records into the repository"%(len(records)), flush=True)
        t_start = time.perf_counter()
        self.context.sequences.insert(records)
        t_end = time.perf_counter()
        if self.context.verbose:
            print("Info: Inserted records, time = %f seconds (%f records/s)" %((t_end - t_start),len(records)/(t_end - t_start)), flush=True)

        # Get the number of annotations for this repertoire (as defined by the ir_project_sample_id)
        if self.context.verbose:
            print("Info: Getting the number of annotations for this repertoire", flush=True)
        t_start = time.perf_counter()
        annotation_count = self.context.sequences.find(
                {ir_project_sample_id_field:{'$eq':ir_project_sample_id}}
            ).count()
        t_end = time.perf_counter()
        if self.context.verbose:
            print("Info: Annotation count = %d, time = %f" % (annotation_count, (t_end - t_start)), flush=True)

        # Set the cached ir_sequeunce_count field for the repertoire/sample.
        self.context.samples.update(
            {"_id":ir_project_sample_id}, {"$set": {"ir_sequence_count":annotation_count}}
        )

        # Inform on what we added and the total count for the this record.
        print("Info: Inserted %d records, total annotation count = %d" % (len(records), annotation_count), flush=True)
        # Clean up annotation files and scratch folder
        if self.context.verbose:
            print("Info: Cleaning up scratch folder: ", self.getScratchFolder())
        rmtree(self.getScratchFolder())

        return True
