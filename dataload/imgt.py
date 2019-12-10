# Script for taking IMGT formatted annotation files
# extracted from a zip archive and loaded
# into an iReceptor data node MongoDb database

import os
from os.path import isfile
from shutil import rmtree
import time
import re
import zipfile
import tarfile
import json
import pandas as pd

from rearrangement import Rearrangement

# IMGT has a "functionality" field which has a text string that indcates
# a functional annotation with the string "productive". Note that the 
# string sometimes contains "productinge (see comment..." so we need to
# check to ensure that the string starts with "productive".
def productive_boolean(functionality):
    if functionality.startswith("productive"):
        return True
    else:
        return False

# IMGT has a "rev_comp" equivalent field called orientation that
# maps the string "+" with "False" and "-" with "True". 
def rev_comp_boolean(orientation):
    if orientation == "+":
        return False
    elif orientation == "-":
        return True
    else:
        return None

# IMGT has a "vj_inframe" equivalent field called "Junction frame" that
# maps the string "in-frame" with "True" and "out-of-frame" with "False".
def vj_in_frame_boolean(frame):
    if frame == "out-of-frame":
        return False
    elif frame == "in-frame":
        return True
    else:
        return None


class IMGT(Rearrangement):
    def __init__(self, verbose, repository_tag, repository_chunk, airr_map, repository):
        Rearrangement.__init__(self, verbose, repository_tag, repository_chunk, airr_map, repository)
        # The annotation tool used for the IMGT parser is V-Quest
        self.setAnnotationTool("V-Quest")
        # The default column in the AIRR Mapping file is mixcr. This can be
        # overrideen by the user should they choose to use a differnt set of
        # columns from the file.
        self.setFileMapping("vquest")
        # IMGT also uses another file column from the AIRR Mapping file to specify
        # which IMGT file to use. IMGT V-Quest annotations come in a set of 11 Annotation
        # files so the parser needs to know which field is in which file.
        self.imgt_filename_map = "vquest_file"
        # IMGT also sometimes required computation to map an IMGT term to an AIRR term.
        # The mapping file has a boolean flag column that denotes whether a given AIRR
        # term requires computation. The column name to use for the mapping is below. 
        self.imgt_calculate_map = "vquest_calculate"


    def process(self, filewithpath):

        # Check to see if the file exists.
        if not isfile(filewithpath):
            print("ERROR: Could not find IMGT compressed archive ", filewithpath)
            return False

        # Process the file...
        return self.processImgtArchive(filewithpath)

    def processImgtArchive(self, filewithpath):
        # Get the AIRR Map object for this class (for convenience).
        airr_map = self.getAIRRMap()

        # Set the tag for the repository that we are using. Note this should
        # be refactored so that it is a parameter provided so that we can use
        # multiple repositories.
        repository_tag = self.getRepositoryTag()

        # Set the tag for the file mapping that we are using. Ths is essentially the
        # look up into the columns of the AIRR Mapping that we are using.
        filemap_tag = self.getFileMapping()

        # Set the tag for the calculation flag mapping that we are using. Ths is essentially
        # the look up into the columns of the AIRR Mapping that we are using for this.
        calculate_tag = self.imgt_calculate_map

        # Get root filename from the path, should be a file if the path
        # is file, so not checking again 8-)
        fileName = os.path.basename(filewithpath)
        # Set the scratch folder based on the file name. This computes a
        # unique termporary folder in which we can uncompress and process
        # data in a safe way.
        self.setScratchFolder(filewithpath, fileName)

        if self.verbose():
            print("Info: Extracting IMGT file: ", fileName)
            print("Info: Path: ", filewithpath)
            print("Info: Scratch folder: ", self.getScratchFolder())

        # Get the sample ID of the data we are processing. We use the IMGT file name for
        # this at the moment, but this may not be the most robust method.
        value = airr_map.getMapping("ir_rearrangement_file_name", "ir_id", repository_tag)
        idarray = []
        if value is None:
            print("ERROR: Could not find ir_rearrangement_file_name in repository " + repository_tag)
            return False
        else:
            if self.verbose():
                print("Info: Retrieving associated repertoire for file " + fileName + " from repository field " + value)
            idarray = self.repositoryGetRepertoireIDs(value, fileName)

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
        vquest_file_map = airr_map.getRearrangementMapColumn(self.imgt_filename_map)
        vquest_files = vquest_file_map.dropna().unique()

        # Create a dictionary that stores an array of fields to process
        # for each IMGT file that we need to process.
        filedict = {}
        first_dataframe = True
        # Arrays to keep track of the vquest fields we need to calculate on.
        vquest_calc_fields = []
        vquest_calc_file = []
        mongo_calc_fields = []
        for vquest_file in vquest_files:
            if self.verbose():
                print("Info: Processing file ", vquest_file, flush=True)
            # Read in the data frame for the file.
            vquest_dataframe = self.readScratchDf(vquest_file, sep='\t')
            # Extract the fields that are of interest for this file.
            imgt_file_column = airr_map.getRearrangementMapColumn(self.imgt_filename_map)
            fields_of_interest = imgt_file_column.isin([vquest_file])

            # We select the rows in the mapping that contain fields of interest for this file.
            # At this point, file_fields contains N columns that contain our mappings for the
            # the specific formats (e.g. ir_id, airr, vquest). The rows are limited to have
            # only data that is relevant to this specific vquest file.
            file_fields = airr_map.getRearrangementRows(fields_of_interest)

            # We need to build the set of fields that the repository can store. We don't
            # want to extract fields that the repository doesn't want.
            vquest_fields = []
            mongo_fields = []
            for index, row in file_fields.iterrows():
                # If the repository column has a value for the IMGT field, track the field
                # from both the IMGT and repository side.
                if not pd.isnull(row[repository_tag]):
                    # If no calculation is required, then it is a direct mapping.
                    # If there is calculation required, we perform those calculations
                    # later...
                    if row[calculate_tag] == 'FALSE':
                        if self.verbose():
                            print("Info:    %s  -> %s"%
                                  (str(row[filemap_tag]),
                                   str(row[repository_tag])),
                                  flush=True)
                        vquest_fields.append(row[filemap_tag])
                        mongo_fields.append(row[repository_tag])
                    else:
                        # The ones we need to calculate later we need to track...
                        vquest_calc_file.append(vquest_file)
                        vquest_calc_fields.append(row[filemap_tag])
                        mongo_calc_fields.append(row[repository_tag])
                        if pd.isnull(row[filemap_tag]):
                            print("Info:    COMBINED IMGT fields -> %s (Derived)"%
                                  (str(row[repository_tag])),
                                  flush=True)
                        else:
                            print("Info:    %s  -> %s (Derived)"%
                                  (str(row[filemap_tag]),
                                   str(row[repository_tag])),
                                  flush=True)
                else:
                    if self.verbose():
                        print("Info:    Repository does not support " + vquest_file + "/" + 
                              str(row[filemap_tag]) + ", not inserting into repository", flush=True)

            # Use the vquest column in our mapping to select the columns we want from the 
            # possibly quite large vquest data frame.
            mongo_dataframe = vquest_dataframe[vquest_fields]
            # We now have a data frame that has only the vquest data we want from this file. 
            # We now replace the vquest column names with the repository column names from
            # the map
            mongo_dataframe.columns = mongo_fields
            # We now have a data frame with vquest data in it with AIRR compliant column names.
            # Store all of this in a dictionay based on the file name so we can use it later.
            filedict[vquest_file] = {filemap_tag: file_fields[filemap_tag],
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
        if self.verbose():
            print("Info: Done building the initial data frame", flush=True) 

        # First, we want to keep track of some of the data from the IMGT Parameters file.
        # Create a dictionary with keys the first column of the parameter file and the values
        # the second column in the parameter file.
        Parameters_11 = self.readScratchDfNoHeader('11_Parameters.txt', sep='\t')
        parameter_dictionary = dict(zip(Parameters_11[0], Parameters_11[1]))

        # Need to grab some data out of the parameters dictionary. This is not really necessary
        # as this information should be stored in the repertoire metadata, but for completeness
        # we err on the side of having more information. Note that this is quite redundant as 
        # it is storing the same information for each rearrangement...
        mongo_concat['imgt_annotation_date'] = parameter_dictionary['Date']
        mongo_concat['imgt_tool_version'] = parameter_dictionary['IMGT/V-QUEST programme version']
        mongo_concat['imgt_reference_version'] = parameter_dictionary[
            'IMGT/V-QUEST reference directory release']
        mongo_concat['imgt_species'] = parameter_dictionary['Species']
        mongo_concat['imgt_receptor_type'] = parameter_dictionary['Receptor type or locus']
        mongo_concat['imgt_reference_directory_set'] = parameter_dictionary[
            'IMGT/V-QUEST reference directory set']
        mongo_concat['imgt_search_insert_delete'] = parameter_dictionary[
            'Search for insertions and deletions']
        mongo_concat['imgt_no_nucleotide_to_add'] = parameter_dictionary[
            "Nb of nucleotides to add (or exclude) in 3' of the V-REGION for the evaluation of the alignment score"]
        mongo_concat['imgt_no_nucleotide_to_exclude'] = parameter_dictionary[
            "Nb of nucleotides to exclude in 5' of the V-REGION for the evaluation of the nb of mutations"]
        if self.verbose():
            print("Info: Done processing IMGT Parameter file", flush=True) 

        # Get rid of columns where the column is null.
        if self.verbose():
            print("Info: Cleaning up NULL columns", flush=True) 
        mongo_concat = mongo_concat.where((pd.notnull(mongo_concat)), "")

        # Critical iReceptor specific fields that need to be built from existing IMGT
        # generated fields.
        if self.verbose():
            print("Info: Setting up iReceptor specific fields", flush=True) 

        for index, value in enumerate(mongo_calc_fields):
            if value == "productive":
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                imgt_name = "imgt_" + mongo_calc_fields[index]
                mongo_concat[imgt_name] = filedict[vquest_calc_file[index]]["vquest_dataframe"][vquest_calc_fields[index]]
                mongo_concat[mongo_calc_fields[index]] = mongo_concat[imgt_name].apply(productive_boolean)
            elif value == "rev_comp":
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                imgt_name = "imgt_" + mongo_calc_fields[index]
                mongo_concat[imgt_name] = filedict[vquest_calc_file[index]]["vquest_dataframe"][vquest_calc_fields[index]]
                mongo_concat[mongo_calc_fields[index]] = mongo_concat[imgt_name].apply(rev_comp_boolean)
            elif value == "vj_in_frame":
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                imgt_name = "imgt_" + mongo_calc_fields[index]
                mongo_concat[imgt_name] = filedict[vquest_calc_file[index]]["vquest_dataframe"][vquest_calc_fields[index]]
                mongo_concat[mongo_calc_fields[index]] = mongo_concat[imgt_name].apply(vj_in_frame_boolean)
            else:
                if pd.isnull(vquest_calc_fields[index]):
                    print("Info: Warning - calculation required to generate AIRR field %s - NOT IMPLEMENTED "%
                          (mongo_calc_fields[index]),
                          flush=True)
                else:
                    print("Info: Warning - calculation required to convert %s  -> %s - NOT IMPLEMENTED "%
                          (vquest_calc_fields[index],
                           mongo_calc_fields[index]),
                          flush=True)
        
        # The internal Mongo sample ID that links the sample to each sequence, constant
        # for all sequences in this file.
        ir_project_sample_id_field = airr_map.getMapping("ir_project_sample_id", "ir_id", repository_tag)
        mongo_concat[ir_project_sample_id_field] = ir_project_sample_id

        # Generate the substring field, which we use to heavily optmiize junction AA
        # searches. Technically, this should probably be an ir_ field, but because
        # it is fundamental to the indexes that already exist, we won't change it for
        # now.
        if self.verbose():
            print("Info: Computing substring from junction", flush=True) 
        junction_aa = airr_map.getMapping("junction_aa", "ir_id", repository_tag)
        ir_substring = airr_map.getMapping("ir_substring", "ir_id", repository_tag)
        
        if junction_aa in mongo_concat:
            mongo_concat[ir_substring] = mongo_concat[junction_aa].apply(Rearrangement.get_substring)

        # We want to keep the original vQuest vdj_string data, so we capture that in the
        # ir_vdjgene_string variables.
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
        mongo_concat["imgt_vgene_string"] = mongo_concat[v_call]
        mongo_concat["imgt_jgene_string"] = mongo_concat[j_call]
        mongo_concat["imgt_dgene_string"] = mongo_concat[d_call]
        # Process the IMGT VQuest v/d/j strings and generate the required columns the repository
        # needs, which are [vdj]_call, ir_[vdj]gene_gene, ir_[vdj]gene_family
        self.processGene(mongo_concat, v_call, v_call, ir_vgene_gene, ir_vgene_family)
        self.processGene(mongo_concat, j_call, j_call, ir_jgene_gene, ir_jgene_family)
        self.processGene(mongo_concat, d_call, d_call, ir_dgene_gene, ir_dgene_family)
        # If we don't already have a locus (that is the data file didn't provide one) then
        # calculate the locus based on the v_call array.
        locus = airr_map.getMapping("locus", "ir_id", repository_tag)
        if not locus in mongo_concat:
            if self.verbose():
                print("Info: Computing locus from v_call", flush=True) 
            mongo_concat[locus] = mongo_concat[v_call].apply(Rearrangement.getLocus)

        # Generate the junction length values as required.
        if self.verbose():
            print("Info: Computing junction lengths", flush=True) 
        junction = airr_map.getMapping("junction", "ir_id", repository_tag)
        junction_length = airr_map.getMapping("junction_length", "ir_id", repository_tag)
        if junction in mongo_concat and not junction_length in mongo_concat:
            mongo_concat[junction_length] = mongo_concat[junction].apply(len)
        # Special case for junction_aa_length. This does not exist in the AIRR standard,
        # so we have to check to see if the mapping returned None as well. 
        junction_aa = airr_map.getMapping("junction_aa", "ir_id", repository_tag)
        junction_aa_length = airr_map.getMapping("junction_aa_length", "ir_id", repository_tag)
        if junction_aa in mongo_concat and (junction_aa_length is None or not junction_aa_length in mongo_concat):
            mongo_concat[junction_aa_length] = mongo_concat[junction_aa].apply(len)

        # Create the created and update values for this block of records. Note that this
        # means that each block of inserts will have the same date.
        now_str = Rearrangement.getDateTimeNowUTC()
        ir_created_at = airr_map.getMapping("ir_created_at", "ir_id", repository_tag)
        ir_updated_at = airr_map.getMapping("ir_updated_at", "ir_id", repository_tag)
        mongo_concat[ir_created_at] = now_str
        mongo_concat[ir_updated_at] = now_str

        # Convert the mongo data frame dats int JSON.
        if self.verbose():
            print("Info: Creating JSON from Dataframe", flush=True) 
        t_start_full = time.perf_counter()
        t_start = time.perf_counter()
        records = json.loads(mongo_concat.T.to_json()).values()
        t_end = time.perf_counter()
        if self.verbose():
            print("Info: JSON created, time = %f seconds (%f records/s)" %((t_end - t_start),len(records)/(t_end - t_start)), flush=True)

        # The climax: insert the records into the MongoDb collection!
        if self.verbose():
            print("Info: Inserting %d records into the repository"%(len(records)), flush=True)
        t_start = time.perf_counter()
        self.repositoryInsertRearrangements(records)
        t_end = time.perf_counter()
        if self.verbose():
            print("Info: Inserted records, time = %f seconds (%f records/s)" %((t_end - t_start),len(records)/(t_end - t_start)), flush=True)

        # Get the number of annotations for this repertoire (as defined by the ir_project_sample_id)
        if self.verbose():
            print("Info: Getting the number of annotations for this repertoire", flush=True)
        t_start = time.perf_counter()
        annotation_count = self.repositoryCountRearrangements(ir_project_sample_id)
        t_end = time.perf_counter()
        if self.verbose():
            print("Info: Annotation count = %d, time = %f" %
                  (annotation_count, (t_end - t_start)), flush=True)

        # Set the cached ir_sequeunce_count field for the repertoire/sample.
        self.repositoryUpdateCount(ir_project_sample_id, annotation_count)
        t_end_full = time.perf_counter()

        # Inform on what we added and the total count for the this record.
        print("Info: Inserted %d records, total annotation count = %d, %f insertions/s" %
              (len(records), annotation_count,
              annotation_count/(t_end_full - t_start_full)), flush=True)

        # Clean up annotation files and scratch folder
        if self.verbose():
            print("Info: Cleaning up scratch folder: ", self.getScratchFolder())
        rmtree(self.getScratchFolder())

        return True
