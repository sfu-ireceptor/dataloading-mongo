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
from Bio.Seq import translate

from rearrangement import Rearrangement
from parser import Parser

# Compute the np2 value from its components parts based on IMGT specification.
# For IGH, TRB and TRD sequences with 1 D-REGION :concatenation of nt sequences
# P3'D+N2-REGION+P5'J;
# With 2 of 3 D-REGION: P3'D1+N2-REGION+P5'D2
# Assumes that one of the key value pairs is the locus of the row being processed
def compute_np2(vquest_object):
    # Get the locus value. We assume this exists.
    if "locus" in vquest_object:
        locus = vquest_object["locus"]

    # Determine how many D-REGIONs we have.
    multiple_dregion = False
    if "D1-REGION" in vquest_object:
        if pd.notnull(vquest_object["D1-REGION"]):  
            multiple_dregion = True
    
    # Depending on the locus type and the number of D-Regions calculate np1.
    np2_str = ""
    if locus in ["IGH", "TRB" ,"TRD"]:
        if not multiple_dregion:
            term1 = vquest_object["P3'D"] if pd.notnull(vquest_object["P3'D"]) else ""
            term2 = vquest_object["N2-REGION"] if pd.notnull(vquest_object["N2-REGION"]) else ""
            term3 = vquest_object["P5'J"] if pd.notnull(vquest_object["P5'J"]) else ""
            np2_str = term1 + term2 + term3
        else:
            term1 = vquest_object["P3'D1"] if pd.notnull(vquest_object["P3'D1"]) else ""
            term2 = vquest_object["N2-REGION"] if pd.notnull(vquest_object["N2-REGION"]) else ""
            term3 = vquest_object["P5'D2"] if pd.notnull(vquest_object["P5'D2"]) else ""
            np2_str = term1 + term2 + term3

    return np2_str
       
# Compute the np1 value from its components parts based on IMGT specification.
# For IGH, TRB, TRD sequences with 1 D-REGION: concatenation of nt sequences
# for P3'V+N1-REGION+P5'D ;
# For IGH, TRB, TRD sequences with 2 or 3 D-REGION: concatenation of nt sequences
# for P3'V+N1-REGION+P5'D1 ;
# For IGK, IGL, IGI, TRA, TRG sequences : P3'V+N-REGION+P5'J
# Assumes that one of the key value pairs is the locus of the row being processed
def compute_np1(vquest_object):
    # Get the locus value. We assume this exists.
    if "locus" in vquest_object:
        locus = vquest_object["locus"]

    # Determine how many D-REGIONs we have.
    multiple_dregion = False
    if "D1-REGION" in vquest_object:
        if pd.notnull(vquest_object["D1-REGION"]):  
            multiple_dregion = True
    
    # Depending on the locus type and the number of D-Regions calculate np1.
    np1_str = ""
    if locus in ["IGH", "TRB" ,"TRD"]:
        if not multiple_dregion:
            term1 = vquest_object["P3'V"] if pd.notnull(vquest_object["P3'V"]) else ""
            term2 = vquest_object["N1-REGION"] if pd.notnull(vquest_object["N1-REGION"]) else ""
            term3 = vquest_object["P5'D"] if pd.notnull(vquest_object["P5'D"]) else ""
            np1_str = term1 + term2 + term3
        else:
            term1 = vquest_object["P3'V"] if pd.notnull(vquest_object["P3'V"]) else ""
            term2 = vquest_object["N1-REGION"] if pd.notnull(vquest_object["N1-REGION"]) else ""
            term3 = vquest_object["P5'D1"] if pd.notnull(vquest_object["P5'D1"]) else ""
            np1_str = term1 + term2 + term3
    elif locus in ["IGK", "IGL", "IGI", "TRA", "TRG"]:
        term1 = vquest_object["P3'V"] if pd.notnull(vquest_object["P3'V"]) else ""
        term2 = vquest_object["N-REGION"] if pd.notnull(vquest_object["N-REGION"]) else ""
        term3 = vquest_object["P5'J"] if pd.notnull(vquest_object["P5'J"]) else ""
        np1_str = term1 + term2 + term3

    return np1_str
       


# IMGT has a number of nt based fields that need to be converted to AA
# fields. This takes an nt sequence and returns an AA equivalent using
# biopython's converter. For now we are truncating the sequence if the
# sequence is not a length that is divisible by 3.
def seq_nt_to_aa(seq_nt):
    if len(seq_nt) % 3 > 0:  seq_nt = seq_nt[:len(seq_nt)- len(seq_nt)%3]
    seq_aa = translate(seq_nt)
    return seq_aa

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

# Check for one of the three stop codons in the the regions provided.
# Check for null regions 
def check_stop_codon(region1, region2):
    if pd.notnull(region1):
        if 'UAA' in region1 or 'UAG' in region1 or 'UGA' in region1:
            return True
    if pd.notnull(region2):
        if 'UAA' in region1 or 'UAG' in region1 or 'UGA' in region1:
            return True
    return False

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

        # Start a timer for performance reasons.
        t_start_full = time.perf_counter()

        # Get the AIRR Map object for this class (for convenience).
        airr_map = self.getAIRRMap()

        # Set the tag for the repository that we are using. Note this should
        # be refactored so that it is a parameter provided so that we can use
        # multiple repositories.
        repository_tag = self.getRepositoryTag()

        # Get the fields to use for finding repertoire IDs, either using those IDs
        # directly or by looking for a repertoire ID based on a rearrangement file
        # name.
        repertoire_link_field = self.getRepertoireLinkIDField()
        rearrangement_link_field = self.getAnnotationLinkIDField()

        # Set the tag for the file mapping that we are using. Ths is essentially the
        # look up into the columns of the AIRR Mapping that we are using.
        filemap_tag = self.getFileMapping()

        # Set the tag for the iReceptor ID that we use.
        ireceptor_tag = self.getiReceptorTag()

        # Set the tag for the calculation flag mapping that we are using. Ths is 
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

        # Get the single, unique repertoire link id for the filename we are loading. If
        # we can't find one, this is an error and we return failure.
        repertoire_link_id = self.getRepertoireInfo(fileName)
        if repertoire_link_id is None:
            print("ERROR: Could not link file %s to a valid repertoire"%(fileName))
            return False

        # Open the tar file, extract the data, and close the tarfile. 
        # This leaves us with a folder with all of the individual vQUest
        # files extracted in this location.

        try:
            tar = tarfile.open(filewithpath)
            tar.extractall(self.getScratchFolder())
            tar.close()
        except Exception as err:
            print("ERROR: Unable to open IMGT tar file %s" % (filewithpath))
            return False

        # Get the column of values from the AIRR tag. We only want the
        # Rearrangement related fields.
        map_column = self.getAIRRMap().getRearrangementMapColumn(self.getAIRRTag())
        # Get a boolean column that flags columns of interest. Exclude nulls.
        fields_of_interest = map_column.notnull()
        # Afer the following airr_fields contains N columns (e.g. iReceptor, AIRR)
        # that contain the AIRR Repertoire mappings.
        airr_fields = self.getAIRRMap().getRearrangementRows(fields_of_interest)

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
            vquest_dataframe = pd.read_csv(self.getScratchPath(vquest_file),
                                           sep='\t', low_memory=False)
            # Extract the fields that are of interest for this file.
            imgt_file_column = airr_map.getRearrangementMapColumn(self.imgt_filename_map)
            fields_of_interest = imgt_file_column.isin([vquest_file])

            # We select rows in the mapping that contain fields of interest for this file.
            # At this point, file_fields contains N columns that contain our mappings for
            # the specific formats (e.g. ireceptor, airr, vquest). The rows are limited 
            # to have only data that is relevant to this specific vquest file.
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
                    if not row[calculate_tag] or row[calculate_tag] == "FALSE":
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
                        if self.verbose():
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
                        print("Info:    Repository does not support " + vquest_file +
                              "/" + str(row[filemap_tag]) + 
                              ", not inserting into repository", flush=True)

            # Use the vquest column in our mapping to select the columns we want from the 
            # possibly quite large vquest data frame.
            mongo_dataframe = vquest_dataframe[vquest_fields]
            # We now have a data frame that has only the vquest data we want from this
            # file. We now replace the vquest column names with the repository column
            # names from the map
            mongo_dataframe.columns = mongo_fields
            # We now have a data frame with vquest data in it with AIRR compliant column
            # names. Store all of this in a dictionay based on the file name so we can
            # use it later.
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
        # Create a dictionary with keys the first column of the parameter file and the 
        # values in the second column in the parameter file.
        Parameters_11 = pd.read_csv(self.getScratchPath('11_Parameters.txt'),
                                    sep='\t', low_memory=False, header=None)
        parameter_dictionary = dict(zip(Parameters_11[0], Parameters_11[1]))

        # Need to grab some data out of the parameters dictionary. This is not really
        # necessary as this information should be stored in the repertoire metadata,
        #  but for completeness we err on the side of having more information.
        # Note that this is quite redundant as it is storing the same information
        # for each rearrangement...
        mongo_concat['vquest_annotation_date'] = parameter_dictionary['Date']
        mongo_concat['vquest_tool_version'] = parameter_dictionary['IMGT/V-QUEST programme version']
        mongo_concat['vquest_reference_version'] = parameter_dictionary[
            'IMGT/V-QUEST reference directory release']
        mongo_concat['vquest_species'] = parameter_dictionary['Species']
        mongo_concat['vquest_receptor_type'] = parameter_dictionary['Receptor type or locus']
        mongo_concat['vquest_reference_directory_set'] = parameter_dictionary[
            'IMGT/V-QUEST reference directory set']
        mongo_concat['vquest_search_insert_delete'] = parameter_dictionary[
            'Search for insertions and deletions']
        mongo_concat['vquest_no_nucleotide_to_add'] = parameter_dictionary[
            "Nb of nucleotides to add (or exclude) in 3' of the V-REGION for the evaluation of the alignment score"]
        mongo_concat['vquest_no_nucleotide_to_exclude'] = parameter_dictionary[
            "Nb of nucleotides to exclude in 5' of the V-REGION for the evaluation of the nb of mutations"]
        if self.verbose():
            print("Info: Done processing IMGT Parameter file", flush=True) 

        # Get rid of columns where the column is null.
        if self.verbose():
            print("Info: Cleaning up NULL columns", flush=True) 
        mongo_concat = mongo_concat.where((pd.notnull(mongo_concat)), "")

        # Explicilty store a link for each rearrangement record in this repertoire to
        # the appropriate record id for the repertoire object. Note this is NOT
        # repertoire_id as repertoire_id is not sufficient to identify a unique row in 
        # the repertoire collection in our repository.
        repository_link_field = airr_map.getMapping(rearrangement_link_field,
                                                    ireceptor_tag,
                                                    repository_tag)
        if not repository_link_field is None:
            mongo_concat[repository_link_field] = repertoire_link_id
        else:
            print("ERROR: Could not get a repository link field for %s"%
                  (rearrangement_link_field))
            return False
            
        # Set the relevant IDs for the record being inserted. If it fails, don't
        # load any data.
        if not self.checkIDFields(mongo_concat, repertoire_link_id):
            return False

        # Generate the substring field, which we use to heavily optmiize junction AA
        # searches. Technically, this should probably be an ir_ field, but because
        # it is fundamental to the indexes that already exist, we won't change it for
        # now.
        if self.verbose():
            print("Info: Computing substring from junction", flush=True) 
        junction_aa = airr_map.getMapping("junction_aa", ireceptor_tag, repository_tag)
        ir_substring = airr_map.getMapping("ir_substring", ireceptor_tag, repository_tag)
        if junction_aa in mongo_concat:
            mongo_concat[ir_substring] = mongo_concat[junction_aa].apply(Rearrangement.get_substring)

        # We want to keep the original vQuest vdj_string data, so we capture that in the
        # ir_vdjgene_string variables.
        # We need to look up the "known parameter" from an iReceptor perspective - the
        # field name in the ireceptor_tag column mapping and map that to the correct
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
        mongo_concat["vquest_vgene_string"] = mongo_concat[v_call]
        mongo_concat["vquest_jgene_string"] = mongo_concat[j_call]
        mongo_concat["vquest_dgene_string"] = mongo_concat[d_call]
        # Process the IMGT VQuest v/d/j strings and generate the required columns the
        # repository needs, which are [vdj]_call, ir_[vdj]gene_gene, ir_[vdj]gene_family
        self.processGene(mongo_concat, v_call, v_call, ir_vgene_gene, ir_vgene_family)
        self.processGene(mongo_concat, j_call, j_call, ir_jgene_gene, ir_jgene_family)
        self.processGene(mongo_concat, d_call, d_call, ir_dgene_gene, ir_dgene_family)
        # If we don't already have a locus (that is the data file didn't provide one) 
        # then calculate the locus based on the v_call array.
        locus = airr_map.getMapping("locus", ireceptor_tag, repository_tag)
        if not locus in mongo_concat:
            if self.verbose():
                print("Info: Computing locus from v_call", flush=True) 
            mongo_concat[locus] = mongo_concat[v_call].apply(Rearrangement.getLocus)

        # Generate the junction length values as required.
        if self.verbose():
            print("Info: Computing junction lengths", flush=True) 
        junction = airr_map.getMapping("junction", ireceptor_tag, repository_tag)
        junction_length = airr_map.getMapping("junction_length", 
                                              ireceptor_tag, repository_tag)
        if junction in mongo_concat and not junction_length in mongo_concat:
            mongo_concat[junction_length] = mongo_concat[junction].apply(len)
        # Special case for junction_aa_length. This does not exist in the AIRR standard,
        # so we have to check to see if the mapping returned None as well. 
        junction_aa = airr_map.getMapping("junction_aa", ireceptor_tag, repository_tag)
        junction_aa_length = airr_map.getMapping("ir_junction_aa_length",
                                                 ireceptor_tag,
                                                 repository_tag)
        if junction_aa in mongo_concat and (junction_aa_length is None or not junction_aa_length in mongo_concat):
            mongo_concat[junction_aa_length] = mongo_concat[junction_aa].apply(
                                                              Parser.len_null_to_null)

        # AIRR fields that need to be built from existing IMGT
        # generated fields. These fields are calculated based on
        # the specification here: http://www.imgt.org/IMGT_vquest/vquest_airr
        # with the IMGT fields the values are build from in the 
        # AIRR mapping.
        if self.verbose():
            print("Info: Setting up AIRR specific fields", flush=True) 

        # Iterate over the fields that require calculation
        for index, value in enumerate(mongo_calc_fields):
            
            # For the field we are processing, look up the field name for the repository.
            repository_field = airr_map.getMapping(value, ireceptor_tag, repository_tag)

            # Get the vquest data frame as we use it everywhere.
            vquest_df = filedict[vquest_calc_file[index]]["vquest_dataframe"]

            # Perform the calculations required based on the ireceptor based field name.
            if value == "productive":
                # Calculate the productive field.
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                imgt_name = "vquest_" + repository_field
                mongo_concat[imgt_name] = vquest_df[vquest_calc_fields[index]]
                mongo_concat[repository_field] = mongo_concat[imgt_name].apply(productive_boolean)
            elif value == "rev_comp":
                # Rev comp...
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                imgt_name = "vquest_" + repository_field
                mongo_concat[imgt_name] = vquest_df[vquest_calc_fields[index]]
                mongo_concat[repository_field] = mongo_concat[imgt_name].apply(rev_comp_boolean)
            elif value == "vj_in_frame":
                # VJ in frame...
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                imgt_name = "vquest_" + repository_field
                mongo_concat[imgt_name] = vquest_df[vquest_calc_fields[index]]
                mongo_concat[repository_field] = mongo_concat[imgt_name].apply(vj_in_frame_boolean)
            elif value == "stop_codon":
                # This fields is determined by checking whther or not there is
                # a stop codon in the two regions in the mapping.
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                vquest_array = vquest_calc_fields[index].split(" or ")
                if len(vquest_array) == 2:
                    # We use the Pandas apply method to iterate over the rows and at each
                    # row we use the lamda function to process the fields in the row. We 
                    # know we have two columns and we call the check_stop_codon function
                    # for each row.
                    process_df =  vquest_df[[vquest_array[0],vquest_array[1]]]
                    mongo_concat[repository_field] = process_df.apply(
                              lambda x : check_stop_codon(x[0], x[1]), axis=1)
            elif value in ["sequence_alignment","sequence_alignment_aa","d_sequence_alignment"]: 
                # These fields are built from one out of two fields that come from
                # the mapping. They are string fields, one of which we assume has
                # data and one won't - so we are safe to concatenate the strings.
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                vquest_array = vquest_calc_fields[index].split(" or ")
                if len(vquest_array) == 2:
                    # We use the Pandas apply method to iterate over the rows and at each
                    # row we use the lamda function to process the fields in the row. We 
                    # extract two columns in each row and we contatenate the strings
                    # handling null values if they exist.
                    process_df =  vquest_df[[vquest_array[0],vquest_array[1]]]
                    mongo_concat[repository_field] = process_df.apply(
                              lambda x : '{}{}'.format(
                                  x[0] if pd.notnull(x[0]) else "",
                                  x[1] if pd.notnull(x[1]) else ""
                              ), axis=1)
            elif value == "np1" or value == "np2":
                # These fields are built from a complex combination of fieldes
                # depending on the type of locus and the number of D genes. 
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                vquest_array = vquest_calc_fields[index].split(" or ")

                # We need locus to define np1 and np2
                locus_field = airr_map.getMapping("locus", ireceptor_tag, repository_tag)
                if not locus_field is None:
                    vquest_df[locus_field] = mongo_concat[locus_field]
                    vquest_array.append(locus_field)

                    # Call the correct conversion function
                    if value == "np1":
                        mongo_concat[repository_field] = vquest_df.apply(compute_np1, axis=1)
                    elif value == "np2":
                        mongo_concat[repository_field] = vquest_df.apply(compute_np2, axis=1)
            elif value == 'd_sequence_start' or value == 'd_sequence_end':
                # These are numerical start/end fields, built from one of two possible
                # source fields. Again, we assume that either field, but not
                # both fields contain data and we use the one that contains data.
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                vquest_array = vquest_calc_fields[index].split(" or ")
                if len(vquest_array) == 2:
                    process_df =  vquest_df[[vquest_array[0],vquest_array[1]]]
                    mongo_concat[repository_field] = process_df.apply(
                              lambda x : x[0] if pd.notnull(x[0]) else x[1], axis=1)
            elif value == 'p5d_length' or value == 'p3d_length' or value == 'n1_length':
                # These are numerical length fields, built from one of two possible
                # source fields. Again, we assume that either field, but not
                # both fields contain data and we use the one that contains data.
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                vquest_array = vquest_calc_fields[index].split(" or ")
                if len(vquest_array) == 2:
                    process_df =  vquest_df[[vquest_array[0],vquest_array[1]]]
                    mongo_concat[repository_field] = process_df.apply(
                              lambda x : x[0] if pd.notnull(x[0]) else x[1], axis=1)
        
        # We need to iterate over the compuation list again, as some of the 
        # computed values needed values computed in the first pass above. For
        # example, computing d_sequence_alignment_aa relies on the computation
        # of d_sequence_alignment first. So we need two passes over the list 
        # as we are not guaranteed of the order.
        for index, value in enumerate(mongo_calc_fields):
            repository_field = airr_map.getMapping(value, ireceptor_tag, repository_tag)
            if value == 'd_sequence_alignment_aa':
                # Covert nt d_sequence_alignment to and AA field.
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                seq_nt = airr_map.getMapping("d_sequence_alignment", 
                                              ireceptor_tag, repository_tag)
                if seq_nt in mongo_concat and not repository_field in mongo_concat:
                    mongo_concat[repository_field] = mongo_concat[seq_nt].apply(seq_nt_to_aa)
            elif value == 'np1_aa':
                # Covert nt d_sequence_alignment to and AA field.
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                seq_nt = airr_map.getMapping("np1", ireceptor_tag, repository_tag)
                if seq_nt in mongo_concat and not repository_field in mongo_concat:
                    mongo_concat[repository_field] = mongo_concat[seq_nt].apply(seq_nt_to_aa)
            elif value == 'np2_aa':
                # Covert nt d_sequence_alignment to and AA field.
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                seq_nt = airr_map.getMapping("np2", ireceptor_tag, repository_tag)
                if seq_nt in mongo_concat and not repository_field in mongo_concat:
                    mongo_concat[repository_field] = mongo_concat[seq_nt].apply(seq_nt_to_aa)
            elif value == 'np1_length':
                # Compute the length of the np1 field
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                np1 = airr_map.getMapping("np1", ireceptor_tag, repository_tag)
                if np1 in mongo_concat and not repository_field in mongo_concat:
                    mongo_concat[repository_field] = mongo_concat[np1].apply(len)
            elif value == 'np2_length':
                # Compute the length of the np2 field
                if self.verbose():
                    print("Info: Computing AIRR field %s"%(value), flush=True) 
                np2 = airr_map.getMapping("np2", ireceptor_tag, repository_tag)
                if np2 in mongo_concat and not repository_field in mongo_concat:
                    mongo_concat[repository_field] = mongo_concat[np2].apply(len)
            else:
                # If we get here we need to check if the field is in mongo_concat. If
                # it isn't then we found a field that we don't yet compute, so we 
                # warn about the situation.
                if not value in mongo_concat:
                    if pd.isnull(vquest_calc_fields[index]):
                        print("Warning: calculation required to generate AIRR field %s - NOT IMPLEMENTED "%
                              (value), flush=True)
                    else:
                        print("Warning: calculation required to convert %s  -> %s - NOT IMPLEMENTED "%
                              (vquest_calc_fields[index], value), flush=True)

        # Check to make sure all AIRR required columns exist
        if not self.checkAIRRRequired(mongo_concat, airr_fields):
            return False

        # Create the created and update values for this block of records. Note that this
        # means that each block of inserts will have the same date.
        now_str = Rearrangement.getDateTimeNowUTC()
        ir_created_at = airr_map.getMapping("ir_created_at", ireceptor_tag, repository_tag)
        ir_updated_at = airr_map.getMapping("ir_updated_at", ireceptor_tag, repository_tag)
        mongo_concat[ir_created_at] = now_str
        mongo_concat[ir_updated_at] = now_str

        # Transform the data frame so that it meets the repository type requirements
        if not self.mapToRepositoryType(mongo_concat):
            print("ERROR: Unable to map data to the repository")
            return False

        # Convert the mongo data frame dats int JSON.
        if self.verbose():
            print("Info: Creating JSON from Dataframe", flush=True) 
        t_start_load_= time.perf_counter()
        t_start = time.perf_counter()
        records = json.loads(mongo_concat.T.to_json()).values()
        t_end = time.perf_counter()
        if self.verbose():
            print("Info: JSON created, time = %f seconds (%f records/s)" %
                  ((t_end - t_start),len(records)/(t_end - t_start)), flush=True)

        # The climax: insert the records into the MongoDb collection!
        if self.verbose():
            print("Info: Inserting %d records into the repository"%(len(records)), flush=True)
        t_start = t_start_load = time.perf_counter()
        self.repositoryInsertRearrangements(records)
        t_end = time.perf_counter()
        if self.verbose():
            print("Info: Inserted records, time = %f seconds (%f records/s)" %
                  ((t_end - t_start),len(records)/(t_end - t_start)), flush=True)

        # Get the number of annotations for this repertoire
        if self.verbose():
            print("Info: Getting the number of annotations for this repertoire", flush=True)
        t_start = time.perf_counter()
        annotation_count = self.repositoryCountRearrangements(repertoire_link_id)
        if annotation_count == -1:
            print("ERROR: invalid annotation count (%d), write failed." %
                  (annotation_count))
            return False

        t_end = time.perf_counter()
        if self.verbose():
            print("Info: Annotation count = %d, time = %f" %
                  (annotation_count, (t_end - t_start)), flush=True)

        # Set the cached ir_sequeunce_count field for the repertoire/sample.
        self.repositoryUpdateCount(repertoire_link_id, annotation_count)
        t_end_load = time.perf_counter()
        if self.verbose():
            print("Info: Total load time = %f" % (t_end_load - t_start_load))

        # Clean up annotation files and scratch folder
        if self.verbose():
            print("Info: Cleaning up scratch folder: ", self.getScratchFolder())
        rmtree(self.getScratchFolder())

        # Inform on what we added and the total count for the this record.
        t_end_full = time.perf_counter()
        print("Info: Inserted %d records, annotation count = %d, %f s, %f insertions/s" %
              (len(records), annotation_count, t_end_full - t_start_full,
              len(records)/(t_end_full - t_start_full)), flush=True)

        return True
