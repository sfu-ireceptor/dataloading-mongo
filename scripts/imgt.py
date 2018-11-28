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
    #print(functionality)
    if functionality.startswith("productive"):
        return 1
    else:
        return 0

class IMGT(Parser):
    def __init__(self, context):
        Parser.__init__(self, context)

    def process(self):

        # The IMGT parser assumes that the IMGT data consists of a single
        # ZIP file and that the ZIP file contains a single directory with
        # the directory name "imgt". This MUST be the case at this time.
        # Within the imgt directory, there should be one or more IMGT
        # annotation archives. Each annotation archive should be a tgz file
        # (a tar'ed, gzip'ed file) as provided from IMGT vQUEST.
        #
        # The data is extracted in the "library" folder provided (the same
        # folder in which the original zip file was found.
        if not isfile(self.context.path):
            print("Could not find IMGT ZIP archive ", self.context.path)
            return False

        with zipfile.ZipFile(self.context.path, "r") as zip:
            # unzip to library directory
            zip.extractall(self.context.library)

	# Get a list of the files in the data folder. The getDataFolder
        # method adds on the "imgt" suffix to the library path.
        onlyfiles = [
            f for f in os.listdir(self.getDataFolder())
            if isfile(self.getDataPath(f))
        ]
        
        # Process annotation files
        for f in onlyfiles:
            if not self.processImgtArchive(f):
                return False

	# Clean up the "imgt" directory tree once the files are processed
        rmtree(self.getDataFolder())

        return True

    def processImgtArchive(self, fileName):

        path = self.getDataPath(fileName)

        if self.context.verbose:
            print("Extracting IMGT file: ", path)

        self.setScratchFolder(fileName)

        tar = tarfile.open(path)

        tar.extractall(self.getScratchFolder())

        tar.close()

        # Get the list of relevant vQuest files. Choose the vquest_file column,
        # drop the NAs, and grab the unique members that remain. This gives us
        # the list of relevant vQuest file names from the configuration file
        # that we should be considering.
        vquest_file_map = self.context.airr_map.airr_rearrangement_map['vquest_file']
        vquest_files = vquest_file_map.dropna().unique()
        if self.context.verbose:
            print("VQuest Files")
            print(vquest_files)
        # Create a dictionary that stores an array of fields to process
        # for each IMGT file that we need to process.
        filedict = {}
        first_dataframe = True
        for vquest_file in vquest_files:
            if self.context.verbose:
                print("Processing file ", vquest_file)
            # Read in the data frame for the file.
            vquest_dataframe = self.readDf(vquest_file)
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
                if self.context.verbose:
                    print("    " + str(row['vquest']) + " -> " + str(row['ir_turnkey']))
                # If the repository column has a value for the IMGT field, track the field
                # from both the IMGT and repository side.
                if not pd.isnull(row['ir_turnkey']):
                    vquest_fields.append(row['vquest'])
                    mongo_fields.append(row['ir_turnkey'])
                else:
                    print("Repository does not support " + vquest_file + "/" + 
                          str(row['vquest']) + ", not inserting into repository")
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
                                     'ir_turnkey': file_fields['ir_turnkey'],
                                     'vquest_dataframe': vquest_dataframe,
                                     'mongo_dataframe': mongo_dataframe}
            if first_dataframe:
                mongo_concat = mongo_dataframe
                vquest_concat = vquest_dataframe
                first_dataframe = False
            else:
                mongo_concat = pd.concat([mongo_concat, mongo_dataframe], axis=1)
                vquest_concat = pd.concat([vquest_concat, vquest_dataframe], axis=1)
                

#        Summary_1 = self.readDf('1_Summary.txt')
#
#        IMGT_gapped_nt_sequences_2 = self.readDf(
#            '2_IMGT-gapped-nt-sequences.txt')
#
#        Nt_sequences_3 = self.readDf('3_Nt-sequences.txt')
#
#        IMGT_gapped_AA_sequences_4 = self.readDf(
#            '4_IMGT-gapped-AA-sequences.txt')
#
#        AA_sequences_5 = self.readDf('5_AA-sequences.txt')
#
#        V_REGION_mutation_and_AA_change_table_7 = self.readDf(
#            '7_V-REGION-mutation-and-AA-change-table.txt')

        Parameters_11 = self.readDfNoHeader('11_Parameters.txt')
        #print(Parameters_11)

        # Create a dictionary with keys the first column of the parameter file and the values
        # the second column in the parameter file.
        parameter_dictionary = dict(zip(Parameters_11[0], Parameters_11[1]))
        #print(parameter_dictionary)

#        Summary_column_list = Summary_1.columns.values.tolist()

#        if 'Functionality' in Summary_column_list:
#
#            df_1 = Summary_1[[
#                'XXSequence ID', 'XXV-GENE and allele', 'XXJ-GENE and allele',
#                'XXD-GENE and allele', 'YYFunctionality', 'XXV-REGION score',
#                'XXJ-REGION score', 'XXV-REGION identity %',
#                'XXD-REGION reading frame', 'XXCDR1-IMGT length',
#                'XXCDR2-IMGT length', 'XXCDR3-IMGT length',
#                'YYFunctionality comment', 'XXOrientation', 'ERROR in OLD CODE - THIS should be J!!!V-REGION identity %'
#            ]]
#
#            df_1.columns = [
#                'seq_name', 'v_string', 'j_string', 'd_string', 'functionality',
#                'v_score', 'j_score', 'vgene_probablity',
#                'dregion_reading_frame', 'cdr1_length', 'cdr2_length',
#                'cdr3_length', 'functionality_comment', 'rev_comp',
#                'vgene_probability'
#            ]
#
#        elif 'V-DOMAIN Functionality' in Summary_column_list:
#
#            df_1 = Summary_1[[
#                'XXSequence ID', 'XXV-GENE and allele', 'XXJ-GENE and allele',
#                'XXD-GENE and allele', 'XXV-DOMAIN Functionality',
#                'XXV-REGION score', 'XXJ-REGION score', 'XXV-REGION identity %',
#                'XXD-REGION reading frame', 'XXCDR1-IMGT length',
#                'XXCDR2-IMGT length', 'XXCDR3-IMGT length',
#                'XXV-DOMAIN Functionality comment', 'XXOrientation',
#                'ERROR!!!V-REGION identity %'
#            ]]
#
#            df_1.columns = [
#                'seq_name', 'v_string', 'j_string', 'd_string', 'functionality',
#                'v_score', 'j_score', 'vgene_probablity',
#                'dregion_reading_frame', 'cdr1_length', 'cdr2_length',
#                'cdr3_length', 'functionality_comment', 'rev_comp',
#                'vgene_probability'
#            ]
#
#        df_2 = IMGT_gapped_nt_sequences_2[[
#            'XXV-D-J-REGION', 'XXV-J-REGION', 'XXV-REGION', 'XXJ-REGION', 'XXFR1-IMGT',
#            'XXFR2-IMGT', 'XXFR3-IMGT', 'XXFR4-IMGT', 'XXCDR1-IMGT', 'XXCDR2-IMGT',
#            'XXCDR3-IMGT', 'XXJUNCTION'
#        ]]
#
#        df_2.columns = [
#            'vdjregion_sequence_nt_gapped', 'vjregion_sequence_nt_gapped',
#            'vregion_sequence_nt_gapped', 'jregion_sequence_nt_gapped',
#            'fr1region_sequence_nt_gapped', 'fr2region_sequence_nt_gapped',
#            'fr3region_sequence_nt_gapped', 'fr4region_sequence_nt_gapped',
#            'cdr1region_sequence_nt_gapped', 'cdr2region_sequence_nt_gapped',
#            'cdr3region_sequence_nt_gapped', 'junction_sequence_nt_gapped'
#        ]
#
#        df_3 = Nt_sequences_3[[
#            'XXV-D-J-REGION', 'XXV-J-REGION', 'XXD-J-REGION', 'XXV-REGION', 'XXJ-REGION',
#            'XXD-REGION', 'XXFR1-IMGT', 'XXFR2-IMGT', 'XXFR3-IMGT', 'XXFR4-IMGT',
#            'XXCDR1-IMGT', 'XXCDR2-IMGT', 'XXCDR3-IMGT', '???????????JUNCTION',
#            'XXV-D-J-REGION start', 'XXV-D-J-REGION end', 'XXV-J-REGION start',
#            'XXV-J-REGION end', 'XXV-REGION start', 'XXV-REGION end',
#            'XXJ-REGION start', 'XXJ-REGION end', 'XXD-REGION start', 'XXD-REGION end',
#            'XXFR1-IMGT start', 'XXFR1-IMGT end', 'XXFR2-IMGT start', 'XXFR2-IMGT end',
#            'XXFR3-IMGT start', 'XXFR3-IMGT end', 'XXFR4-IMGT start', 'XXFR4-IMGT end',
#            'XXCDR1-IMGT start', 'XXCDR1-IMGT end', 'XXCDR2-IMGT start',
#            'XXCDR2-IMGT end', 'XXCDR3-IMGT start', 'XXCDR3-IMGT end',
#            'XXJUNCTION start', 'XXJUNCTION end', 'XXD-J-REGION start',
#            'XXD-J-REGION end'
#        ]]
#
#        df_3.columns = [
#            'vdjregion_sequence_nt', 'vjregion_sequence_nt',
#            'djregion_sequence_nt', 'vregion_sequence_nt',
#            'jregion_sequence_nt', 'dregion_sequence_nt',
#            'fr1region_sequence_nt', 'fr2region_sequence_nt',
#            'fr3region_sequence_nt', 'fr4region_sequence_nt',
#            'cdr1region_sequence_nt', 'cdr2region_sequence_nt',
#            'cdr3region_sequence_nt', 'junction_nt', 'vdjregion_start',
#            'vdjregion_end', 'vjregion_start', 'vjregion_end', 'v_start',
#            'v_end', 'j_start', 'j_end', 'd_start', 'd_end', 'fwr1_start',
#            'fwr1_end', 'fwr2_start', 'fwr2_end', 'fwr3_start', 'fwr3_end',
#            'fwr4_start', 'fwr4_end', 'cdr1_start', 'cdr1_end', 'cdr2_start',
#            'cdr2_end', 'cdr3_start', 'cdr3_end', 'junction_start',
#            'junction_end', 'djregion_start', 'djregion_end'
#        ]
#
#        df_4 = IMGT_gapped_AA_sequences_4[[
#            'XXV-D-J-REGION', 'XXV-J-REGION', 'XXV-REGION', 'XXJ-REGION', 'XXFR1-IMGT',
#            'XXFR2-IMGT', 'XXFR3-IMGT', 'XXFR4-IMGT', 'XXCDR1-IMGT', 'XXCDR2-IMGT',
#            'XXCDR3-IMGT', 'XXJUNCTION'
#        ]]
#
#        df_4.columns = [
#            'vdjregion_sequence_aa_gapped', 'vjregion_sequence_aa_gapped',
#            'vregion_sequence_aa_gapped', 'jregion_sequence_aa_gapped',
#            'fr1region_sequence_aa_gapped', 'fr2region_sequence_aa_gapped',
#            'fr3region_sequence_aa_gapped', 'fr4region_sequence_aa_gapped',
#            'cdr1region_sequence_aa_gapped', 'cdr2region_sequence_aa_gapped',
#            'cdr3region_sequence_aa_gapped', 'junction_sequence_aa_gapped'
#        ]
#
#        df_5 = AA_sequences_5[[
#            'XXV-D-J-REGION', 'XXV-J-REGION', 'XXV-REGION', 'XXJ-REGION', 'XXFR1-IMGT',
#            'XXFR2-IMGT', 'XXFR3-IMGT', 'XXFR4-IMGT', 'XXCDR1-IMGT', 'XXCDR2-IMGT',
#            'XXCDR3-IMGT', 'XXJUNCTION'
#        ]]
#
#        df_5.columns = [
#            'vdjregion_sequence_aa', 'vjregion_sequence_aa',
#            'vregion_sequence_aa', 'jregion_sequence_aa',
#            'fr1region_sequence_aa', 'fr2region_sequence_aa',
#            'fr3region_sequence_aa', 'fr4region_sequence_aa',
#            'cdr1region_sequence_aa', 'cdr2region_sequence_aa',
#            'cdr3region_sequence_aa', 'junction_aa'
#        ]
#
        #df_7 = V_REGION_mutation_and_AA_change_table_7[[
        #    'XXV-REGION', 'XXFR1-IMGT', 'XXFR2-IMGT', 'XXFR3-IMGT', 'XXCDR1-IMGT',
        #    'XXCDR2-IMGT', 'XXCDR3-IMGT'
        #]]

        #df_7.columns = [
        #    'vregion_mutation_string', 'fr1region_mutation_string',
        #    'fr2region_mutation_string', 'fr3region_mutation_string',
        #    'cdr1region_mutation_string', 'cdr2region_mutation_string',
        #    'cdr3region_mutation_string'
        #]

        #mongo_concat = pd.concat([df_1, df_2, df_3, df_4, df_5, df_7], axis=1)
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

        # Get rid of columns where the column is null.
        mongo_concat = mongo_concat.where((pd.notnull(mongo_concat)), "")
        #mongo_concat['cdr1_length'] = df_concat['cdr1region_sequence_aa'].apply(len)
        #mongo_concat['cdr2_length'] = df_concat['cdr2region_sequence_aa'].apply(len)
        #mongo_concat['cdr3_length'] = df_concat['cdr3region_sequence_aa'].apply(len)

        # IMGT annotates a rearrangement with a text string. We have a utility function
        # that takes the string and changes it to an integer 1/0 which the repository
        # expects. We want to keep the original data in case we need further interpretation,
        mongo_concat['ir_productive'] = mongo_concat['functional']
        mongo_concat['functional'] = mongo_concat['functional'].apply(functional_boolean)

        sampleid = self.context.samples.find({
            "imgt_file_name": {
                '$regex': fileName
            }
        }, {'_id': 1})
        ir_project_sample_id = [i['_id'] for i in sampleid][0]
        # Critical iReceptor specific fields
        # The internal Mongo sample ID that links the sample to each sequence, constant
        # for all sequences in this file.
        mongo_concat['ir_project_sample_id'] = ir_project_sample_id
        # The annotation tool used
        mongo_concat['ir_annotation_tool'] = "V-Quest"

        # Generate the substring field, which we use to heavily optmiize junction AA
        # searches. Technically, this should probably be an ir_ field, but because
        # it is fundamental to the indexes that already exist, we won't change it for
        # now.
        mongo_concat['substring'] = mongo_concat['junction_aa'].apply(Parser.get_substring)

        # We want to keep the original vQuest vdj_string data, so we capture that in the
        # ir_vdj_string variables. We use the ir_ prefix because they are non AIRR fields.
        mongo_concat['ir_v_string'] = mongo_concat['v_call']
        mongo_concat['ir_j_string'] = mongo_concat['j_call']
        mongo_concat['ir_d_string'] = mongo_concat['d_call']
        # Process the IMGT VQuest v/d/j strings and generate the required columns the repository
        # needs, which is [vdj]_call, [vdj]gene_gene, [vdj]gene_family
        Parser.processGene(self.context, mongo_concat, "ir_v_string", "v_call", "vgene_gene", "vgene_family")
        Parser.processGene(self.context, mongo_concat, "ir_j_string", "j_call", "jgene_gene", "jgene_family")
        Parser.processGene(self.context, mongo_concat, "ir_d_string", "d_call", "dgene_gene", "dgene_family")

        # Generate the junction length values as required.
        mongo_concat['junction_length'] = mongo_concat['junction_nt'].apply(len)
        mongo_concat['junction_aa_length'] = mongo_concat['junction_aa'].apply(len)

        records = json.loads(mongo_concat.T.to_json()).values()

        # The climax: insert the records into the MongoDb collection!
        self.context.sequences.insert(records)

        ir_sequence_count = len(records)

        #     self.context.samples.update_one({"imgt_file_name":{'$regex': fileName}},{"$set" : {"ir_sequence_count":0}})

        if self.context.counter == 'reset':
            ori_count = 0
        else:
            ori_count = self.context.samples.find_one({
                "imgt_file_name": {
                    '$regex': fileName
                }
            }, {"ir_sequence_count": 1})["ir_sequence_count"]

        self.context.samples.update(
            {
                "imgt_file_name": {
                    '$regex': fileName
                }
            }, {"$set": {
                "ir_sequence_count": ir_sequence_count + ori_count
            }},
            multi=True)

        #     self.context.samples.update_one({"imgt_file_name":{'$regex': fileName}},{"$set" : {"ir_sequence_count":ir_sequence_count+ori_count}})

        # Clean up annotation files and scratch folder
        if self.context.verbose:
            print("Cleaning up scratch folder: ", self.getScratchFolder())

        rmtree(self.getScratchFolder())

        return True
