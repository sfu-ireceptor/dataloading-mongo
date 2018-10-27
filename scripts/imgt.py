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
        # drop the NAs, and grab the unique members that remain.
        vquest_file_map = self.context.airr_map.airr_rearrangement_map['vquest_file'].dropna()
        vquest_files = vquest_file_map.unique()
        print(vquest_files)
        # Create a dictionary that stores an array of fields to process
        # for each IMGT file that we need to process.
        filedict = {}
        for vquest_file in vquest_files:
            # Read in the data frame for the file.
            vquest_dataframe = self.readDf(vquest_file)
            # Extract the fields that are of interest for this file.
            # We first select the rows in the mapping that contain
            # the current file in the "vquest_file" column.
            file_fields = self.context.airr_map.airr_rearrangement_map.loc[self.context.airr_map.airr_rearrangement_map['vquest_file'].isin([vquest_file])]
            # Use the vquest column names to select the data of interest
            # from the data frame.
            airr_dataframe = vquest_dataframe[file_fields['vquest']]
            # Replace the vquest column names with the AIRR column names we map
            airr_dataframe.columns = file_fields['airr']
            # Store it all in a dictionay so we can use it later.
            filedict[vquest_file] = {'vquest': file_fields['vquest'],
                                     'airr': file_fields['airr'],
                                     'vquest_dataframe': vquest_dataframe,
                                     'airr_dataframe': airr_dataframe}

        print(filedict)
        #print(filedict.keys())
        #print(filedict.values())

        return False

        Summary_1 = self.readDf('1_Summary.txt')

        IMGT_gapped_nt_sequences_2 = self.readDf(
            '2_IMGT-gapped-nt-sequences.txt')

        Nt_sequences_3 = self.readDf('3_Nt-sequences.txt')

        IMGT_gapped_AA_sequences_4 = self.readDf(
            '4_IMGT-gapped-AA-sequences.txt')

        AA_sequences_5 = self.readDf('5_AA-sequences.txt')

        V_REGION_mutation_and_AA_change_table_7 = self.readDf(
            '7_V-REGION-mutation-and-AA-change-table.txt')

        Parameters_11 = self.readDfNoHeader('11_Parameters.txt')

        Para_dict = dict(zip(Parameters_11[0], Parameters_11[1]))

        Summary_column_list = Summary_1.columns.values.tolist()

        if 'Functionality' in Summary_column_list:

            df_1 = Summary_1[[
                'Sequence ID', 'V-GENE and allele', 'J-GENE and allele',
                'D-GENE and allele', 'Functionality', 'V-REGION score',
                'J-REGION score', 'V-REGION identity %',
                'D-REGION reading frame', 'CDR1-IMGT length',
                'CDR2-IMGT length', 'CDR3-IMGT length',
                'Functionality comment', 'Orientation', 'V-REGION identity %'
            ]]

            df_1.columns = [
                'seq_name', 'v_string', 'j_string', 'd_string', 'functionality',
                'v_score', 'j_score', 'vgene_probablity',
                'dregion_reading_frame', 'cdr1_length', 'cdr2_length',
                'cdr3_length', 'functionality_comment', 'rev_comp',
                'vgene_probability'
            ]

        elif 'V-DOMAIN Functionality' in Summary_column_list:

            df_1 = Summary_1[[
                'Sequence ID', 'V-GENE and allele', 'J-GENE and allele',
                'D-GENE and allele', 'V-DOMAIN Functionality',
                'V-REGION score', 'J-REGION score', 'V-REGION identity %',
                'D-REGION reading frame', 'CDR1-IMGT length',
                'CDR2-IMGT length', 'CDR3-IMGT length',
                'V-DOMAIN Functionality comment', 'Orientation',
                'V-REGION identity %'
            ]]

            df_1.columns = [
                'seq_name', 'v_string', 'j_string', 'd_string', 'functionality',
                'v_score', 'j_score', 'vgene_probablity',
                'dregion_reading_frame', 'cdr1_length', 'cdr2_length',
                'cdr3_length', 'functionality_comment', 'rev_comp',
                'vgene_probability'
            ]

        df_2 = IMGT_gapped_nt_sequences_2[[
            'V-D-J-REGION', 'V-J-REGION', 'V-REGION', 'J-REGION', 'FR1-IMGT',
            'FR2-IMGT', 'FR3-IMGT', 'FR4-IMGT', 'CDR1-IMGT', 'CDR2-IMGT',
            'CDR3-IMGT', 'JUNCTION'
        ]]

        df_2.columns = [
            'vdjregion_sequence_nt_gapped', 'vjregion_sequence_nt_gapped',
            'vregion_sequence_nt_gapped', 'jregion_sequence_nt_gapped',
            'fr1region_sequence_nt_gapped', 'fr2region_sequence_nt_gapped',
            'fr3region_sequence_nt_gapped', 'fr4region_sequence_nt_gapped',
            'cdr1region_sequence_nt_gapped', 'cdr2region_sequence_nt_gapped',
            'cdr3region_sequence_nt_gapped', 'junction_sequence_nt_gapped'
        ]

        df_3 = Nt_sequences_3[[
            'V-D-J-REGION', 'V-J-REGION', 'D-J-REGION', 'V-REGION', 'J-REGION',
            'D-REGION', 'FR1-IMGT', 'FR2-IMGT', 'FR3-IMGT', 'FR4-IMGT',
            'CDR1-IMGT', 'CDR2-IMGT', 'CDR3-IMGT', 'JUNCTION',
            'V-D-J-REGION start', 'V-D-J-REGION end', 'V-J-REGION start',
            'V-J-REGION end', 'V-REGION start', 'V-REGION end',
            'J-REGION start', 'J-REGION end', 'D-REGION start', 'D-REGION end',
            'FR1-IMGT start', 'FR1-IMGT end', 'FR2-IMGT start', 'FR2-IMGT end',
            'FR3-IMGT start', 'FR3-IMGT end', 'FR4-IMGT start', 'FR4-IMGT end',
            'CDR1-IMGT start', 'CDR1-IMGT end', 'CDR2-IMGT start',
            'CDR2-IMGT end', 'CDR3-IMGT start', 'CDR3-IMGT end',
            'JUNCTION start', 'JUNCTION end', 'D-J-REGION start',
            'D-J-REGION end'
        ]]

        df_3.columns = [
            'vdjregion_sequence_nt', 'vjregion_sequence_nt',
            'djregion_sequence_nt', 'vregion_sequence_nt',
            'jregion_sequence_nt', 'dregion_sequence_nt',
            'fr1region_sequence_nt', 'fr2region_sequence_nt',
            'fr3region_sequence_nt', 'fr4region_sequence_nt',
            'cdr1region_sequence_nt', 'cdr2region_sequence_nt',
            'cdr3region_sequence_nt', 'junction_nt', 'vdjregion_start',
            'vdjregion_end', 'vjregion_start', 'vjregion_end', 'v_start',
            'v_end', 'j_start', 'j_end', 'd_start', 'd_end', 'fwr1_start',
            'fwr1_end', 'fwr2_start', 'fwr2_end', 'fwr3_start', 'fwr3_end',
            'fwr4_start', 'fwr4_end', 'cdr1_start', 'cdr1_end', 'cdr2_start',
            'cdr2_end', 'cdr3_start', 'cdr3_end', 'junction_start',
            'junction_end', 'djregion_start', 'djregion_end'
        ]

        df_4 = IMGT_gapped_AA_sequences_4[[
            'V-D-J-REGION', 'V-J-REGION', 'V-REGION', 'J-REGION', 'FR1-IMGT',
            'FR2-IMGT', 'FR3-IMGT', 'FR4-IMGT', 'CDR1-IMGT', 'CDR2-IMGT',
            'CDR3-IMGT', 'JUNCTION'
        ]]

        df_4.columns = [
            'vdjregion_sequence_aa_gapped', 'vjregion_sequence_aa_gapped',
            'vregion_sequence_aa_gapped', 'jregion_sequence_aa_gapped',
            'fr1region_sequence_aa_gapped', 'fr2region_sequence_aa_gapped',
            'fr3region_sequence_aa_gapped', 'fr4region_sequence_aa_gapped',
            'cdr1region_sequence_aa_gapped', 'cdr2region_sequence_aa_gapped',
            'cdr3region_sequence_aa_gapped', 'junction_sequence_aa_gapped'
        ]

        df_5 = AA_sequences_5[[
            'V-D-J-REGION', 'V-J-REGION', 'V-REGION', 'J-REGION', 'FR1-IMGT',
            'FR2-IMGT', 'FR3-IMGT', 'FR4-IMGT', 'CDR1-IMGT', 'CDR2-IMGT',
            'CDR3-IMGT', 'JUNCTION'
        ]]

        df_5.columns = [
            'vdjregion_sequence_aa', 'vjregion_sequence_aa',
            'vregion_sequence_aa', 'jregion_sequence_aa',
            'fr1region_sequence_aa', 'fr2region_sequence_aa',
            'fr3region_sequence_aa', 'fr4region_sequence_aa',
            'cdr1region_sequence_aa', 'cdr2region_sequence_aa',
            'cdr3region_sequence_aa', 'junction_aa'
        ]

        df_7 = V_REGION_mutation_and_AA_change_table_7[[
            'V-REGION', 'FR1-IMGT', 'FR2-IMGT', 'FR3-IMGT', 'CDR1-IMGT',
            'CDR2-IMGT', 'CDR3-IMGT'
        ]]

        df_7.columns = [
            'vregion_mutation_string', 'fr1region_mutation_string',
            'fr2region_mutation_string', 'fr3region_mutation_string',
            'cdr1region_mutation_string', 'cdr2region_mutation_string',
            'cdr3region_mutation_string'
        ]

        df_concat = pd.concat([df_1, df_2, df_3, df_4, df_5, df_7], axis=1)
        df_concat['annotation_date'] = Para_dict['Date']
        df_concat['tool_version'] = Para_dict['IMGT/V-QUEST programme version']
        df_concat['reference_version'] = Para_dict[
            'IMGT/V-QUEST reference directory release']
        df_concat['species'] = Para_dict['Species']
        df_concat['receptor_type'] = Para_dict['Receptor type or locus']
        df_concat['reference_directory_set'] = Para_dict[
            'IMGT/V-QUEST reference directory set']
        df_concat['search_insert_delete'] = Para_dict[
            'Search for insertions and deletions']
        df_concat['no_nucleotide_to_add'] = Para_dict[
            "Nb of nucleotides to add (or exclude) in 3' of the V-REGION for the evaluation of the alignment score"]
        df_concat['no_nucleotide_to_exclude'] = Para_dict[
            "Nb of nucleotides to exclude in 5' of the V-REGION for the evaluation of the nb of mutations"]
        df_concat = df_concat.where((pd.notnull(df_concat)), "")
        df_concat['cdr1_length'] = df_concat['cdr1region_sequence_aa'].apply(
            len)
        df_concat['cdr2_length'] = df_concat['cdr2region_sequence_aa'].apply(
            len)
        df_concat['cdr3_length'] = df_concat['cdr3region_sequence_aa'].apply(
            len)
        df_concat['functional'] = df_concat['functionality'].apply(functional_boolean)

        sampleid = self.context.samples.find({
            "imgt_file_name": {
                '$regex': fileName
            }
        }, {'_id': 1})
        ir_project_sample_id = [i['_id'] for i in sampleid][0]
        # Critical iReceptor specific fields
        # The internal Mongo sample ID that links the sample to each sequence, constant
        # for all sequences in this file.
        df_concat['ir_project_sample_id'] = ir_project_sample_id
        # The annotation tool used
        df_concat['ir_annotation_tool'] = "V-Quest"

        # Generate the substring field, which we use to heavily optmiize junction AA
        # searches.
        df_concat['substring'] = df_concat['junction_aa'].apply(Parser.get_substring)

        # Process the IMGT VQuest v/d/j strings and generate the required columns the repository
        # needs, which is [vdj]_call, [vdj]gene_gene, [vdj]gene_family
        Parser.processGene(self.context, df_concat, "v_string", "v_call", "vgene_gene", "vgene_family")
        Parser.processGene(self.context, df_concat, "j_string", "j_call", "jgene_gene", "jgene_family")
        Parser.processGene(self.context, df_concat, "d_string", "d_call", "dgene_gene", "dgene_family")

        # Generate the junction length values as required.
        df_concat['junction_length'] = df_concat['junction_nt'].apply(len)
        df_concat['junction_aa_length'] = df_concat['junction_aa'].apply(len)

        records = json.loads(df_concat.T.to_json()).values()

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
