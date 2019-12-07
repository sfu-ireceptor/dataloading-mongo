# Parent Parser class of data file type specific AIRR data parsers
# Extracted common code patterns shared across various parsers.

from os.path import join
from datetime import datetime
from datetime import timezone
import re
import os
import pandas as pd
from parser import Parser


class Rearrangement(Parser):

    # Class constructor
    def __init__(self, verbose, repository_tag, repository_chunk, airr_map, repository):
        # Initialize the base class
        Parser.__init__(self, verbose, repository_tag, repository_chunk, airr_map, repository)
        # Each rearrangement parser is used to parse data from an annotation tool.
        # This keeps track of the annotation tool being used and is used
        # to insert annotation tool information into the repository.
        # Subclasses that process data files from a specific type of 
        # annotation tool should set this value.
        # This is only used for rearrangement files.
        self.annotation_tool = ""
        # Each file has fields in it. This variable holds the mapping column
        # from the AIRR Mapping file to use for this parser. Again, subclasses
        # for specific file types should explicitly set this to the correct
        # column.
        self.file_mapping = ""

    # Method to set the Annotation Tool for the class.
    def setAnnotationTool(self, toolname):
        self.annotation_tool = toolname

    # Method to get the Annotation Tool for the class.
    def getAnnotationTool(self):
        return self.annotation_tool

    # Method to set the File Mapping column for the class.
    def setFileMapping(self, file_mapping):
        self.file_mapping = file_mapping

    # Method to get the File Mapping column for the class.
    def getFileMapping(self):
        return self.file_mapping

    @staticmethod
    def get_all_substrings(string):
        if type(string) == float:
            return
        else:
            length = len(string)
            for i in range(length):
                for j in range(i + 1, length + 1):
                    yield (string[i:j])

    @staticmethod
    def get_substring(string):
        strlist = []
        for i in Rearrangement.get_all_substrings(string):
            if len(i) > 3:
                strlist.append(i)
        return strlist

    # Process a gene call to generate the appropriate call, gene, and family
    # fields in teh data frame.
    # Inputs:
    #    - dataframe: the dataframe to process. The call_tage should exist
    #                 within this dataframe, and the gene_tag, family_tag
    #                 columns will be created within this data frame based
    #                 on the call_tag column
    #    - call_tag: a string that represents the column name to be processed
    #    - gene_tag: a string that represents the column name of the gene tag
    #                 to be created
    #    - family_tag: a string that represents the column name of the gene tag
    #                 to be created
    def processGene(self, dataframe, base_tag, call_tag, gene_tag, family_tag):
        # Build the gene call field, as an array if there is more than one gene
        # assignment made by the annotator.
            if base_tag in dataframe:
                if self.verbose():
                    print("Info: Constructing %s array from %s"%(call_tag, base_tag), flush=True)
                dataframe[call_tag] = dataframe[base_tag].apply(Rearrangement.setGene)

                # Build the vgene_gene field (with no allele)
                if self.verbose():
                    print("Info: Constructing %s from %s"%(gene_tag, base_tag), flush=True)
                dataframe[gene_tag] = dataframe[call_tag].apply(Rearrangement.setGeneGene)

                # Build the vgene_family field (with no allele and no gene)
                if self.verbose():
                    print("Info: Constructing %s from %s"%(family_tag, base_tag), flush=True)
                dataframe[family_tag] = dataframe[call_tag].apply(Rearrangement.setGeneFamily)



    # A method to take a list of gene assignments from an annotation tool
    # and create an array of strings with just the allele strings without
    # the cruft that the annotators add.
    @staticmethod
    def setGene(gene):
        # Do some error checking to ensure we have a string. If not return
        # an empty list.
        gene_list = list()
        if gene == None or not type(gene) is str or gene == '':
            return gene_list

        # Split the string based on possible string delimeters.
        gene_string = re.split(',| ', gene)
        gene_orig_list = list(set(gene_string))

        # If there are no strings in the list, return the empty list.
        if len(gene_orig_list) == 0:
            return gene_list
        else:
            # Only keep genes that start with either IG or TR.
            for gene in gene_orig_list:
                if gene.startswith("IG") or gene.startswith("TR"):
                    gene_list.append(gene)

            return gene_list

    # function  to extract just the gene from V/D/J-GENE fields   
    # essentially ignore the part of the gene after *, if it exists     
    @staticmethod
    def setGeneGene(gene_array):
        gene_gene = list()

        for gene in gene_array:
            pattern = re.search('([^\*]*)\*', gene)
            if pattern == None:
                #there wasn't an allele - gene is same as _call
                if gene not in gene_gene:
                    gene_gene.append(gene)                
            else:
                if pattern.group(1) not in gene_gene:               
                    gene_gene.append(pattern.group(1))
        return gene_gene 

    #function to extract just the family from V/D/J-GENE fields
    # ignore part of the gene after -, or after * if there's no -
    @staticmethod
    def setGeneFamily(gene_array):
        gene_family = list()
        for gene in gene_array:
            pattern = re.search('([^\*^-]*)[\*\-]', gene)
            if pattern == None:
                #there wasn't an allele - gene is same as _call
                if gene not in gene_family:
                    gene_family.append(gene)
                else:
                    1
            else:
                if pattern.group(1) not in gene_family:
                    gene_family.append(pattern.group(1))
        return gene_family

    # Function to extract the locus from an array of v_call rearrangements. 
    # Returns a string that is the first three characters of the
    # above fields, as specified in the MiAIRR standard (as of v1.2.1, it should
    # be one of "IGH, IGK, IGL, TRA, TRB, TRD, or TRG". We perform a sanity check
    # to make sure that if there is a v_call that the locus for all valid
    # v_calls is the same.
    @staticmethod
    def getLocus(v_call_array):
        final_locus = ''
        # Iterate over the v_calls in the v_call_array.
        for v_call in v_call_array:
            # If the length isn't 3, then we don't have a locus yet.
            if len(final_locus) != 3:
                # If the len of the v_call is more than three, take the first three characters.
                # Otherwise print a warning, as all v_calls should be > 3 in length.
                if len(v_call) > 3:
                    final_locus = v_call[:3]
                else:
                    print("Warning: Attempting to set locus fron invalid v_call " + v_call)
            # If we have a locus, check to see if it is the same in the current v_call. If not
            # also print a warning.
            elif v_call[:3] != final_locus:
                print("Warning: Inconsistent loci across " + str(v_call_array))
        # Sanity check, check to see that we found a locus of length three and that it
        # is either a valid IG or TR value.
        #if len(final_locus) != 3:
        #    print("Warning: unable to compute locus from v_call " + str(v_call_array))
        if len(final_locus) == 3 and final_locus[:2] != "IG" and final_locus[:2] != "TR":
            print("Warning: locus with non IG and non TR found in " + str(v_call_array))
        return final_locus

    # Hide the use of the scratch directory for temporary files from the subclasses.
    def getScratchPath(self, fileName):
        return join(self.getScratchFolder(), fileName)

    def readScratchDf(self, fileName, sep=','):
        return pd.read_csv(self.getScratchPath(fileName), sep, low_memory=False)

    def readScratchDfNoHeader(self, fileName, sep=','):
        return pd.read_csv(self.getScratchPath(fileName), sep, low_memory=False, header=None)

    #####################################################################################
    # Hide the repository implementation from the Rearrangement subclasses.
    #####################################################################################

    # Look for the file_name given in the repertoire collection in the file_field field
    # in the repository. Return an array of integers which are the sample IDs where the
    # file_name was found in the field field_name.
    def repositoryGetRepertoireIDs(self, file_field, file_name):
        return self.repository.getRepertoireIDs(file_field, file_name)

    # Write the set of JSON records provided to the "rearrangements" collection.
    # This is hiding the Mongo implementation. Probably should refactor the 
    # repository implementation completely.
    def repositoryInsertRearrangements(self, json_records):
        self.repository.insertRearrangements(json_records)

    # Count the number of rearrangements that belong to a specific repertoire. Note: In our
    # early implementations, we had an internal field name called ir_project_sample_id. We
    # want to hide this and just talk about reperotire IDs, so this is hidden in the 
    # Rearrangement class...
    def repositoryCountRearrangements(self, repertoire_id):
        repertoire_field = self.airr_map.getMapping("ir_project_sample_id",
                                                    "ir_id",
                                                    self.repository_tag)
        return self.repository.countRearrangements(repertoire_field, repertoire_id)

    # Update the cached sequence count for the given reperotire to be the given count.
    def repositoryUpdateCount(self, repertoire_id, count):
        self.repository.updateCount(repertoire_id, "ir_sequence_count", count)

