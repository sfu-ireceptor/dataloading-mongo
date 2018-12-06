# Parent Parser class of data file type specific AIRR data parsers
# Extracted common code patterns shared across various parsers.

from os.path import join
import re
import pandas as pd


class Parser:
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
        for i in Parser.get_all_substrings(string):
            if len(i) > 3:
                strlist.append(i)
        return strlist

    # Process a gene call to generate the appropriate call, gene, and family
    # fields in teh data frame.
    # Inputs:
    #    - context: the context for the processing to take place
    #    - dataframe: the dataframe to process. The call_tage should exist
    #                 within this dataframe, and the gene_tag, family_tag
    #                 columns will be created within this data frame based
    #                 on the call_tag column
    #    - call_tag: a string that represents the column name to be processed
    #    - gene_tag: a string that represents the column name of the gene tag
    #                 to be created
    #    - family_tag: a string that represents the column name of the gene tag
    #                 to be created
    @staticmethod
    def processGene(context, dataframe, base_tag, call_tag, gene_tag, family_tag):
        # Build the gene call field, as an array if there is more than one gene
        # assignment made by the annotator.
            if base_tag in dataframe:
                if context.verbose:
                    print("Constructing %s array from %s"%(call_tag, base_tag), flush=True)
                dataframe[call_tag] = dataframe[base_tag].apply(Parser.setGene)

                # Build the vgene_gene field (with no allele)
                if context.verbose:
                    print("Constructing %s from %s"%(gene_tag, base_tag), flush=True)
                dataframe[gene_tag] = dataframe[call_tag].apply(Parser.setGeneGene)

                # Build the vgene_family field (with no allele and no gene)
                if context.verbose:
                    print("Constructing %s from %s"%(family_tag, base_tag), flush=True)
                dataframe[family_tag] = dataframe[call_tag].apply(Parser.setGeneFamily)



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
        if len(final_locus) != 3:
            print("Warning: unable to compute locus from v_call " + str(v_call_array))
        if final_locus[:2] != "IG" and final_locus[:2] != "TR":
            print("Warning: locus with non IG and non TR found in " + str(v_call_array))
        return final_locus

    def __init__(self, context):
        self.context = context

    def getDataFolder(self):
        return self.context.library + "/" + self.context.type + "/"

    def getDataPath(self, fileName):
        return join(self.getDataFolder(), fileName)

    scratchFolder = ""

    # We create a specific temporary 'scratch' folder for each sequence archive
    def setScratchFolder(self, fileName):
        folderName = fileName[:fileName.index('.')]
        self.scratchFolder = self.getDataFolder() + folderName + "/"

    def getScratchFolder(self):
        return self.scratchFolder

    def getScratchPath(self, fileName):
        return join(self.getScratchFolder(), fileName)

    def readDf(self, fileName):
        return pd.read_table(self.getScratchPath(fileName))

    def readDfNoHeader(self, fileName):
        return pd.read_table(self.getScratchPath(fileName), header=None)
