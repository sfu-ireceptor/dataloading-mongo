# Parent Parser class of data file type specific AIRR data parsers
# Extracted common code patterns shared across various parsers.

from os.path import join
from datetime import datetime
from datetime import timezone
import re
import os
import pandas as pd


class Parser:

    # Class constructor
    def __init__(self, verbose, repository_tag, repository_chunk, airr_map, repository):

        # Keep track of the verbosity level
        self.verbose_level = verbose

        # The tag in the map file to use for the repository
        self.repository_tag = repository_tag

        # The size of each data chunk to load in the parser, when loading large files.
        self.repository_chunk = repository_chunk

        # A helper variable that parser's can use if they need to create a scratch
        # folder for storing temporary data in it. The folder name is guaranteed to
        # be unique to the process being run, as it uses the process id in the folder
        # name.
        self.scratchFolder = ""

        # Manage the repository 
        self.repository = repository

        # Save the AIRR Mapping object
        self.airr_map = airr_map

        # Keep track of the fields that we use to link the two types of files
        # that we parse, repertoire and rearrangement files. When we parse
        # rearrangement files, we assume that all of the rearrangements in each
        # file belong to a single repertoire, although it is possible to have more
        # than one file contain rearrangements of the same repertoire.
        #
        # As a result, when we load a file of rearrangements we need to associate
        # each file with a repertoire. There are two ways of doing this.
        # 
        # Firstly, each row in the Repertoire collection has a repertoire ID field
        # (identified by a specific ir_id field) and it is this field 
        # that must be unique across all of the repertoires in the repository. As
        # a result, it is possible to specify a repertoire ID to which all of the
        # rearrangements in a specific file should be associated.
        #
        # Secondly, it is possible to use the rearrangement file name of the file 
        # being loaded to identify the repertoire to which the rearrangements
        # belong. The file name for rearrangements is stored in a field (again,
        # identified by a specific ir_id field).
        #
        # Below, we keep track of both of these important fields. This is
        # maintained by the Parser class because these are the fields that link
        # the two types of parsed files.
        self.repertoire_linkid_field = "ir_project_sample_id"
        self.rearrangement_file_field = "ir_rearrangement_file_name"

        # Finally, we need to keep track of the field (identified by an ir_id
        # field name in the rearrangement collection that points to the
        # Repertoire ID field in the repertoire collection.
        self.rearrangement_linkid_field = "repertoire_id"

    def getRepertoireLinkIDField(self):
        return self.repertoire_linkid_field

    def getRearrangementFileField(self):
        return self.rearrangement_file_field

    def getRearrangementLinkIDField(self):
        return self.rearrangement_linkid_field

    # Utility method to return the tag used by the mapping for the repository.
    def getRepositoryTag(self):
        return self.repository_tag

    # Utility method to return the size of the data set chunks to load in
    # the repository. 
    def getRepositoryChunkSize(self):
        return self.repository_chunk

    # Utility method to return the verbose boolena flag - all parsers need this.
    def verbose(self):
        return self.verbose_level

    # Utility method to get the AIRR Map class associated with this object.
    def getAIRRMap(self):
       return self.airr_map

    #####################################################################################
    # Hide the repository implementation from the Parser subclasses.
    #####################################################################################

    # Look for the file_name given in the repertoire collection in the file_field field
    # in the repository. Return an array of integers which are the sample IDs where the
    # file_name was found in the field field_name.
    def repositoryGetRepertoireIDs(self, file_field, file_name):
        return self.repository.getRepertoireIDs(file_field, file_name)

    # Utility methods to deal with data and scratch folders that all Parsers need.
    def getDataFolder(self, fileWithPath):
        # Data folder is based on the path to the file.
        data_dir = os.path.dirname(fileWithPath)
        # If there is no path, then the data directory is the current directory.
        if (not os.path.isdir(data_dir)):
            data_dir = "."
        return data_dir

    # We create a specific temporary 'scratch' folder for each sequence archive
    def setScratchFolder(self, fileWithPath, fileName):
        folderName = fileName[:fileName.index('.')]
        self.scratchFolder = self.getDataFolder(fileWithPath) + "/tmp_" + str(os.getpid()) + "_" + folderName + "/"

    def getScratchFolder(self):
        return self.scratchFolder

    @staticmethod
    def getDateTimeNowUTC():
        return datetime.now(timezone.utc).strftime("%a %b %d %Y %H:%M:%S %Z")

