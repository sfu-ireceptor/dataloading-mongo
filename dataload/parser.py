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
        self.rearrangement_linkid_field = "repertoire_id_rearrangement"

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


    # Utility function to map a key of a specific value to the correct field for
    # the repository. Possible to limit the mapping to a class in the mapping.
    def fieldToRepository(self, field, map_class=None):
        # Define the columns to use for the mappings
        airr_tag = "airr"

        # Check to see if the field is in the AIRR mapping, if not warn.
        airr_field = self.getAIRRMap().getMapping(field, airr_tag, airr_tag, map_class)
        if airr_field is None:
            print("Warning: Could not find %s in AIRR mapping"%(field))

        # Check to see if the field can be mapped to a field in the repository, if not warn.
        repo_field = self.getAIRRMap().getMapping(field, airr_tag, self.repository_tag,
                                                  map_class)
        if repo_field is None:
            repo_field = field
            print("Warning: Could not find repository mapping for %s, storing as is"%(field))

        # If we are verbose, tell about the mapping...
        if self.verbose():
            print("Info: Mapping %s => %s" % (field, repo_field))

        # Return the mapping.
        return repo_field


    # Utility function to map a key of a specific value to the correct type for
    # the repository. 
    def valueToRepository(self, field, field_column, value, map_class=None):
        # Define the columns to use for the mappings
        airr_type_tag = "airr_type"
        repository_type_tag = "ir_repository_type"

        # Get the types of the fields, both the AIRR type and the repository type
        airr_field_type = self.getAIRRMap().getMapping(field, field_column,
                                                       airr_type_tag, map_class)
        repository_field_type = self.getAIRRMap().getMapping(field, field_column,
                                                             repository_type_tag, map_class)

        # Do the conversion for the value
        rep_value = value
        if repository_field_type == "string":
            # We don't want null strings, we want empty strings.
            if value is None:
                rep_value = ""
            else:
                rep_value = str(value)
        elif repository_field_type == "boolean":
            rep_value = bool(value)
        elif repository_field_type == "integer":
            # We allow integers to be null, as we don't know what to replace them
            # with.
            if value is None:
                rep_value = None
            else:
                rep_value = int(value)
        elif repository_field_type == "number":
            rep_value = float(value)
        else:
            if self.verbose():
                print("Info: Unable to convert field %s (%s -> %s), no conversion done"%
                      (field, airr_field_type, repository_field_type))
         
        #print("Info: Converting field %s (%s -> %s)"%(field, value, rep_value))
        return rep_value

    #####################################################################################
    # Hide the repository implementation from the Parser subclasses.
    #####################################################################################

    # Look for the file_name given in the repertoire collection in the file_field field
    # in the repository. Return an array of integers which are the sample IDs where the
    # file_name was found in the field field_name.
    def repositoryGetRepertoireIDs(self, search_field, search_name):
        # Get the field the repository is using to linke repertoires and rearrangements.
        # That is the field we want to use to generate the repertoire IDs
        repertoire_field = self.airr_map.getMapping(self.getRepertoireLinkIDField(),
                                                    "ir_id",
                                                    self.repository_tag)
        # Ask the repository to do the search and return the results.
        return self.repository.getRepertoireIDs(repertoire_field, search_field, search_name)

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

