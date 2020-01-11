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

        # The tag in the map file to use for iReceptor specific fields
        self.ireceptor_tag = "ir_id" 

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
        # Firstly, each row in the Repertoire collection has a ID field
        # (identified by a specific iReceptor field) and it is this field 
        # that must be unique across all of the repertoires in the repository. As
        # a result, it is possible to specify a ID to which all of the
        # rearrangements in a specific file should be associated.
        #
        # Secondly, it is possible to use the rearrangement file name of the file 
        # being loaded to identify the repertoire to which the rearrangements
        # belong. The file name for rearrangements is stored in a field (again,
        # identified by a specific iReceptor field).
        #
        # Below, we keep track of these important fields. This is
        # maintained by the Parser class because these are the fields that link
        # the two types of parsed files. repertoire_link_id field is the ID of
        # record in the repertoire collection. Rearrangements are associated with
        # this ID through the field identified by the rearrangement_linkid_field.
        # The rearrangement_file_field is the field in the repertoire where file
        # names for rearrangement files are stored. This is the main lookup
        # mechanism when a rearrangement file is loaded against a repertoire.
        # Finally ir_sequence_count it the internal field that the repository
        # uses to cache the could of all the sequences that belong to a specific
        # repertoire record.
        self.repertoire_linkid_field = "ir_project_sample_id"
        self.rearrangement_file_field = "ir_rearrangement_file_name"
        self.rearrangement_count_field = "ir_sequence_count"

        # Finally, we need to keep track of the field (identified by an iReceptor 
        # field name) in the rearrangement collection that points to the
        # Repertoire ID field in the repertoire collection. This should exist in
        # each rearrangement record.
        self.rearrangement_linkid_field = "ir_project_sample_id_rearrangement"

    # Sanity check for validity for the parser...
    def checkValidity(self):
        if not self.airr_map.hasColumn(self.ireceptor_tag):
            print("ERROR: Could not find required iReceptor column in AIRR Mapping")
            return False
        if not self.airr_map.hasColumn(self.repository_tag):
            print("ERROR: Could not find required Repository column in AIRR Mapping")
            return False
        return True

    # Utility methods to return internal field names of importance
    def getRepertoireLinkIDField(self):
        return self.repertoire_linkid_field

    def getRearrangementFileField(self):
        return self.rearrangement_file_field

    def getRearrangementCountField(self):
        return self.rearrangement_count_field

    def getRearrangementLinkIDField(self):
        return self.rearrangement_linkid_field

    # Utility method to return the tag used by the mapping for the repository.
    def getRepositoryTag(self):
        return self.repository_tag
    # Utility method to return the tag used by the mapping for the repository.
    def getiReceptorTag(self):
        return self.ireceptor_tag

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

    @staticmethod
    def str_to_bool(string_value):
        if not isinstance(string_value, (str)):
            raise TypeError("Can't convert non-string value " + str(string_value))
        elif string_value in ["T","t","True","TRUE","true"]:
            return True
        elif string_value in ["F","f","False","FALSE","false"]:
            return False
        # If we get here we failed...
        raise TypeError("Can't convert string " + string_value + " to boolean")

    @staticmethod
    def int_to_bool(int_value):
        if not isinstance(int_value, (int)):
            raise TypeError("Can't convert non-integer value " + str(int_value))
        elif int_value == 1:
            return True
        elif int_value == 0:
            return False
        # If we get here we failed...
        raise TypeError("Can't convert integer " + str(int_value) + " to boolean")
 
    @staticmethod
    def float_to_str(float_value):
        if not isinstance(float_value, (float)):
            raise TypeError("Can't convert non-float value " + str(float_value))
        elif pd.isnull(float_value):
            return ""
        else:
            return str(float_value)


    # Utility function to map a key of a specific value to the correct type for
    # the repository. 
    def valueToRepository(self, field, field_column, value, map_class=None):
        # Define the columns to use for the mappings
        airr_type_tag = "airr_type"
        airr_nullable_tag = "airr_nullable"
        repository_type_tag = "ir_repository_type"

        # Get the types of the fields, both the AIRR type and the repository type
        airr_field_type = self.getAIRRMap().getMapping(field, field_column,
                                                       airr_type_tag, map_class)
        repository_field_type = self.getAIRRMap().getMapping(field, field_column,
                                                             repository_type_tag, map_class)
        field_nullable = self.getAIRRMap().getMapping(field, field_column,
                                                      airr_nullable_tag, map_class)

        # Check for a null value on a nullable field, if it happens this is an error
        # so raise an exception. Note if we could not find a mapping for the
        # field in the nullable mapping column then this is not an error. No nullable
        # mapping means we don't know if it is nullable or not, so we assume it is.
        if value is None and not field_nullable is None and not field_nullable:
            raise TypeError("Null value for AIRR non nullable field " + field)

        # Do a default the conversion for the value
        rep_value = value

        if repository_field_type == "string":
            # We don't want null strings, we want empty strings.
            if value is None:
                rep_value = ""
            else:
                rep_value = str(value)
        elif repository_field_type == "boolean":
            # Even though python does not allow boolean values to be null
            # (e.g. bool(None) == False), JSON and data repositories often do,
            # so in this case we don't want to return False for a None value. 
            # We want to return None...
            if value is None:
                rep_value = None
            elif isinstance(value,(str)):
                rep_value = Parser.str_to_bool(value)
                #if value in ["T","t","True","TRUE","true"]:
                #    rep_value = True
                #elif value in ["F","f","False","FALSE","false"]:
                #    rep_value = False
                #else:
                #    raise TypeError("Invalid boolean value " + value + " for field " + field)
            elif isinstance(value,(int)):
                rep_value = Parser.int_to_bool(value)
            else:
                rep_value = bool(value)
        elif repository_field_type == "integer":
            # We allow integers to be null, as we don't know what to replace them
            # with.
            if value is None:
                rep_value = None
            else:
                # This is a complex case... We are converting a boolean AIRR field to
                # an integer representation of that boolean value. The problem is that
                # on loading the boolean AIRR field may be represented as an integer (0/1),
                # a string (T/True/TRUE/true), or a boolean (True/False). We want to handle 
                # all of these cases.
                if airr_field_type == "boolean":
                    if isinstance(value, (int)):
                        if value == 1: rep_value = 1
                        elif value == 0: rep_value = 0
                        else:
                            raise TypeError("Invalid boolean value " + str(value) +
                                            " for field " + field)
                    elif isinstance(value, (str)):
                        if value in ["T","t","True","TRUE","true"]:
                            rep_value = 1
                        elif value in ["F","f","False","FALSE","false"]:
                            rep_value = 0
                        else:
                            raise TypeError("Invalid boolean value " + value +
                                            " for field " + field)
                    elif isinstance(value, (bool)):
                        if value == True: rep_value = 1
                        elif value == False: rep_value = 0
                        else:
                            raise TypeError("Invalid boolean value " + str(value) +
                                            " for field " + field)
                    else:
                        raise TypeError("Invalid boolean value " + str(value) +
                                            " for field " + field)

                else:
                    # This is the base case - we convert an integer to an integer...
                    rep_value = int(value)
        elif repository_field_type == "number":
            # We allow floats to be null, as we don't know what to replace them
            # with.
            if value is None:
                rep_value = None
            else:
                rep_value = float(value)
        else:
            if self.verbose():
                print("Info: Unable to convert field %s = %s (%s, %s, %s), not converted"%
                      (field, value, airr_field_type, repository_field_type, type(value)))
         
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
        link_field = self.airr_map.getMapping(self.getRepertoireLinkIDField(),
                                              self.getiReceptorTag(),
                                              self.getRepositoryTag())
        # Ask the repository to do the search and return the results.
        return self.repository.getRepertoireIDs(link_field, search_field, search_name)

    #####################################################################################
    # Hide the internal use of temporary folders from the subclasses
    #####################################################################################

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

    #####################################################################################
    # Hide the internal implementation of performing timing functions.
    #####################################################################################

    @staticmethod
    def getDateTimeNowUTC():
        return datetime.now(timezone.utc).strftime("%a %b %d %Y %H:%M:%S %Z")

