# Parent Parser class of data file type specific AIRR data parsers
# Extracted common code patterns shared across various parsers.

from os.path import join
from datetime import datetime
from datetime import timezone
import re
import os
import pandas as pd
import numpy as np


class Parser:

    # Class constructor
    def __init__(self, verbose, repository_tag, repository_chunk, airr_map, repository):

        # Keep track of the verbosity level
        self.verbose_level = verbose

        # The tag in the map file to use for the repository
        self.repository_tag = repository_tag

        # The tag in the map file to use for iReceptor specific fields
        self.ireceptor_tag = "ir_id" 

        # The tag in the map file to use for iReceptor specific fields
        self.airr_tag = "airr" 

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
        # rearrangements or clones in a specific file should be associated.
        #
        # Secondly, it is possible to use the reperotire file name of the file 
        # being loaded to identify the repertoire to which the rearrangements
        # and clones belong. The file name for rearrangements and cloens is stored
        # in a field (again, identified by a specific iReceptor field).
        #
        # Below, we keep track of these important fields. This is
        # maintained by the Parser class because these are the fields that link
        # the two types of parsed files. repertoire_link_id field is the ID of
        # record in the repertoire collection. Rearrangements are associated with
        # this ID through the field identified by the rearrangement_linkid_field.
        # The repertoire_file_field is the field in the repertoire where file
        # names for rearrangement and clone files are stored. This is the main lookup
        # mechanism when a rearrangement or clone file is loaded against a repertoire.
        # Finally ir_sequence_count it the internal field that the repository
        # uses to cache the could of all the sequences that belong to a specific
        # repertoire record.
        self.repertoire_linkid_field = "ir_annotation_set_metadata_id"
        self.repertoire_file_field = "ir_rearrangement_file_name"
        self.rearrangement_count_field = "ir_sequence_count"

        # We need to keep track of the field (identified by an iReceptor 
        # field name) in the annotation collection that points to the
        # Repertoire ID field in the repertoire collection. This should be
        # set by the subclass (Rearrangement or Clone).
        self.annotation_linkid_field = ""

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

    def getRepertoireFileField(self):
        return self.repertoire_file_field

    def getRearrangementCountField(self):
        return self.rearrangement_count_field

    def getAnnotationLinkIDField(self):
        return self.annotation_linkid_field

    # Utility method to return the tag used by the mapping for the repository.
    def getRepositoryTag(self):
        return self.repository_tag
    # Utility method to return the tag used by the mapping for the iReceptor fields.
    def getiReceptorTag(self):
        return self.ireceptor_tag
    # Utility method to return the tag used by the mapping for the AIRR fields.
    def getAIRRTag(self):
        return self.airr_tag

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

        # Check to see if the field is in the AIRR mapping, if not warn.
        airr_field = self.getAIRRMap().getMapping(field, self.airr_tag,
                                                  self.airr_tag, map_class)
        if airr_field is None:
            print("Warning: Could not find %s in AIRR mapping"%(field))

        # Check to see if the field can be mapped to a field in the repository, if not warn.
        repo_field = self.getAIRRMap().getMapping(field, self.airr_tag,
                                                  self.repository_tag, map_class)
        if repo_field is None:
            repo_field = field
            print("Warning: Could not find repository mapping for %s, storing as is"%(field))

        # If we are verbose, tell about the mapping...
        if self.verbose():
            print("Info: Mapping %s => %s" % (field, repo_field))

        # Return the mapping.
        return repo_field

    @staticmethod
    def len_null_to_0(value):
        if pd.isnull(value) or value is None or not isinstance(value,(str)):
            return 0
        else:
            return len(value)

    @staticmethod
    def len_null_to_null(value):
        if pd.isnull(value) or value is None or not isinstance(value,(str)):
            return np.nan
        else:
            return len(value)

    @staticmethod
    def null_integer_to_0(value):
        if pd.isnull(value) or value is None:
            return 0
        else:
            return value

    @staticmethod
    def to_string(value):
        # Some string values are lists of strings, so we return the list.
        # For now we don't check the contents of the list for string type.
        if isinstance(value, (list)):
            return value
        # Null values are OK
        elif pd.isnull(value):
            return np.nan
        # If its a string, return it
        elif isinstance(value, (str)):
            return value
        # Convert to string, if it generates an error that is OK as errors
        # are expected as failure mode for this method.
        else:
            return str(value)

        # If we get here something bad happened
        raise TypeError("Can't convert value %s (%s) to string"%(str(value), type(value)))

    @staticmethod
    def to_number(value):
        # Some string values are lists of strings, so we return the list.
        # For now we don't check the contents of the list for string type.
        if isinstance(value, (list)):
            raise TypeError("Can't convert value %s (%s) to number"
                            %(str(value), type(value)))
        # Null values are OK
        elif pd.isnull(value):
            return np.nan
        # If its a float or an integer, return it
        elif isinstance(value, (float, int)):
            return float(value)
        # If its a string, try and convert it, handling the error
        elif isinstance(value, (str)):
            # If it is an empty string, treat that has a Null
            if value == "":
                return np.nan
            # Try and convert the string and if it fails, handle the error.
            try:
                return float(value)
            except Exception as err:
                raise TypeError("Can't convert value %s (%s) to string"
                                 %(str(value), type(value)))
        # If we get here something bad happened
        raise TypeError("Can't convert value %s (%s) to string"
                        %(str(value), type(value)))

    @staticmethod
    def to_integer(value):
        # Some string values are lists of strings, so we return an error. We
        # don't check for lists of integers.
        if isinstance(value, (list)):
            raise TypeError("Can't convert value %s (%s) to integer"
                            %(str(value), type(value)))
        # Null values are OK
        elif pd.isnull(value):
            return np.nan
        # If its a integer, return it
        elif isinstance(value, int):
            return value
        # If its a float, try and convert it.
        elif isinstance(value, float):
            if int(value) == value:
                return int(value)
            else:
                raise TypeError("Can't convert value %f (%s) to integer"
                                %(value, type(value)))
        # If its a string, try and convert it, handling the error
        elif isinstance(value, (str)):
            # If it is an empty string, treat that has a Null
            if value == "":
                return np.nan
            # Try and convert the string and if it fails, handle the error.
            try:
                return int(value)
            except Exception as err:
                raise TypeError("Can't convert value %s (%s) to integer"
                                 %(str(value), type(value)))
        # If we get here something bad happened
        raise TypeError("Can't convert value %s (%s) to integer"%(str(value), type(value)))

    @staticmethod
    def to_boolean(value):
        # Null values are OK
        if pd.isnull(value):
            return np.nan
        elif isinstance(value, (bool)):
            return value
        elif isinstance(value, (str)):
            if value in ["T","t","True","TRUE","true","1"]:
            	return True
            elif value in ["F","f","False","FALSE","false","0"]:
                return False
            else:
                raise TypeError("Can't convert string value %s to boolean"%(value))
        elif isinstance(value, (int)):
            if value in [1]:
            	return True
            elif value in [0]:
                return False
            else:
                raise TypeError("Can't convert integer value %d to boolean"%(value))
        # If we get here we failed...
        raise TypeError("Can't convert value %s (%s) to boolean"%(str(value), type(value)))

    @staticmethod
    def str_to_bool(string_value):
        # Null values are OK, as are string variations of various types...
        if pd.isnull(string_value):
            return None
        elif not isinstance(string_value, (str)):
            raise TypeError("Can't convert non-string value " + str(string_value))
        elif string_value in ["T","t","True","TRUE","true","1"]:
            return True
        elif string_value in ["F","f","False","FALSE","false","0"]:
            return False
        # If we get here we failed...
        raise TypeError("Can't convert string " + string_value + " to boolean")

    @staticmethod
    def int_to_bool(int_value):
        # Null values are OK as are 1/0, nothing else...
        if pd.isnull(int_value):
            return None
        elif not isinstance(int_value, (int)):
            raise TypeError("Can't convert non-integer value " + str(int_value))
        elif int_value == 1:
            return True
        elif int_value == 0:
            return False
        # If we get here we failed...
        raise TypeError("Can't convert integer " + str(int_value) + " to boolean")
 
    @staticmethod
    def float_to_str(float_value):
        # Pandas null, return null
        if pd.isnull(float_value):
            return None
        # We expect a float!!!
        elif not isinstance(float_value, (float)):
            raise TypeError("Can't convert non-float value " + str(float_value))
        # Convert away
        else:
            return str(float_value)

    @staticmethod
    def str_to_float(str_value):
        # Pandas null, None, or empty string map to a null value.
        if pd.isnull(str_value) or str_value is None or str_value == "":
            return None
        # If it ain't string, we have a problem...
        elif not isinstance(str_value, (str)):
            raise TypeError("Can't convert non-float value " + str(str_value))
        # Convert away...
        else:
            return float(str_value)

    @staticmethod
    def str_to_int(str_value):
        # Pandas null, None, or empty string map to a null value.
        if pd.isnull(str_value) or str_value is None or str_value == "":
            return None
        # If it ain't string, we have a problem...
        elif not isinstance(str_value, (str)):
            raise TypeError("str_to_int: Can't convert non-string value " + str(str_value))
        # Convert away...
        else:
            float_value = float(str_value)
            if float(int(str_value)) != float_value:
                raise TypeError("str_to_int: Can't convert a non integer float to an integer, " +
                                str_value)
            return int(str_value)

    @staticmethod
    def float_to_int(float_value):
        # Pandas null or None map to a null value.
        if float_value is None or pd.isnull(float_value):
            return None
        # If it ain't float, we have a problem...
        elif not isinstance(float_value, (float)):
            # OK, this is really weird... In and ideal world this function would
            # always take a float, so we wouldn't need the test below. This method
            # would typically get used on a Pandas data frame column, which are 
            # supposed to be all the same type (correct me if I am wrong). For
            # some mysterious reason, when this is called from 
            # Rearrangement.mapToRepositoryType where the type of the first
            # element of a column is a float, somehow other values in the 
            # data frame have different types (an empty string rather than None).
            # This takes care of this case - not sure why this happens!
            if isinstance(float_value, (str)) and float_value == "":
                return None
            raise TypeError("Can't convert non-float value '%s' (%s)"
                            %(str(float_value), type(float_value)))
        # Convert away...
        else:
            if float(int(float_value)) != float_value:
                raise TypeError("Can't convert a non integer float to an integer, " +
                                str(float_value))
            return int(float_value)


    # Utility function to map a key of a specific value to the correct type for
    # the repository. 
    def valueToRepository(self, field, field_column, value, map_class=None):
        # Define the columns to use for the mappings
        airr_type_tag = "airr_type"
        airr_nullable_tag = "airr_nullable"
        airr_array_tag = "airr_is_array"
        repository_type_tag = "ir_repository_type"

        # Get the types of the fields, both the AIRR type and the repository type
        airr_field_type = self.getAIRRMap().getMapping(field, field_column,
                                                airr_type_tag, map_class)
        repository_field_type = self.getAIRRMap().getMapping(field, field_column,
                                                repository_type_tag, map_class)
        field_nullable = self.getAIRRMap().getMapping(field, field_column,
                                                airr_nullable_tag, map_class)
        is_array = self.getAIRRMap().getMapping(field, field_column,
                                                airr_array_tag, map_class)

        # Check for a null value on a nullable field, if it happens this is an error
        # so raise an exception. Note if we could not find a mapping for the
        # field in the nullable mapping column then this is not an error. No nullable
        # mapping means we don't know if it is nullable or not, so we assume it is.
        if value is None and not field_nullable is None and not field_nullable:
            raise TypeError("Null value for AIRR non nullable field " + field)

        # Do a default the conversion for the value
        rep_value = value

        #print("Info: Converting field %s %s"%(field, value))
        #print("Info:    field type %s %s"%(airr_field_type, repository_field_type))
        #print("Info:    nullable %s"%(str(field_nullable)))
        #print("Info:    is_array %s"%(str(is_array)))

        # Handle arrays first... We want to do this as the field_type of an
        # array is a string, which can mess us up.
        if is_array:
            # Handle arrays, a null value is OK...
            if value is None:
                rep_value = None
            elif isinstance(value, list):
                # Currently, the spec only supports arrays of strings. If any of the
                # elements are not string, raise a type error.
                for item in value:
                    self.valueToRepository(field, field_column, item, map_class)
                # Otherwise, return the array of strings...
                rep_value = value
            elif isinstance(value, str):
                # Assume a comma separated list of strings, create the array and return it.
                rep_value = value.split(',')
                if isinstance(rep_value, list):
                    rep_value = [x.strip() for x in rep_value]
                else:
                    raise TypeError("Unable to convert a ',' separated string to an array (" +
                                     value + ")")
            else:
                if self.verbose():
                    print("Info: Unable to convert field %s = %s (%s, %s, %s), not converted"%
                          (field, value, airr_field_type, repository_field_type, type(value)))
        elif repository_field_type == "string":
            # We don't want null strings, we want empty strings.
            if value is None:
                rep_value = None 
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

    # Look for the field given in the repertoire collection in the repository.
    # Return an array of repertoires which match the search criteria 
    def repositoryGetRepertoires(self, search_field, search_name):
        # Ask the repository to do the search and return the results.
        return self.repository.getRepertoires(search_field, search_name)

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

    # Get the folder
    def getScratchFolder(self):
        return self.scratchFolder

    # Get the folder with the filename appended.
    def getScratchPath(self, fileName):
        return join(self.getScratchFolder(), fileName)


    #####################################################################################
    # Hide the internal implementation of performing timing functions.
    #####################################################################################

    @staticmethod
    def getDateTimeNowUTC():
        return datetime.now(timezone.utc).strftime("%a %b %d %Y %H:%M:%S %Z")

