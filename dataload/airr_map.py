# Class to implement mappings between fields across AIRR data

import pandas as pd

class AIRRMap:
    def __init__(self, verbose):
        # Set up initial class mappings from the file. These are defined in the
        # MiAIRR Standard.
        self.repertoire_class = "Repertoire"
        self.rearrangement_class = "Rearrangement"
        self.clone_class = "Clone"
        # Create an internal class for IR Repertoire objects. This is defined in the
        # Mapping file and should be one of the values in the ir_class column.
        self.ir_repertoire_class = "IR_Repertoire"
        self.ir_rearrangement_class = "IR_Rearrangement"
        self.ir_rearrangement_class = "IR_Clone"

        # Keep track of the mapfile being used.
        self.mapfile = ""
        # Keep track of the verbosity flag.
        self.verbose = verbose
        # Initialize the internal data structures
        # Full mapping set.
        self.airr_mappings = []
        # AIRR rearrangement mappings only
        self.airr_rearrangement_map = []
        # AIRR clone mappings only
        self.airr_clone_map = []
        # AIRR repertoire mappings only
        self.airr_repertiore_map = []
        # AIRR and IR repertoire mapping only
        self.ir_repertiore_map = []
        # AIRR and IR rearrangement mappings only
        self.ir_rearrangement_map = []
        # AIRR and IR clone mappings only
        self.ir_clone_map = []
        
    # Read in a map file given a file name.
    def readMapFile(self, mapfile):
        # Load the mapfile in.
        try:
            self.airr_mappings = pd.read_csv(mapfile, sep='\t')
        except:
            print("Error: Could not load AIRR Map file %s" % mapfile)
            return False 

        # If we have read a mapfile, keep track of the file name.
        self.mapfile = mapfile

        # We need the ir_class column to be in the AIRR Mapping.
        if not "ir_class" in self.airr_mappings:
            print("ERROR: Could not find required ir_subclass field in AIRR Mapping")
            return False

        # We need the ir_subclass column to be in the AIRR Mapping.
        if not "ir_subclass" in self.airr_mappings:
            print("ERROR: Could not find required ir_class field in AIRR Mapping")
            return False

        # Write some diagnostics about the file read in
        if self.verbose:
            print("Info: Successfully read in %d mapping columns from %s" %
                  (len(self.airr_mappings.columns), mapfile))

        # Get the labels for all of the fields that are in the airr rearrangements class.
        labels = self.airr_mappings['ir_class'].isin([self.rearrangement_class])
        # Get all of the rows that have the rearrangement class labels.
        self.airr_rearrangement_map = self.airr_mappings.loc[labels]

        # Get the labels for all of the fields that are in the airr rearrangements class.
        labels = self.airr_mappings['ir_class'].isin([self.rearrangement_class,
                                                      self.ir_rearrangement_class])
        # Get all of the rows that have the rearrangement class labels.
        self.ir_rearrangement_map = self.airr_mappings.loc[labels]

        # Get the labels for all of the fields that are in the airr clone class.
        labels = self.airr_mappings['ir_class'].isin([self.clone_class])
        # Get all of the rows that have the clone class labels.
        self.airr_clone_map = self.airr_mappings.loc[labels]

        # Get the labels for all of the fields that are in the airr and IR clone class.
        labels = self.airr_mappings['ir_class'].isin([self.clone_class,
                                                      self.ir_clone_class])
        # Get all of the rows that have the clone class labels.
        self.ir_clone_map = self.airr_mappings.loc[labels]

        # Get the labels for all of the fields that are in the airr repertoire class.
        labels = self.airr_mappings['ir_class'].isin([self.repertoire_class])
        # Get all of the rows that have the repertoire class labels.
        self.airr_repertoire_map = self.airr_mappings.loc[labels]

        # Get the labels for all of the fields that are in the airr repertoire class.
        labels = self.airr_mappings['ir_class'].isin([self.repertoire_class,
                                                      self.ir_repertoire_class])
        # Get all of the rows that have the AIRR and IR repertoire class labels.
        self.ir_repertoire_map = self.airr_mappings.loc[labels]

        # Return success if we get here.
        return True

    # Abstract the class strings for Repertoire and Rearrangements.
    def getRepertoireClass(self):
        return self.repertoire_class

    def getIRRepertoireClass(self):
        return self.ir_repertoire_class

    def getRearrangementClass(self):
        return self.rearrangement_class

    def getIRRearrangementClass(self):
        return self.ir_rearrangement_class

    # Utility function to determine if the mapping has a specific column
    def hasColumn(self, column_name):
        if column_name in self.airr_mappings:
            return True
        else:
            return False

    # Return the value for the row and column keys provided. If it can't be found
    # None is returned. 
    def getMapping(self, field, from_column, to_column, map_class=None):
        # Get the mapping to use
        if map_class is None:
           mapping = self.airr_mappings
        elif map_class == self.rearrangement_class: 
           mapping = self.airr_rearrangement_map
        elif map_class == self.clone_class: 
           mapping = self.airr_clone_map
        elif map_class == self.repertoire_class: 
           mapping = self.airr_repertoire_map
        elif map_class == self.ir_repertoire_class: 
           mapping = self.ir_repertoire_map
        elif map_class == self.ir_rearrangement_class: 
           mapping = self.ir_rearrangement_map
        elif map_class == self.ir_clone_class: 
           mapping = self.ir_clone_map
        else:
            print("Warning: Invalid maping class %s"%(map_class))
            return None

        # Check to see if we have a valid from_column, if not return None
        if not from_column in mapping:
            return None
        # Get the data in the from_column
        from_column_data = mapping[from_column]
        # Get a boolean array that is true where we found the field of interest.
        from_boolean = from_column_data.isin([field])
        # And extract all rows that have the from key.
        from_row = mapping.loc[from_boolean]
        # If we can't find the to_column in the from_row then we couldn't find it
        # because the to_column doesn't exist in our mapping.
        if not to_column in from_row:
            return None
        # Get the value. This is an object atill so we need to get the values from the
        # dictionary.
        value = from_row[to_column]
        # This could be an array. If it is, we only return a mapping for unique objects,
        # so if there is more than one value return None. If there is one, return it
        if len(value.values) == 1:
            if pd.notnull(value.values[0]):
                return value.values[0]
            else:
                return None
        elif len(value.values) > 1:
            print("Warning: Duplicate AIRR mapping for field %s (%s -> %s)"%
                  (field, from_column, to_column))
            return value.values[0]
        else:
            return None


    # Return a full column of the Rearrangment mapping based on the name given.
    # Return None if the column is not in the mapping.
    def getRearrangementMapColumn(self, column_name):
        if column_name in self.airr_rearrangement_map:
            return self.airr_rearrangement_map[column_name]
        else:
            return None

    # Return the rows in the rearrangement table that are marked as true in the 
    # boolean array provided. The boolean array must be the same size as the
    # Rearrangement table size.
    def getRearrangementRows(self, extract_flags):
        return self.airr_rearrangement_map.loc[extract_flags]

    # Return a full column of the Rearrangment mapping based on the name given.
    # Return None if the column is not in the mapping.
    def getIRRearrangementMapColumn(self, column_name):
        if column_name in self.ir_rearrangement_map:
            return self.ir_rearrangement_map[column_name]
        else:
            return None

    # Return the rows in the rearrangement table that are marked as true in the 
    # boolean array provided. The boolean array must be the same size as the
    # Rearrangement table size.
    def getIRRearrangementRows(self, extract_flags):
        return self.ir_rearrangement_map.loc[extract_flags]


    # Return a full column of the Clone mapping based on the name given.
    # Return None if the column is not in the mapping.
    def getCloneMapColumn(self, column_name):
        if column_name in self.airr_clone_map:
            return self.airr_clone_map[column_name]
        else:
            return None

    # Return the rows in the clone table that are marked as true in the 
    # boolean array provided. The boolean array must be the same size as the
    # Clone table size.
    def getCloneRows(self, extract_flags):
        return self.airr_clone_map.loc[extract_flags]

    # Return a full column of the Clone mapping based on the name given.
    # Return None if the column is not in the mapping.
    def getIRCloneMapColumn(self, column_name):
        if column_name in self.ir_clone_map:
            return self.ir_clone_map[column_name]
        else:
            return None

    # Return the rows in the clone table that are marked as true in the 
    # boolean array provided. The boolean array must be the same size as the
    # Clone table size.
    def getIRCloneRows(self, extract_flags):
        return self.ir_clone_map.loc[extract_flags]

    # Return a full column of the Repertoire mapping based on the name given.
    # Return None if the column is not in the mapping.
    def getRepertoireMapColumn(self, column_name):
        if column_name in self.airr_repertoire_map:
            return self.airr_repertoire_map[column_name]
        else:
            return None

    # Return the rows in the repertoire table that are marked as true in the 
    # boolean array provided. The boolean array must be the same size as the
    # repertoire table size.
    def getRepertoireRows(self, extract_flags):
        return self.airr_repertoire_map.loc[extract_flags]

    # Return a full column of the iReceptor Repertoire mapping based on the name given.
    # Return None if the column is not in the mapping.
    def getIRRepertoireMapColumn(self, column_name):
        if column_name in self.ir_repertoire_map:
            return self.ir_repertoire_map[column_name]
        else:
            return None

    # Return the rows in the iReceptor repertoire table that are marked as true in the 
    # boolean array provided. The boolean array must be the same size as the
    # repertoire table size.
    def getIRRepertoireRows(self, extract_flags):
        return self.ir_repertoire_map.loc[extract_flags]
