# Class to implement mappings between fields across AIRR data

import pandas as pd

class AIRRMap:
    def __init__(self, verbose):
        # Set up initial class mappings from the file. These are defined in the MiAIRR Standard.
        # Repertoire metadata spans a number of MiAIRR data class types.
        self.airr_repertoire_classes = ["study", "subject", "diagnosis", "sample",
                                        "cell_processing", "nucleic_acid_processing",
                                        "sequencing_run", "software_processing"]
        # Rearrangement data has one or two classes, rearrangement defines the
        # the fields in the MiAIRR standard. ir_rearrangement defines the 
        # rearrangement fields that are specific to iReceptor outside of the
        # MiAIRR standard.
        self.airr_rearrangement_classes = ["rearrangement", "ir_rearrangement"]
        #self.airr_rearrangement_classes = ["rearrangement"]
        # Keep track of the mapfile being used.
        self.mapfile = ""
        # Keep track of the verbosity flag.
        self.verbose = verbose
        # Initialize the internal data structures
        self.airr_mappings = []
        self.airr_rearrangement_map = []
        self.airr_repertiore_map = []
        
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

        # We need the ir_subclass column to be in the AIRR Mapping.
        if not "ir_subclass" in self.airr_mappings:
            print("ERROR: Could not find required ir_subclass field in AIRR Mapping")
            return False

        # We need the ir_id column to be in the AIRR Mapping. This is the iReceptor key
        # column that we use across all mapping internally.
        if not "ir_id" in self.airr_mappings:
            print("ERROR: Could not find required ir_id field in AIRR Mapping")
            return False

        # Write some diagnostics about the file read in
        if self.verbose:
            print("Successfully read in %d mapping columns" % (len(self.airr_mappings.columns)))

        # Get the labels for all of the fields that are in the airr rearrangements class.
        labels = self.airr_mappings['ir_subclass'].isin(self.airr_rearrangement_classes)
        # Get all of the rows that have the rearrangement class labels.
        self.airr_rearrangement_map = self.airr_mappings.loc[labels]
        # Get the labels for all of the fields that are in the airr repertoire class.
        labels = self.airr_mappings['ir_subclass'].isin(self.airr_repertoire_classes)
        # Get all of the rows that have the repertoire class labels.
        self.airr_repertoire_map = self.airr_mappings.loc[labels]
        #print(self.airr_repertoire_map['ir_id'])
        #print(self.airr_rearrangement_map['ir_id'])
        return True


    # Return the value for the row and column keys provided. If it can't be found
    # None is returned. 
    def getMapping(self, field, from_column, to_column):
        # Check to see if we have a valid from_column, if not return None
        if not from_column in self.airr_mappings:
            return None
        # Get the data in the from_column
        from_column_data = self.airr_mappings[from_column]
        # Get a boolean array that is true where we found the field of interest.
        from_boolean = from_column_data.isin([field])
        # And extract all rows that have the from key.
        from_row = self.airr_mappings.loc[from_boolean]
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
            return value.values[0]
        else:
            return None



