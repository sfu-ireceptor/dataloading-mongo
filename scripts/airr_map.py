# Class to implement mappings between fields across AIRR data

import pandas as pd

class AIRRMap:
    def __init__(self, verbose):
        # Set up initial class mappings from the file. These are defined in the MiAIRR Standard.
        # Repertoire metadata spans a number of MiAIRR data class types.
        self.airr_repertoire_classes = ["study", "subject", "diagnosis", "sample",
                                        "cell_processing", "nucleic_acid_processing",
                                        "sequencing_run", "software_processing"]
        # Rearrangement data has only one class in the MiAIRR standard.
        self.airr_rearrangement_classes = ["rearrangement"]
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

        # Write some diagnostics about the file read in
        if self.verbose:
            print("Successfully read in %d mapping columns" % (len(self.airr_mappings.columns)))

        # Get the rows that we care about for MiAIRR repertiores (sample loading)
        # and MiAIRR rearrangments.
        self.airr_rearrangement_map = self.airr_mappings.loc[self.airr_mappings['airr_class'].isin(self.airr_rearrangement_classes)]
        self.airr_repertoire_map = self.airr_mappings.loc[self.airr_mappings['airr_class'].isin(self.airr_repertoire_classes)]
        #print(self.airr_repertoire_map['ir_id'])
        return True




