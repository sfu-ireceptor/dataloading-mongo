# Class to implement mappings between fields across AIRR data

import pandas as pd

class AIRRMap:
    def __init__(self, mapfile):
        # Set up initial class mappings from the file. These are defined in the MiAIRR Standard.
        self.airr_metadata_classes = ["study", "subject", "diagnosis", "sample",
                                      "cell_processing", "nucleic_acid_processing", "sequencing_run", "software_processing"]
        self.airr_rearrangement_classes = ["rearrangement"]
        # Keep track of the mapfile being used.
        self.mapfile = mapfile
        # Read in the mapfile
        self.readMapFile(self.mapfile)

    # Read in a map file given a file name.
    def readMapFile(self, mapfile):
        self.mapfile = mapfile
        try:
            self.airr_mappings = pd.read_csv(self.mapfile, sep='\t')
        except:
            print("Error: Could not load AIRR Map file %s" % self.mapfile)
            return False 
        self.airr_rearrangement_map = self.airr_mappings.loc[self.airr_mappings['airr_class'].isin(self.airr_rearrangement_classes)]
        self.airr_metadata_map = self.airr_mappings.loc[self.airr_mappings['airr_class'].isin(self.airr_metadata_classes)]
        #print(self.airr_rearrangement_map)
        #print(self.airr_metadata_map)
        return True




