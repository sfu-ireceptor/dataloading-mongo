# Parent Parser class of data file type specific AIRR data parsers
# Extracted common code patterns shared across various parsers.

from os.path import join
from datetime import datetime
from datetime import timezone
import re
import os
import time
import pandas as pd
import numpy as np
from annotation import Annotation


class Receptor(Annotation):

    # Class constructor
    def __init__(self, verbose, repository_tag, repository_chunk, airr_map, repository):
        # Initialize the base class
        Annotation.__init__(self, verbose, repository_tag, repository_chunk,
                            airr_map, repository)
        # We need to keep track of the field (identified by an iReceptor
        # field name) in the rearrangement collection that points to the
        # Repertoire ID field in the repertoire collection. This should exist in
        # each annotation record. This overrides the Annotation class value for
        # this field.
        self.annotation_linkid_field = "ir_annotation_set_metadata_id_receptor"


    #####################################################################################
    # Hide the repository implementation from the Receptor subclasses. These methods
    # are used by all subclasses of the Receptor object.
    #####################################################################################

    # Write the set of JSON records provided to the "receptor" collection.
    # This is hiding the Mongo implementation. Probably should refactor the 
    # repository implementation completely.
    def repositoryInsertRecords(self, json_records):
        # Insert the JSON and get a list of IDs back. If no data returned, return an error
        record_ids = self.repository.insertReceptors(json_records)
        if record_ids is None:
            return False
        # Get the field we want to map for the receptor ID for each record.
        receptor_id_field =  self.getAIRRMap().getMapping("receptor_id_receptor",
                                              self.getiReceptorTag(),
                                              self.getRepositoryTag(),
                                              self.getAIRRMap().getReceptorClass())
        # If we found a repository record, write a string repersentation of the ID 
        # returned into the receptor_id field.
        if not receptor_id_field is None:
            # Check to make sure we have a list if a single instance is returned.
            if not isinstance(record_ids, list):
                record_ids = [ record_ids ]
            # Over write the field.
            for record_id in record_ids:
                self.repository.updateReceptorField("_id", record_id,
                                                    receptor_id_field, str(record_id))

        return True

    # Count the number of receptors that belong to a specific repertoire. We
    # want to hide the count field and just talk about reperotire IDs, so
    # this is hidden in the Receptor class...
    def repositoryCountRecords(self, repertoire_id):
        repertoire_field = self.airr_map.getMapping(self.getAnnotationLinkIDField(),
                                                    self.ireceptor_tag,
                                                    self.repository_tag)
        return self.repository.countReceptors(repertoire_field, repertoire_id)

    # Update the cached sequence count for the given reperotire to be the given count.
    def repositoryUpdateCount(self, repertoire_id, count):
        repertoire_field = self.airr_map.getMapping(self.getRepertoireLinkIDField(),
                                                    self.ireceptor_tag,
                                                    self.repository_tag)
        count_field = self.airr_map.getMapping(self.getReceptorCountField(),
                                                    self.ireceptor_tag,
                                                    self.repository_tag)
        if count_field is None:
           return False
        else:
            return self.repository.updateField(repertoire_field, repertoire_id,
                                                    count_field, count)

