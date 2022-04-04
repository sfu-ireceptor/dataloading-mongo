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


class Expression(Annotation):

    # Class constructor
    def __init__(self, verbose, repository_tag, repository_chunk, airr_map, repository):
        # Initialize the base class
        Annotation.__init__(self, verbose, repository_tag, repository_chunk,
                            airr_map, repository)
        # We need to keep track of the field (identified by an iReceptor
        # field name) in the expression collection that points to the
        # Repertoire ID field in the repertoire collection. This should exist in
        # each annotation record. This overrides the Annotation class value for
        # this field.
        self.annotation_linkid_field = "ir_annotation_set_metadata_id_expression"


    #####################################################################################
    # Hide the repository implementation from the Expression subclasses. These methods
    # are used by all subclasses of the Expression object.
    #####################################################################################

    # Write the set of JSON records provided to the "expression" collection.
    # This is hiding the Mongo implementation. Probably should refactor the 
    # repository implementation completely.
    def repositoryInsertRecords(self, json_records):
        # Insert the JSON and get a list of IDs back. If no data returned, return an error
        record_ids = self.repository.insertExpression(json_records)
        if record_ids is None:
            return False
        # Get the field we want to map for the clone ID for each record.
        #gex_id_field =  self.getAIRRMap().getMapping("cell_id",
        #                                      self.getiReceptorTag(),
        #                                      self.getRepositoryTag(),
        #                                      self.getAIRRMap().getExpressionClass())
        # If we found a repository record, write a string repersentation of the ID 
        # returned into the clone_id field.
        #if not cell_id_field is None:
        #    for record_id in record_ids:
        #        self.repository.updateExpressionField("_id", record_id,
        #                                         cell_id_field, str(record_id))

        return True

    # Count the number of expression values that belong to a specific repertoire. We
    # want to hide the count field and just talk about reperotire IDs, so
    # this is hidden in the Expression class...
    def repositoryCountRecords(self, repertoire_id):
        repertoire_field = self.airr_map.getMapping(self.getAnnotationLinkIDField(),
                                                    self.ireceptor_tag,
                                                    self.repository_tag)
        return self.repository.countExpression(repertoire_field, repertoire_id)

    # Update the cached sequence count for the given reperotire to be the given count.
    def repositoryUpdateCount(self, repertoire_id, count):
        repertoire_field = self.airr_map.getMapping(self.getRepertoireLinkIDField(),
                                                    self.ireceptor_tag,
                                                    self.repository_tag)
        count_field = self.airr_map.getMapping(self.getExpressionCountField(),
                                                    self.ireceptor_tag,
                                                    self.repository_tag)
        if count_field is None:
           return False
        else:
            return self.repository.updateField(repertoire_field, repertoire_id,
                                                    count_field, count)

