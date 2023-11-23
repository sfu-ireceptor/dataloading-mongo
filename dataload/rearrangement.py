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


class Rearrangement(Annotation):

    # Class constructor
    def __init__(self, verbose, repository_tag, repository_chunk, airr_map, repository):
        # Initialize the base class
        Annotation.__init__(self, verbose, repository_tag, repository_chunk,
                            airr_map, repository)
        # We need to keep track of the field (identified by an iReceptor
        # field name) in the rearrangement collection that points to the
        # Repertoire ID field in the repertoire collection. This should exist in
        # each rearrangemnt record. This overrides the Annotation class value for
        # this field.
        self.annotation_linkid_field = "ir_annotation_set_metadata_id_rearrangement"


    # Method to map a dataframe to the repository type mapping.
    def mapToRepositoryTypeOld(self, df):
        # time this function
        t_start = time.perf_counter()

        # Get the general information we need to do the mapping
        airr_type_tag = "airr_type"
        repo_type_tag = "ir_repository_type"
        repository_tag = self.getRepositoryTag()
        map_class = self.airr_map.getRearrangementClass()
        ir_map_class = self.airr_map.getIRRearrangementClass()

        column_types = df.dtypes
        # For each column in the data frame, we want to convert it to the type
        # required by the repository.
        for (column, column_data) in df.items():
            # Get both the AIRR type for the column and the Repository type.
            airr_type = self.airr_map.getMapping(column, repository_tag,
                                                 airr_type_tag, map_class)
            repo_type = self.airr_map.getMapping(column, repository_tag,
                                                 repo_type_tag, map_class)
            # If we can't find it in the AIRR fields, check the IR fields.
            if airr_type is None:
                airr_type = self.airr_map.getMapping(column, repository_tag,
                                                 airr_type_tag, ir_map_class)
                repo_type = self.airr_map.getMapping(column, repository_tag,
                                                 repo_type_tag, ir_map_class)
            # Try to do the conversion
            try:
                # Get the type of the first element of the column 
                oldtype = type(column_data.iloc[0])

                if repo_type == "boolean":
                    # Need to convert to boolean for the repository
                    df[column]= column_data.apply(Parser.to_boolean)
                    if self.verbose():
                        print("Info: Mapped column %s to bool in repository (%s, %s, %s, %s)"%
                              (column, airr_type, repo_type, oldtype,
                               type(df[column].iloc[0])))
                elif repo_type == "integer":
                    # Need to convert to integer for the repository
                    df[column]= column_data.apply(Parser.to_integer)
                    if self.verbose():
                        print("Info: Mapped column %s to int in repository (%s, %s, %s, %s)"%
                              (column, airr_type, repo_type, oldtype,
                               type(df[column].iloc[0])))
                elif repo_type == "number":
                    # Need to convert to number (float) for the repository
                    df[column]= column_data.apply(Parser.to_number)
                    if self.verbose():
                        print("Info: Mapped column %s to number in repository (%s, %s, %s, %s)"%
                              (column, airr_type, repo_type, oldtype,
                               type(df[column].iloc[0])))
                elif repo_type == "string":
                    # Need to convert to string for the repository
                    df[column]= column_data.apply(Parser.to_string)
                    if self.verbose():
                        print("Info: Mapped column %s to string in repository (%s, %s, %s, %s)"%
                              (column, airr_type, repo_type, oldtype,
                               type(df[column].iloc[0])))
                else:
                    # No mapping for the repository, which is OK, we don't make any changes
                    print("Warning: No mapping for type %s storing as is, %s (type = %s)."
                          %(repo_type,column,type(column_data.iloc[0])))
            # Catch any errors
            except TypeError as err:
                print("ERROR: Could not map column %s to repository (%s, %s, %s, %s)"%
                      (column, airr_type, repo_type, oldtype, type(df[column].iloc[0])))
                print("ERROR: %s"%(err))
                return False
            except Exception as err:
                print("ERROR: Could not map column %s to repository (%s, %s)"%
                      (column, airr_type, repo_type))
                print("ERROR: %s"%(err))
                return False

        t_end = time.perf_counter()
        if self.verbose():
            print("Info: Conversion to repository type took %f s"%(t_end - t_start),
                  flush=True)

        return True

    #####################################################################################
    # Hide the repository implementation from the Rearrangement subclasses. These methods
    # are used by all subclasses of the Rearrangement object.
    #####################################################################################

    # Write the set of JSON records provided to the "rearrangements" collection.
    # This is hiding the Mongo implementation. Probably should refactor the 
    # repository implementation completely.
    def repositoryInsertRecords(self, json_records):
        # Insert the JSON and get a list of IDs back. If no data returned, return an error
        record_ids = self.repository.insertRearrangements(json_records)
        if record_ids is None:
            return False
        # Get the field we want to map for the rearrangement ID for each record.
        rearrange_id_field =  self.getAIRRMap().getMapping("rearrangement_id",
                                              self.getiReceptorTag(),
                                              self.getRepositoryTag(),
                                              self.getAIRRMap().getRearrangementClass())
        # If we found a repository record, write a string repersentation of the ID 
        # returned into the rearrangement_id field.
        if not rearrange_id_field is None:
            for record_id in record_ids:
                self.repository.updateRearrangementField("_id", record_id,
                                                         rearrange_id_field, str(record_id))

        return True

    # Count the number of rearrangements that belong to a specific repertoire. Note: In our
    # early implementations, we had an internal field name called ir_project_sample_id. We
    # want to hide this and just talk about reperotire IDs, so this is hidden in the 
    # Rearrangement class...
    def repositoryCountRecords(self, repertoire_id):
        repertoire_field = self.airr_map.getMapping(self.getAnnotationLinkIDField(),
                                                    self.ireceptor_tag,
                                                    self.repository_tag)
        return self.repository.countRearrangements(repertoire_field, repertoire_id)

    # Update the cached sequence count for the given reperotire to be the given count.
    def repositoryUpdateCount(self, repertoire_id, count):
        repertoire_field = self.airr_map.getMapping(self.getRepertoireLinkIDField(),
                                                    self.ireceptor_tag,
                                                    self.repository_tag)
        count_field = self.airr_map.getMapping(self.getRearrangementCountField(),
                                                    self.ireceptor_tag,
                                                    self.repository_tag)
        return self.repository.updateField(repertoire_field, repertoire_id,
                                           count_field, count)

