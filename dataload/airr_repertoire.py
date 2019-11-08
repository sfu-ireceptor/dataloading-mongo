#!/usr/bin/python3

import pandas as pd
import json
import os
from datetime import datetime
from datetime import timezone
from repertoire import Repertoire
import airr

class AIRRRepertoire(Repertoire):
    
    def __init__(self,context):
        self.context = context
        Repertoire.__init__(self, context)
        

    def ir_flatten(self, key, value, dictionary):
        # If it is an integer, float, or bool we just use the key value pair.
        if isinstance(value, (int, float, bool)):
            print("'"+key+"':"+str(value))
            dictionary[key] = value
        # If it is a string we just use the key value pair.
        elif isinstance(value, str):
            print("'"+key+"':'"+value+"'")
            dictionary[key] = value
        elif isinstance(value, dict):
            # We need to handle the AIRR ontology terms. If we get one we want 
            # to use the value of the ontology term in our repository for now.
            if key == "organism" or key == "study_type" or key == "cell_subset":
                print("'"+key+"':'"+value['value']+"'")
                dictionary[key] = value['value']
            else:
                for sub_key, sub_value in value.items():
                    self.ir_flatten(sub_key, sub_value, dictionary)
        # There are currently three possible array situations in the spec. 
        # - keywords_study: This is an array of strings that should be concatenated
        # - diagnosis: We only support one per repertoire. Warn and continue with 1st
        # - pcr_target: We only support one per repertoire. Warn and continue with 1st
        # - data_processing: We only support one per repertoire. Warn and continue with 1st
        elif isinstance(value, list):
            # We flatten this explicitly as a special case.
            if key == "keywords_study":
                print("'"+key+"':'"+str(value)+"'")
                dictionary[key] = value
            else:
                # If we are handling a data processing element list, we have a hint as 
                # to which element is the most important, as we can use the "primary_annotation"
                # field to determine which one to use.
                if key == "data_processing":
                    # Look for the primary annotation
                    if len(value) > 1:
                        print("Warning: Found more than one %s element (found %d)."%(key, len(value)))
                    got_primary = False
                    for element in value:
                        if element['primary_annotation']:
                            # If we found it, flatten it and the break out of the loop
                            for sub_key, sub_value in element.items():
                                self.ir_flatten(sub_key, sub_value, dictionary)
                            got_primary = True
                            print("Warning: Found a primary annotation, using it.")
                            break
                    # If we didn't find the primary, then use the first one as a best guess.
                    if not got_primary:
                        print("Warning: Could not find a primary annotation, using the first one.")
                        for sub_key, sub_value in value[0].items():
                            self.ir_flatten(sub_key, sub_value, dictionary)
                else:
                    # In the general case, iReceptor only supports a single instance in 
                    # array subtypes. If this occurs, we print a warning and use the first
                    # element in the array and ignore the rest. This is a fairly substantial
                    # issue and MAYBE it should be a FATAL ERROR???
                    if len(value) > 1:
                        print("Warning: Found a repertoire list for %s > 1 (%d)."%(key, len(value)))
                        print("Warning: iReceptor only supports a single array, using first instance.")
                    for sub_key, sub_value in value[0].items():
                        self.ir_flatten(sub_key, sub_value, dictionary)
        return dictionary

    def process(self, filename):

        # Check to see if we have a file    
        if not os.path.isfile(filename):
            print("ERROR: input file " + filename + " is not a file")
            return False

        # Check for a valid repertoire file
        #if not airr.validate_repertoire(filename):
        #    print("ERROR: AIRR repertoire validation failed")
        #    return False

        # Load the repertoires
        try:
            data = airr.load_repertoire(filename, validate=True)
        except airr.ValidationError as err:
            print("ERROR: AIRR repertoire validation failed for file %s - %s" % (filename, err))

        data = airr.load_repertoire(filename)
        # The 'Repertoire' contains a dictionary for each repertoire.
        repertoire_list = []
        for repertoire in data['Repertoire']:
            repertoire_dict = dict()
            for key, value in repertoire.items():
                self.ir_flatten(key, value, repertoire_dict)

            repertoire_list.append(repertoire_dict)
                
        #print(repertoire_list)

        print("AIRRRepertoire: Not yet implemented!")
        return False

        # Set the tag for the repository that we are using.
        repository_tag = self.context.repository_tag

        # Extract the fields that are of interest for this file. Essentiall all
        # non null curator fields
        curation_tag = "ir_curator"
        if not curation_tag in self.context.airr_map.airr_repertoire_map:
            print("ERROR: Could not find Curation mapping (" + curation_tag + ") in mapping file")
            return False

        field_of_interest = self.context.airr_map.airr_repertoire_map[curation_tag].notnull()
        
        # We select the rows in the mapping that contain fields of interest for curataion.
        # At this point, file_fields contains N columns that contain our mappings for the
        # the specific formats (e.g. ir_id, airr, vquest). The rows are limited to have
        # only data that is relevant to curataion
        airr_fields = self.context.airr_map.airr_repertoire_map.loc[field_of_interest]
        
        # We need to build the set of fields that the repository can store. We don't
        # want to extract fields that the repository doesn't want.
        curationColumns = []
        columnMapping = {}

        if self.context.verbose:
            print("Info: Dumping AIRR repertoire mapping")
        for index, row in airr_fields.iterrows():
            if self.context.verbose:
                print("    " + str(row[curation_tag]) + " -> " + str(row[repository_tag]))
            # If the repository column has a value for the curator field, track the field
            # from both the curator and repository side.
            if not pd.isnull(row[repository_tag]):
                curationColumns.append(row[curation_tag])
                columnMapping[row[curation_tag]] = row[repository_tag]
            else:
                print("Repository does not map " +
                    str(row[curation_tag]) + ", inserting into repository as is")

                # Read in the CSV file. We need to read this with a utf-8-sig encoding,
        # which means it is a UTF file with a BOM signature. Note that this has
        # been confirmed to work with a Non-UTF ASCII file fine...
        try:
            df = pd.read_csv( filename, sep=None, engine='python', encoding='utf-8-sig' )
        except Exception as err:
            print("ERROR: Unable to open file %s - %s" % (filename, err))
            return False

        # Remove any records that are Unnamed. Note: This occurs when a 
        # Pandas dataframe has a column without a name. In general, this 
        # should not occur and it should probably be detected as an error or
        # at least a warning given.
        if (df.columns.str.contains('^Unnamed').any()):
            print("Warning: column without a title detected in file ", filename)    
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

        # Check the validity of the columns in the actual file being loaded.
        for curation_file_column in df.columns:
            if curation_file_column in columnMapping:
                # If the file column is in the mapping, change the data frame column name
                # to be the column name for the repository.
                mongo_column = columnMapping[curation_file_column]
                if self.context.verbose:
                    print("Info: Mapping input file column " + curation_file_column + " -> " + mongo_column)
                df.rename({curation_file_column:mongo_column}, axis='columns', inplace=True)
            else:
                # If we don't have a mapping, keep the name the same, as we want to
                # still save the data even though we don't have a mapping.
                if self.context.verbose:
                    print("Info: No mapping for input file column " + curation_file_column + ", storing in repository as is")
        # Check to see which desired Curation mappings we don't have... We check this
        # against the "mongo_column" from the repository in the data frame, because
        # we have already mapped the columns from the file columns to the repository columns.
        for curation_column, mongo_column in columnMapping.items():
            if not mongo_column in df.columns:
                if self.context.verbose:
                    print("Warning: Missing data in input file for " + curation_column)

        # Get the mapping for the sequence count field for the repository and 
        # initialize the sequeunce count to 0. If we can't find a mapping for this
        # field then we can't do anything. 
        count_field = self.context.airr_map.getMapping("ir_sequence_count", "ir_id", repository_tag)
        if count_field is None:
            print("Warning: Could not find ir_sequence_count tag in repository " + repository_tag + ", field not initialized")
        else:
            df[count_field] = 0

        # Ensure that we have a correct file name to link fields. If not return.
        # This is a fatal error as we can not link any data to this set of samples,
        # so there is no point adding the samples...
        repository_file_field = self.context.airr_map.getMapping("ir_rearrangement_file_name", "ir_id", repository_tag)
        # If we can't find a mapping for this field in the repository mapping, then
        # we might still be OK if the metadata spreadsheet has the field. If the fails, 
        # then we should exit.
        if repository_file_field is None or len(repository_file_field) == 0:
            print("Warning: Could not find a valid repository mapping for the rearrangement file name (ir_rearrangement_file_name)")
            repository_file_field = "ir_rearrangement_file_name"

        # If we can't find the file field for the rearrangement field in the repository, then
        # abort, as we won't be able to link the repertoire to the rearrangement.
        if not repository_file_field in df.columns:
            print("ERROR: Could not find a rearrangement file field in the metadata (ir_rearrangement_file_name)")
            print("ERROR: Will not be able to link repertoire to rearrangement annotations")
            df["ir_rearrangment_file_name"] = ""
            return False

        # Add a created_at and updated_at field in the repository.
        now_str = Parser.getDateTimeNowUTC()
        df["ir_created_at"] = now_str
        df["ir_updated_at"] = now_str

        # Conver to JSON
        records = json.loads(df.T.to_json()).values()
        record_list = list(records)
        
        # Iterate over the list and load records. Note that this code inserts all data
        # that was read in the CSV file. That is, all of the non MiAIRR fileds that exist
        # are stored in the repository. So if the provided CSV file has lots of extra fields
        # they will exist in the repository.
        for r in record_list:
            self.insertDocument( r )
    
        return True
