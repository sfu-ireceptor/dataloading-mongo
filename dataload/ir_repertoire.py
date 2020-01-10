
import pandas as pd
import numpy as np 
import json
import os
from datetime import datetime
from datetime import timezone
from repertoire import Repertoire
from parser import Parser

class IRRepertoire(Repertoire):
    
    def __init__(self, verbose, repository_tag, repository_chunk, airr_map, repository):
        Repertoire.__init__(self, verbose, repository_tag, repository_chunk, airr_map, repository)
        
    def process(self, filename):
        # Check to see if we have a file    
        if not os.path.isfile(filename):
            print("ERROR: input file " + filename + " is not a file")
            return False

        # Set the tag for the iReceptor column
        ireceptor_tag =self.getiReceptorTag() 

        # Check to see if we have the columns needed in the mapping, if not exit.
        if not self.getAIRRMap().hasColumn(ireceptor_tag):
            print("ERROR: Could not find iReceptor mapping (%s) in mapping file"%
                  (ireceptor_tag))
            return False

        # Set the tag for the repository that we are using.
        repository_tag = self.getRepositoryTag()

        # Check to see if we have the columns needed in the mapping, if not exit.
        if not self.getAIRRMap().hasColumn(repository_tag):
            print("ERROR: Could not find Repository mapping (%s) in mapping file"%
                  (repository_tag))
            return False

        # Set the tag for the Curator column
        curation_tag = "ir_curator"
        # Check to see if we have the columns needed in the mapping, if not exit.
        if not self.getAIRRMap().hasColumn(curation_tag):
            print("ERROR: Could not find Curation mapping (%s) in mapping file"%
                  (curation_tag))
            return False

        # Set the tag for the AIRR column
        airr_tag = "airr"
        # Check to see if we have the columns needed in the mapping, if not exit.
        if not self.getAIRRMap().hasColumn(airr_tag):
            print("ERROR: Could not find AIRR mapping (%s) in mapping file"%
                  (airr_tag))
            return False

        # Set the tag for the AIRR type column
        airr_type_tag = "airr_type"
        # Check to see if we have the columns needed in the mapping, if not exit.
        if not self.getAIRRMap().hasColumn(airr_type_tag):
            print("ERROR: Could not find AIRR type mapping (%s) in mapping file"%
                  (airr_type_tag))
            return False

        # Set the tag for the Repisotory type column
        repository_type_tag = "ir_repository_type"
        # Check to see if we have the columns needed in the mapping, if not exit.
        if not self.getAIRRMap().hasColumn(repository_type_tag):
            print("ERROR: Could not find Repository type mapping (%s) in mapping file"%
                  (repository_type_tag))
            return False

        # Get the fields to use for finding repertoire IDs, either using those IDs
        # directly or by looking for a repertoire ID based on a rearrangement file
        # name.
        repertoire_id_field = self.getRepertoireLinkIDField()
        rearrangement_file_field = self.getRearrangementFileField()
        rearrangement_count_field = self.getRearrangementCountField()

        # Get the column of values from the AIRR tag. We only want the
        # Repertoire related fields.
        map_column = self.getAIRRMap().getRepertoireMapColumn(airr_tag)
        # Get a boolean column that flags columns of interest. Exclude nulls.
        fields_of_interest = map_column.notnull()
        # Afer the following, airr_fields contains N columns (e.g. iReceptor, AIRR, VQuest) 
        # that contain the AIRR Repertoire mappings. 
        airr_fields = self.getAIRRMap().getRepertoireRows(fields_of_interest)

        # Get the fields (as above) for the iReceptor Curator mapping.
        map_column = self.getAIRRMap().getIRRepertoireMapColumn(curation_tag)
        fields_of_interest = map_column.notnull()
        curator_fields = self.getAIRRMap().getIRRepertoireRows(fields_of_interest)

        # If we are in verbose mode, dump out the AIRR mapping.
        if self.verbose():
            print("Info: Dumping AIRR repertoire mapping")
            for index, row in airr_fields.iterrows():
                print("Info:    %s -> %s"%
                      (str(row[airr_tag]), str(row[repository_tag])))
        
        # We need to build a column mapping for the curation columns to the fields
        # that the repository will store.
        columnMapping = dict()
        curationType = dict()
        repositoryType = dict()
        type_dict = dict()
        na_dict = dict()
        if self.verbose():
            print("Info: Building field and type mappings (curator -> repository)")
        for index, row in curator_fields.iterrows():
            curation_field = row[curation_tag]
            # Get the types of the fields, both the AIRR type and the repository type
            curation_field_type = self.getAIRRMap().getMapping(curation_field,
                                           curation_tag, airr_type_tag,
                                           self.getAIRRMap().getIRRepertoireClass())
            repository_field_type = self.getAIRRMap().getMapping(curation_field,
                                           curation_tag, repository_type_tag,
                                           self.getAIRRMap().getIRRepertoireClass())

            # For each column, get a type value and if it is integer or string set the
            # type in the type dictionary. We use this later when we read the CSV file
            # to make sure our column types are correct. NOTE: Int64 is the important
            # type here, as it handles integer NaN values correctly. If an integer
            # column is not Int64 then whenever it has a Nan in it it will be cast
            # to a float column, which is bad...
            # See:
            #   - https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html
            #   - https://pandas.pydata.org/pandas-docs/stable/user_guide/integer_na.html
            if curation_field_type == "integer":
                type_dict[curation_field] = "Int64"
            elif curation_field_type == "string":
                #print("%s %s"%(curation_field, curation_field_type))
                type_dict[curation_field] = str
                #na_dict[curation_field] = None
            elif curation_field_type == "number":
                type_dict[curation_field] = float
            elif curation_field_type == "boolean":
                type_dict[curation_field] = bool

            # If the repository column has a value for the curator field, track the field
            # from both the curator and repository side.
            if not pd.isnull(row[repository_tag]):
                columnMapping[curation_field] = row[repository_tag]
                curationType[curation_field] = curation_field_type
                repositoryType[curation_field] = repository_field_type
                if self.verbose():
                    print("Info:    %s (%s)-> %s (%s)"%
                          (str(curation_field), curationType[curation_field],
                           str(row[repository_tag]), repositoryType[curation_field]))
            else:
                print("Warning: No Curation mapping for field %s"%(str(curation_field)))

        # Read in the CSV file. We need to read this with a utf-8-sig encoding,
        # which means it is a UTF file with a BOM signature. Note that this has
        # been confirmed to work with a Non-UTF ASCII file fine... Use the 
        # type_dict dictionary to enforce the type of each column. This will
        # cause Pandas to complain if the typing is wrong.
        try:
            df = pd.read_csv(filename, sep=None, engine='python',
                             encoding='utf-8-sig')
                             #encoding='utf-8-sig',
                             #dtype=type_dict)
                             #dtype=type_dict, na_values=na_dict)
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

        # Check to make sure all of the curator_fields are present in the file read in.
        # If not, this is an error.
        for index, row in curator_fields.iterrows():
            if not row[curation_tag] in df.columns:
                print("ERROR: Could not find curation field %s in file %s"%
                      (row[curation_tag], filename))
                return False

        # Check to make sure all AIRR required columns exist
        for index, row in airr_fields.iterrows():
            # If the row is required by the AIRR standard
            if row["airr_required"] == "TRUE":
                repository_field = row[repository_tag]
                # If the repository representation of the AIRR column is not
                # in the data we are going to write to the repository, then
                # we have an error.
                if not repository_field in df.columns:
                    print("ERROR: Required AIRR field %s (%s) missing"%
                          (row["airr"],repository_field))
                    return False

        # Check the type of the columns in the actual file being loaded. Note that
        # it is sufficient to test the type of the column by its first value because 
        # Pandas data frames are stongly typed by column. So if the first value in
        # the column is correct, then all values in the column are correct.
        # This should typically not be a problem as we force the types of the columns
        # to be the correct type when loading the CSV file.
        # Note that we are only warning at this point... we have the types enforced
        # on JSON write so for now we keep track and warn about the issues only here.
        bad_columns = []
        for (curation_file_column, column_data) in df.iteritems():
            # For each column, check the value of the data against the type expected
            field_type = self.getAIRRMap().getMapping(curation_file_column,
                                           airr_tag, airr_type_tag,
                                           self.getAIRRMap().getIRRepertoireClass())
            # Skip ontology fields and array fields for now.
            if not field_type in ["ontology", "array"]:
                value = column_data[0]
                if not self.validAIRRFieldType(curation_file_column, value, False):
                    if self.verbose():
                        print("Info: Found type mismatch in column %s (%s, %s, %s)"%
                              (curation_file_column, field_type, str(value), type(value)))
                    bad_columns.append(curation_file_column)

        # This probably shouldn't occur, given we force the types at data load.
        for column in bad_columns:
            # Get the field type
            field_type = self.getAIRRMap().getMapping(column, "airr", "airr_type",
                                           self.getAIRRMap().getRepertoireClass())
            if field_type == "string":
                if self.verbose():
                    print("Info: Trying to force column type to string for %s"%(column))
                df[column] = df[column].apply(str)
                value = df.at[0, column]
                if not self.validAIRRFieldType(column, value, False):
                    print("ERROR: Unable to force column type to string for %s"%(column))
                    return False
                if self.verbose():
                    print("Info: Succesfully forced column type to string for %s"%(column))
            else:
                value = df.at[0, column]
                print("Warning: Unable to force column type for %s from %s to %s"%
                      (column, type(value), field_type))
            
        # Change the name of the columns to reflect the repository's naming rather
        # than the name in the input file.
        for (curation_file_column, column_data) in df.iteritems():
            if curation_file_column in columnMapping:
                repository_column = columnMapping[curation_file_column]
                if self.verbose():
                    print("Info: Mapping input file column %s -> %s" %
                          (curation_file_column, repository_column))
                df.rename({curation_file_column:repository_column},
                          axis='columns', inplace=True)
            else:
                # If we don't have a mapping, keep the name the same, as we want to
                # still save the data even though we don't have a mapping.
                if self.verbose():
                    print("Info: No mapping for file column %s, storing in repository as is"
                          %(curation_file_column))

        # Get the mapping for the sequence count field for the repository and 
        # initialize the sequeunce count to 0. If we can't find a mapping for this
        # field then we can't do anything. 
        count_field = self.getAIRRMap().getMapping(rearrangement_count_field,
                                                   ireceptor_tag,
                                                   repository_tag)
        if count_field is None:
            print("Warning: Could not find %s field in repository, not initialized"
                  %(rearrangement_count_field, repository_tag))
        else:
            df[count_field] = 0

        # Ensure that we have a correct file name to link fields. If not return.
        # This is a fatal error as we can not link any data to this set of samples,
        # so there is no point adding the samples...
        repository_file_field = self.getAIRRMap().getMapping(rearrangement_file_field,
                                                             ireceptor_tag,
                                                             repository_tag)
        # If we can't find a mapping for this field in the repository mapping, then
        # we might still be OK if the metadata spreadsheet has the field. If the fails, 
        # then we should exit.
        if repository_file_field is None or len(repository_file_field) == 0:
            print("Warning: No repository mapping for the rearrangement file field (%s)"
                  %(rearrangement_file_field))
            repository_file_field = rearrangement_file_field

        # If we can't find the file field for the rearrangement field in the repository
        # abort, as we won't be able to link the repertoire to the rearrangement.
        if not repository_file_field in df.columns:
            print("ERROR: Could not find a rearrangement file field in the metadata (%s)"
                  %(rearrangement_file_field))
            print("ERROR: Will not be able to link repertoire to rearrangement annotations")
            return False

        # Add a created_at and updated_at field in the repository.
        now_str = Parser.getDateTimeNowUTC()
        df["ir_created_at"] = now_str
        df["ir_updated_at"] = now_str

        # Check to make sure all of our columns are unique.
        if len(df.columns) != len(df.columns.unique()):
            print("ERROR: Duplicate column name in data to be written")
            for column in df.columns:
                count = list(df.columns.values).count(column)
                if count > 1:
                    print("ERROR: found %d occurences of column %s"%(count, column))
            return False
        
        # Conver to JSON
        records = json.loads(df.T.to_json()).values()
        record_list = list(records)
        
        # Iterate over the list and load records. Note that this code inserts all data
        # that was read in the CSV file. That is, all of the non MiAIRR fileds that exist
        # are stored in the repository. So if the provided CSV file has lots of extra fields
        # they will exist in the repository.
        rep_class = self.getAIRRMap().getIRRepertoireClass()
        for r in record_list:
            # Create a temporary dict() for the converted record
            converted_record = dict()
            # Traverse all of the fields in the record and do a type check and converion
            # to use the repository type as required. We use the repository_tag as the
            # field to look things up in because we have already converted the field names
            # to the repository field names (we did that as a pandas data frame conversion.
            for key, value in r.items():
                # Catch type errors. The method throws errors for things it thinks are not
                # recoverable and should cause the record to not be written...
                try:
                    rep_value = self.valueToRepository(key, repository_tag, value, rep_class)
                except TypeError as error:
                    print("ERROR: %s"%(error))
                    return False
                # If the conversion worked for this key, store the converted value
                converted_record[key] = rep_value

            # Write it to the repository, return on failure (-1).
            if not self.repositoryInsertRepertoire(converted_record) > 0:
                return False
    
        # If we got here, we are DONE!
        return True
