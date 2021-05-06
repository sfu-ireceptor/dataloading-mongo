#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  8 22:58:48 2021

@author: laura.gf
"""

import curlairripa  # https://test.pypi.org/project/curlairripa/
import time  # time stamps
import pandas as pd
import sys
from xlrd import open_workbook, XLRDError
import json
import requests
import airr

pd.set_option('display.max_columns', 500)


class SanityCheck:
    """
    This class contains several attributes associated to key pieces of information for
    repertoire "sanity checking", or metadata quality assurance.

    :parameter metadata_df: full path containing the name of AIRR metadata file, accepted formats CSV EXCEL and JSON
    :parameter repertoire_json: full path containing the name of JSON query input for repertoire metadata
    :parameter facet_json: full path containing the name of JSON query input for facet count data
    :parameter annotation_dir: full path containing sequence annotation files
    :parameter repertoire_id: unique identifier associated with a repertoire, uniqueness only attained at study level
    :parameter url_api_end_point: base url of AIRR API
    :parameter study_id: identifies study
    :parameter mapping_file: full path to CSV file containing mapping
    :parameter output_directory: full path to directory where sanity check results will be stored
    """

    def __init__(self, metadata_df: str, repertoire_json: str, facet_json: str, annotation_dir: str,
                 repertoire_id: str, url_api_end_point: str, study_id: str, mapping_file: str, output_directory: str):
        self.metadata_df = metadata_df
        self.repertoire_json = repertoire_json
        self.facet_json = facet_json
        self.annotation_dir = annotation_dir
        self.repertoire_id = repertoire_id
        self.url_api_end_point = url_api_end_point
        self.study_id = study_id
        self.mapping_file = mapping_file
        self.output_directory = output_directory

    def test_book(self):

        """This function verifies whether it is possible to open a metadata EXCEL file.

        It returns True if yes, False otherwise"""

        filename = self.metadata_df

        try:
            open_workbook(filename)
            print("HEALTHY FILE: Proceed with tests\n")
        except XLRDError:
            print("CORRUPT FILE: Please verify master metadata file\n")
            print("INVALID INPUT\nInput is an EXCEL metadata file.")
            sys.exit()

    # Get appropriate metadata sheet
    def get_metadata_sheet(self):

        """This function extracts the 'metadata' sheet from an EXCEL metadata file """

        # Tabulate Excel file
        master_metadata_file = self.metadata_df
        table = pd.ExcelFile(master_metadata_file)  # ,encoding="utf8")
        # Identify sheet names in the file and store in array
        sheets = table.sheet_names
        # How many sheets does it have
        number_sheets = len(sheets)

        # Select metadata spreadsheet
        metadata_sheet = ""
        for i in range(number_sheets):
            # Check which one contains the word metadata in the title and hold on to it
            if "Metadata" == sheets[i] or "metadata" == sheets[i]:
                metadata_sheet = metadata_sheet + sheets[i]
                break

        # This is the sheet we want
        metadata = table.parse(metadata_sheet)

        return metadata

    def get_study_entries(self):
        """
        Parameters
        ----------

        Returns
        -------
        sub_data_df : dataframe
            subset of the data containing data for a particular study.

        """
        # Get metadata sheet
        master_sheet = self.get_metadata_sheet()
        # Provide study id
        study_id = self.study_id

        # Get metadata and specific study
        master = master_sheet.loc[:, master_sheet.columns.notnull()]
        master = master.replace('\n', ' ', regex=True)
        # for master metadata only
        # grab the first row for the header
        new_header = master.iloc[1]
        # take the data less the header row
        master = master[2:]
        # set the header row as the df header
        master.columns = new_header
        # if "study_id" in master.columns and master['study_id'].isnull().sum()<1:
        if "study_id" in master.columns:
            master["study_id"] = master["study_id"].str.strip()
            master['study_id'] = master['study_id'].replace(" ", "", regex=True)
            data_df = master.loc[master['study_id'] == study_id]
        else:
            print("INVALID STUDY SHEET. Please ensure master metadata file contains filed with study_id")
            sys.exit(0)

        return data_df

    def flatten_json(self, flag: str):
        """
        This function takes metadata in JSON format. Data is flattened and object of type dataframe is returned
        """

        def rename_cols(flattened_sub_df, field_name):
            """

            :param flattened_sub_df: JSON response turned into object of type dataframe
            :param field_name: field name to be renamed
            :return: object of type dataframe with renamed columns
            """
            # Access original columns
            flattened_cols = flattened_sub_df.columns
            # Rename columns
            new_col_names = {item: str(field_name) + ".0." + str(item) for item in flattened_cols}
            # Apply new names
            flattened_sub_df = flattened_sub_df.rename(columns=new_col_names)

            return flattened_sub_df

        # Option to flatten repertoire metadata in JSON format or repertoire JSON response from API
        if flag == "metadata":
            json_data = self.metadata_df
        elif flag == "json_response":
            json_data = self.execute_query("repertoire")
        else:
            print("INVALID FLAG: pick one of 'metadata' or 'json_response'")
            sys.exit(0)

        # Begin flattening
        try:
            # Level: repertoire
            repertoire = pd.json_normalize(data=json_data['Repertoire'])

        except KeyError:
            print('No Repertoire field found in JSON metadata')
            sys.exit(0)

        try:

            # Level: data processing under repertoire
            data_pro = pd.json_normalize(data=json_data['Repertoire'], record_path='data_processing')
            data_pro = rename_cols(data_pro, "data_processing")

        except KeyError:
            print('No data_processing field found in JSON metadata')
            sys.exit(0)

        try:
            # Level: sample under repertoire
            sample = pd.json_normalize(data=json_data['Repertoire'], record_path='sample')
            sample = rename_cols(sample, "sample")

        except KeyError:
            print('No sample field found in JSON metadata')
            sys.exit(0)

        try:
            # Level pcr_target under sample, under repertoire
            pcr_target = pd.json_normalize(json_data["Repertoire"], record_path=['sample', 'pcr_target'])
            pcr_target = rename_cols(pcr_target, "sample.0.pcr_target")

        except KeyError:
            print('No pcr_target or sample fields found in JSON metadata')
            sys.exit(0)

        try:
            # Level: diagnosis under subject, under repertoire
            subject = pd.json_normalize(data=json_data['Repertoire'], record_path=["subject", "diagnosis"])
            subject = rename_cols(subject, "subject.diagnosis")
        except KeyError:
            print('No diagnosis or subject field found in JSON metadata')
            sys.exit(0)

        # Concatenate
        concat_version = pd.concat([repertoire, data_pro, sample,
                                    pcr_target, subject], 1).drop(["data_processing", "sample",
                                                                   'sample.0.pcr_target'], 1)
        return concat_version

    def identify_file_type(self):
        """
        Determine whether metadata file is xlsx, csv or json

        Function works with metadata_df attribute of the SanityCheck class and returns as output
        an object of type pandas dataframe containing repertoire metadata

        """

        # Access metadata attribute
        metadata = self.metadata_df

        try:
            # Metadata is of type Excel
            if "xlsx" in metadata:
                self.test_book()
                master = self.get_study_entries()
            # Metadata is of type CSV
            elif "csv" in metadata:
                master = pd.read_csv(metadata, encoding='utf-8')
                master = master.loc[:, ~master.columns.str.contains('^Unnamed')]

            # Metadata is of type TSV
            elif "tsv" in metadata:
                master = pd.read_csv(metadata, encoding='utf8', sep="\t")

            # Metadata if of type JSON
            elif "json" in metadata:
                florian_json = requests.get(metadata)
                florian_json = florian_json.json()
                master = self.flatten_json(florian_json)
            else:
                print("File format provided is not valid")
                sys.exit(0)

            # Check if file is empty
            if master.empty:
                print("EMPTY DATA FRAME: Cannot find specified study ID\n")
                print(master)
                sys.exit(0)

            return master

        except:
            print("Warning: Provided wrong type file: cannot read metadata.")
            sys.exit(0)

    def execute_query(self, flag: str) -> object:
        """
        :return: parsed_query: (JSON object with response)
        """
        # Query parameters
        expect_pass = True
        verbose = False
        force = True

        # Ensure our HTTP set up has been done.
        curlairripa.initHTTP()
        # Get the HTTP header information (in the form of a dictionary)
        header_dict = curlairripa.getHeaderDict()

        # Query files
        if flag == "repertoire":
            query_files = self.repertoire_json
        elif flag == "facet":
            query_files = self.facet_json
        else:
            print("INVALID FLAG: provide one of 'repertoire' or 'facet'")
            sys.exit(0)

        # End point
        query_url = self.url_api_end_point

        # Test query is well built, then perform query
        try:

            # Process json file into JSON structure readable by Python
            query_dict = curlairripa.process_json_files(force, verbose, query_files)

            # Perform the query. Time it
            start_time = time.time()
            query_json = curlairripa.processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)
            total_time = time.time() - start_time

            # Parse
            parsed_query = json.loads(query_json)

            # Time
            print("ELAPSED DOWNLOAD TIME (in seconds): %s" % total_time)
            print("------------------------------------------------------")

            return parsed_query

        except:
            print("Error in URL - cannot complete query. Ensure the input provided points to an API")

    def validate_repertoire_data_airr(self):

        # Initialize variables
        json_input = self.repertoire_json
        study_id = self.study_id
        output_dir = self.output_directory

        # Construct file name
        query_name = str(json_input.split("/")[-1].split(".")[0])
        filename = "_".join([query_name, str(study_id), "OUT.json"])

        # Get JSON response from API, repertoire metadata
        rep_json = self.execute_query("repertoire")
        # Dump into JSON file
        with open(output_dir + filename, "w") as outfile:
            json.dump(rep_json, outfile)
        outfile.close()
        # Perform AIRR validation test
        airr.load_repertoire("test.json", validate=True)

    def perform_mapping_test(self, repertoire_metadata_df, repertoire_response_df):
        """

        :param repertoire_metadata_df: dataframe containing clean repertoire metadata (from sheet)
        :param repertoire_response_df: dataframe containing clean repertoire metadata (from API)
        :return: list of lists reporting fields in mapping not in API response, fields in mapping not in
            metadata sheet, fields in both 
        """

        # Initialize file
        mapping_file = self.mapping_file

        # Read and subset data
        map_csv = pd.read_csv(mapping_file, sep="\t", encoding="utf8", engine='python', error_bad_lines=False)
        map_csv_repertoire = map_csv[['ir_adc_api_response', 'ir_curator']].iloc[:103]

        # Subset series into lists for each of the mappings
        ir_adc_fields = map_csv_repertoire["ir_adc_api_response"].tolist()
        ir_cur_fields = map_csv_repertoire["ir_curator"].tolist()

        # Initialize result list
        field_names_in_mapping_not_in_api = []
        field_names_in_mapping_not_in_md = []
        in_both = []

        # For each field in ADC API response and ir_curator
        for f1, f2 in zip(ir_adc_fields, ir_cur_fields):
            # Check if ADC API mapping field is not in ADC API response
            if f1 not in repertoire_response_df.columns:
                field_names_in_mapping_not_in_api.append(f1)
            # Check if corresponding ir_curator field is not in repertoire metadata
            if f2 not in repertoire_metadata_df.columns:
                field_names_in_mapping_not_in_md.append(f2)
            # Trace when both conditions are satisfied
            if f1 in repertoire_response_df.columns and f2 in repertoire_metadata_df.columns:
                in_both.append([f1, f2])

        return [field_names_in_mapping_not_in_api, field_names_in_mapping_not_in_md, in_both]


def print_data_validator():
    # Begin sanity checking
    print("########################################################################################################")
    print("---------------------------------------VERIFY FILES ARE HEALTHY-----------------------------------------\n")
    print("---------------------------------------------Metadata file----------------------------------------------\n")


def print_separators():
    print("--------------------------------------------------------------------------------------------------------")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    pass
