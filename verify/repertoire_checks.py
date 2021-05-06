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

pd.set_option('display.max_columns', 500)


class SanityCheck:
    """
    This class contains several attributes associated to key pieces of information for
    repertoire "sanity checking", or metadata quality assurance.

    :parameter metadata_df: full path containing the name of AIRR metadata file, accepted formats CSV EXCEL and JSON
    :parameter repertoire_json: full path containing the name of JSON query input for repertoire metadata
    :parameter facet_json: full path containing the name of JSON query input for facet count data
    :parameter annotation_dir: full path containing sequence annotation files
    :parameter repertoire id: unique identifier associated with a repertoire, uniqueness only attained at study level
    :parameter url_api_end_point: base url of AIRR API
    :parameter study_id: identifies study
    """

    def __init__(self, metadata_df: str, repertoire_json: str, facet_json: str, annotation_dir: str,
                 repertoire_id: str, url_api_end_point: str, study_id: str):
        self.metadata_df = metadata_df
        self.repertoire_json = repertoire_json
        self.facet_json = facet_json
        self.annotation_dir = annotation_dir
        self.repertoire_id = repertoire_id
        self.url_api_end_point = url_api_end_point
        self.study_id = study_id

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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    pass
