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
import argparse

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
                 url_api_end_point: str, study_id: str, mapping_file: str, output_directory: str):
        self.metadata_df = metadata_df
        self.repertoire_json = repertoire_json
        self.facet_json = facet_json
        self.annotation_dir = annotation_dir
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

    def validate_repertoire_data_airr(self, validate: bool):

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
        airr.load_repertoire(output_dir + filename, validate)

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
        # Data clean up and accessing repertoire fields only
        map_csv_no_na = map_csv[['ir_adc_api_response', 'ir_curator', 'airr_type', "ir_class"]].fillna("")
        map_csv_repertoire = map_csv_no_na[(map_csv_no_na['ir_class'].str.contains("Repertoire"))]

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

    def print_mapping_results(self, field_names_in_mapping_not_in_api, field_names_in_mapping_not_in_md):
        """

        :param field_names_in_mapping_not_in_api: (list)  list of field names (str) found in mapping not in
                                                    API response
        :param field_names_in_mapping_not_in_md: (list)  list of field names (str) found in mapping not in
                                                    file metadata
        :return: None
        """
        print_separators()
        print("Field names in mapping, ir_adc_api_response, not in API response\n")
        # Print items not found in API, skip those reported as NaN or empty string
        for item in field_names_in_mapping_not_in_api:
            if type(item) == float or item == "":
                continue
            else:
                print(item)

        print_separators()
        print("Field names in mapping, ir_curator, not in metadata fields\n")
        # Print items not found in metadata sheet, skip those reported as NaN or empty string
        for item in field_names_in_mapping_not_in_md:
            if type(item) == float or item == "":
                continue
            else:
                print(item)


def test_connecting_field(connecting_field, data_df, json_study_df):
    """

    :param connecting_field: str with metadata field uniquely identifying repertoires,
                            typically repertoire_id
    :param data_df: dataframe object containing CVS/Excel metadata associated with
                            specific study
    :param json_study_df: dataframe object containing JSON response of metadata associated with
                            specific study
    :return: result (bool) True if repertoire_id is present in both JSON response dataframe and
                            CSV metadata file
    """

    result = True
    if connecting_field not in data_df.columns or "repertoire_id" not in json_study_df.columns:
        print(
            f"Failure, need an ID to compare fields, usually {connecting_field} in metadata file and "
            f"{connecting_field} in ADC API response. "
            "If at least one of these is missing, the test cannot be completed.")
        result = False
        return result
    else:
        return result


def identify_mutual_repertoire_ids_in_data(connecting_field, data_df, json_study_df):
    """

    :param connecting_field: str with metadata field uniquely identifying repertoires,
                            typically repertoire_id
    :param data_df: dataframe object containing CVS/Excel metadata associated with
                            specific study
    :param json_study_df: dataframe object containing JSON response of metadata associated with
                            specific study
    :return: unique_items: list containing repertoire ids found in both JSON response and metadata
                            file

    """
    if test_connecting_field(connecting_field, data_df, json_study_df):
        # Get entries of interest in API response
        repertoire_list = json_study_df["repertoire_id"].to_list()

        # Get corresponding entries in metadata
        sub_data = data_df[data_df[connecting_field].isin(repertoire_list)]
        # Generate list of repertoires
        unique_items = sub_data[connecting_field].to_list()
    else:
        sys.exit(0)

    # Check whether there are no common repertoire_ids in both sources of data
    if len(unique_items) == 0:
        print(
            "WARNING: NON-MATCHING REPERTOIRE IDS - no id's match at ADC API and metadata level. "
            "Test results 'pass' as there is nothing to compare. Verify the repertoire ids in metadata are correct.")

    return unique_items


def metadata_content_testing(unique_items, json_study_df, data_df, connecting_field, mutual_fields):
    """

    :param mutual_fields: list containing fields found in both JSON repertoire response and metadata
                            file
    :param connecting_field: str with metadata field uniquely identifying repertoires,
                            typically repertoire_id
    :param unique_items: list containing repertoire ids found in both JSON response and metadata
                            file
    :param json_study_df: dataframe object containing JSON response of metadata associated with
                            specific study
    :param data_df: dataframe object containing CVS/Excel metadata associated with
                            specific study
    :return:
    """
    print("Content cross comparison\n")

    # Get entries of interest in API response
    repertoire_list = json_study_df["repertoire_id"].to_list()

    # Get corresponding entries in metadata
    sub_data = data_df[data_df[connecting_field].isin(repertoire_list)]

    # Store information
    api_fields = []
    md_fields = []
    api_val = []
    md_val = []
    data_proc_id = []

    # Iterate over each rearrangement_number/repertoire_id
    for item in unique_items:

        # Get the row corresponding to the matching response in API
        row_api = json_study_df[json_study_df[connecting_field] == str(item)]

        row_md = sub_data[sub_data[connecting_field] == item]

        # Content check
        for i in mutual_fields:

            # Get row of interest
            md_entry = row_md[i[1]].to_list()  # [0]
            api_entry = row_api[i[0]].to_list()  # [0]

            # Content is equal or types are equivalent
            try:
                if md_entry == api_entry or api_entry[0] is None and type(md_entry[0]) == float or type(
                        api_entry[0]) == float and type(md_entry[0]) == float:
                    continue

                elif type(md_entry[0]) != type(api_entry[0]) and str(md_entry[0]) == str(api_entry[0]):
                    continue
                # Content mismatch
                else:
                    data_proc_id.append(item)
                    api_fields.append(i[0])
                    md_fields.append(i[1])
                    api_val.append(api_entry)
                    md_val.append(md_entry)

            except:
                print("Cannot compare types")
                print("-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*-*\n")
    # Report and store results
    content_results = pd.DataFrame({"DataProcessingID": data_proc_id,
                                    "API field": api_fields,
                                    "MD field": md_fields,
                                    "API value": api_val,
                                    "MD value": md_val})
    return content_results


def print_content_test_results(content_results, details_dir, study_id):
    """

    :param study_id:
    :param details_dir:
    :param content_results: dataframe object containing content test results
    :return: None
    """
    # Perfect results
    if content_results.empty:
        print("Could not find differing results between column content.")
    # Not so perfect results
    else:
        print("Some fields may require attention:")
        print("In ADC API: ", content_results["API field"].unique())
        print("In metadata: ", content_results["MD field"].unique())
        file_name = "".join([details_dir, str(study_id), "_reported_fields_", str(pd.to_datetime('today')), ".csv"])
        print(f"For details refer to {file_name}")
        content_results.to_csv(file_name)


def print_data_validator():
    # Begin sanity checking
    print("########################################################################################################")
    print("---------------------------------------VERIFY FILES ARE HEALTHY-----------------------------------------\n")
    print("---------------------------------------------Metadata file----------------------------------------------\n")


def print_separators():
    print("--------------------------------------------------------------------------------------------------------")


def get_arguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )

    # Output Directory - where Performance test results will be stored
    parser.add_argument(
        "mapping_file",
        help="Indicate the full path to where the mapping file is found"
    )

    # Array with URL
    parser.add_argument(
        "base_url",
        help="String containing URL to API server  (e.g. https://airr-api2.ireceptor.org)"
    )
    # Entry point
    parser.add_argument(
        "entry_point",
        help="Options: string 'rearrangement' or string 'repertoire'"
    )
    # Full path to directory with JSON file containing repertoire id queries associated to a given study
    parser.add_argument(
        "json_files",
        help="Enter full path to JSON query containing repertoire ID's for a given study - "
             "this must match the value given for study_id"
    )

    # Full path to metadata sheet (CSV or Excel format)
    parser.add_argument(
        "master_md",
        help="Full path to master metadata"
    )

    # Study ID (study_id)
    parser.add_argument(
        "study_id",
        help="Study ID (study_id) associated to this study"
    )

    # Full path to directory with JSON files containing facet count queries associated to each repertoire
    parser.add_argument(
        "facet_count",
        help="Enter full path to JSON queries containing facet count request for each repertoire"
    )

    # Full path to annotation files
    parser.add_argument(
        "annotation_dir",
        help="Enter full path to where annotation files associated with study_id"
    )

    # Full path to directory where output logs will be stored
    parser.add_argument(
        "details_dir",
        help="Enter full path where you'd like to store content feedback in CSV format"
    )

    # Test type
    parser.add_argument(
        "Coverage",
        help="Sanity check levels: enter CC for content comparison, enter FC for facet count vs ir_curator count test, enter AT for AIRR type test"
    )

    # Annotation tool
    parser.add_argument(
        "annotation_tool",
        help="Name of annotation tool used to process sequences. Choice between MiXCR, VQuest, IGBLAST"
    )

    # Verbosity flag
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Run the program in verbose mode.")

    # Parse the command line arguements.
    options = parser.parse_args()
    return options


def main():
    print("DATA PROVENANCE TEST \n")

    # Input reading
    options = get_arguments()
    mapping_file = options.mapping_file
    base_url = options.base_url
    entry_pt = options.entry_point
    json_input = options.json_files
    metadata = options.master_md
    study_id = options.study_id
    facet_json_input = options.facet_count
    annotation_directory = options.annotation_dir
    details_dir = options.details_dir
    cover_test = options.Coverage
    annotation_tool = options.annotation_tool

    # Handle odd study_id
    study_id = study_id.replace('/', '')
    # Set connecting_field
    connecting_field = 'repertoire_id'
    # Build full query
    query_url = base_url + "/airr/v1/" + entry_pt

    # Initialize sanity check
    sanity_check = SanityCheck(metadata_df=metadata, repertoire_json=json_input, facet_json=facet_json_input,
                               annotation_dir=annotation_directory, url_api_end_point=query_url,
                               study_id=study_id, mapping_file=mapping_file, output_directory=details_dir)
    # Generate printed report
    print_data_validator()

    # Read repertoire response from metadata file
    master = sanity_check.identify_file_type()
    data_df = master

    # Report separators
    print_separators()

    # Read repertoire response from API
    concat_version = sanity_check.flatten_json("json_response")
    concat_version['study.study_id'] = concat_version['study.study_id'].replace(" ", "", regex=True)
    json_study_df = concat_version[concat_version['study.study_id'] == study_id]

    # Mapping file test
    [field_names_in_mapping_not_in_api, field_names_in_mapping_not_in_md,
     mutual_fields] = sanity_check.perform_mapping_test(master, concat_version)

    # Print mapping file test results
    sanity_check.print_mapping_results(field_names_in_mapping_not_in_api, field_names_in_mapping_not_in_md)

    # Report separators
    print_separators()

    # Content test
    identify_mutual_repertoire_ids_in_data(connecting_field, data_df, json_study_df)
    # Select repertoire ids
    unique_items = identify_mutual_repertoire_ids_in_data(connecting_field, data_df, json_study_df)
    # Perform content sanity check
    sanity_test_df = metadata_content_testing(unique_items, json_study_df, data_df, connecting_field, mutual_fields)
    # Generate CSV results
    print_content_test_results(sanity_test_df, details_dir, study_id)

    # Report separators
    print_separators()

    # Report AIRR validation
    print("AIRR FIELD VALIDATION")
    sanity_check.validate_repertoire_data_airr(validate=True)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
