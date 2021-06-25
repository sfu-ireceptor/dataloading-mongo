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
import subprocess
import tarfile
import math
import os

pd.set_option('display.max_columns', 500)


class SanityCheck:
    """
    This class contains several attributes associated to key pieces of information for
    repertoire "sanity checking", or metadata quality assurance.

    :parameter metadata_df: full path containing the name of AIRR metadata file, accepted formats CSV EXCEL and JSON
    :parameter repertoire_json: full path containing the name of JSON query input for repertoire metadata
    :parameter facet_json: full path containing the name of JSON query input for facet count data
    :parameter annotation_dir: full path containing sequence annotation files
    :parameter url_api_end_point: base url of AIRR API
    :parameter study_id: identifies study
    :parameter mapping_file: full path to CSV file containing mapping
    :parameter output_directory: full path to directory where sanity check results will be stored
    """

    def __init__(self, metadata_df, repertoire_json, facet_json, annotation_dir,
                 url_api_end_point, study_id, mapping_file, output_directory, url_facet_query):
        self.metadata_df = metadata_df
        self.repertoire_json = repertoire_json
        self.facet_json = facet_json
        self.annotation_dir = annotation_dir
        self.url_api_end_point = url_api_end_point
        self.study_id = study_id
        self.mapping_file = mapping_file
        self.output_directory = output_directory
        self.url_facet_query = url_facet_query

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

    def flatten_json(self, flag):
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
            json_data = self.execute_query("repertoire", "")
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

    def execute_query(self, flag, repertoire_id):
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
            # End point
            query_url = self.url_api_end_point
        elif flag == "facet":
            query_files = f"{self.facet_json}{self.study_id}/facet_repertoire_id_{repertoire_id}.json"
            query_url = self.url_facet_query
        else:
            print("INVALID FLAG: provide one of 'repertoire' or 'facet'")
            sys.exit(0)

        

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

    def test_facet_query(self, repertoire_id):
        """
        This function performs facet query (determines number of facets associated with repertoire)
        and returns count.

        :param repertoire_id: (str) id uniquely identify repertoire
        :return: list containing:
            ir_seq_api (str) contains facet count returned by API response
            fac_count (dataframe object) contains flattened JSON response containing facet count

            0 is returned if the query is empty

        """
        # Perform the query.
        json_data = self.execute_query("facet", repertoire_id)

        # Check if query is empty
        if pd.json_normalize(json_data["Facet"]).empty == True:
            ir_seq_api = "NINAPI"
            fac_count = pd.DataFrame({"repertoire_id": [0]})
        else:
            fac_count = pd.json_normalize(json_data["Facet"])
            ir_seq_api = str(fac_count['count'][0])

        return [ir_seq_api, fac_count]

    def validate_repertoire_data_airr(self, validate):
        """
        This function validates schema is airr compliant
        :param validate: either boolean True or False, if True perform airr validation, False otherwise
        :return:
        """
        # Initialize variables
        json_input = self.repertoire_json
        study_id = self.study_id
        output_dir = self.output_directory

        # Construct file name
        query_name = str(json_input.split("/")[-1].split(".")[0])
        filename = "_".join([query_name, str(study_id), "OUT.json"])

        # Get JSON response from API, repertoire metadata
        rep_json = self.execute_query("repertoire", "")
        # Dump into JSON file
        with open(output_dir + filename, "w") as outfile:
            json.dump(rep_json, outfile)
        outfile.close()
        # Perform AIRR validation test
        airr.load_repertoire(output_dir + filename, validate)

    def perform_mapping_test(self, repertoire_metadata_df, repertoire_response_df):
        """
        This function performs mapping test: compare fields in metadata against
        mapping file field names, flag missing fields. Compare fields in API response
        against mapping file field names. Flag missing names in metadata or API

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
        This function prints missing fields in either metadata file or API response.
        See function perform_mapping_test for details of test.

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

    def annotation_count(self, data_df, repertoire_id, test_type_key):
        # Initialize
        connecting_field = 'repertoire_id'
        annotation_dir = self.annotation_dir
        # Get file names
        line_one = get_data_processing_files(data_df)
        # Access repertoire_id
        ir_rea = data_df[connecting_field].tolist()[0]
        # Access curator count
        ir_sec = data_df["ir_curator_count"].tolist()[0]
        # List files in annotation directory
        files = os.listdir(annotation_dir)

        # Check to verify file ending is found according to file type
        check_file_end = test_file_type_keyword(test_type_key, data_df)

        # Perform annotation count
        [files_found, files_notfound, sum_all] = run_count(check_file_end, line_one, files, test_type_key, annotation_dir)

        # Run facet count test
        [ir_seq_api, fac_count] = self.test_facet_query(repertoire_id)
        # Run ir_curator test
        [message_mdf, ir_sec] = test_ir_curator(data_df)
        # Asses results
        test_result = assess_test_results(ir_seq_api, sum_all, ir_sec, ir_rea)

        # Store results
        result_suite = pd.DataFrame.from_dict({"MetadataFileNames": [line_one],
                                               "FilesFound": [files_found],
                                               "FilesNotFound": [files_notfound],
                                               "MessageMDF": [message_mdf],
                                               "RepertoireID(MD)": [ir_rea],
                                               "RepertoireID(JSON)": [fac_count['repertoire_id'][0]],
                                               "FacetCountAPI": [ir_seq_api],
                                               "ir_curator": [ir_sec],
                                               "NoLinesAnnotation": [sum_all],
                                               "TestResult": [test_result]})

        return result_suite


def test_connecting_field(connecting_field, data_df, json_study_df):
    """
    This function ensures there is a common field to uniquely identify matching
    repertoires in metadata sheet and API response.

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

    :param study_id: str ID identifying a study
    :param details_dir: str full path to directory where sanity check result output will be saved
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


# Annotation file tests
def test_ir_curator(data_df):
    """

    :param data_df: (dataframe object) contains row within metadata with selected repertoire
    :return: list with items
        message_mdf: (str)
        ir_sec: (str) if ir_curator_value not found, (int) with metadata ir_curator_count value
    """
    # Validate ir_curator_count exists
    if "ir_curator_count" in data_df.columns:
        message_mdf = ""
        ir_sec = data_df["ir_curator_count"].tolist()[0]
    else:
        message_mdf = "ir_curator_count not found in metadata"
        ir_sec = 0

        # Handle
    if math.isnan(ir_sec):
        ir_sec = "Null"
    else:
        ir_sec = int(ir_sec)

    return [message_mdf, ir_sec]


def get_data_processing_files(data_df):
    """

    :param data_df: (dataframe object) contains row within metadata with selected repertoire
    :return: (list) contains file names for annotation files
    """
    if type(data_df["data_processing_files"].tolist()[0]) == float:
        sys.exit()

    else:
        ir_file = data_df["data_processing_files"].tolist()[0].replace(" ", "")
        line_one = ir_file.split(",")

    return line_one


def assess_test_results(ir_seq_api, sum_all, ir_sec, ir_rea):
    """

    :param ir_seq_api: number of facet count results in ADC API
    :param sum_all: number of lines in annotation file
    :param ir_sec: metadata ir_curator_count value
    :param ir_rea: rearrangement number
    :return:
        test_result (bool) True if test passes, False otherwise
    """
    test_flag = set([str(ir_seq_api), str(sum_all), str(ir_sec)])
    if len(test_flag) == 1:
        test_result = True
        print(ir_rea + " returned TRUE (test passed), see CSV for details")
    else:
        test_result = False
        print(ir_rea + " returned FALSE (test failed), see CSV for details")

    return test_result


def test_file_type_keyword(key, data_df):
    """

    :param key: one of 'imgt', 'mixcr', 'airr'
    :param data_df: (dataframe object) contains row within metadata with selected repertoire
    :return: flag (str) contains 'True_' or 'False_' along with the key selected
        'True_' denotes that they file extension was found, 'False_' denotes otherwise
    """
    # Get file names
    annotation_file_nm = data_df["data_processing_files"].tolist()[0].replace(" ", "")
    # Dictionary with test types and file endings
    test_type = {'imgt': ["txz"],
                 'airr': ["fmt", "tsv"],
                 'mixcr': ["txt"]}

    # Perform test
    if key == 'imgt':
        if test_type[key][0] not in annotation_file_nm:
            flag = "False_imgt"
        else:
            flag = "True_imgt"
    elif key == "mixcr":
        if test_type[key][0] not in annotation_file_nm:
            flag = "False_mixcr"
        else:
            flag = "True_mixcr"
    elif key == "airr":
        if test_type[key][0] not in annotation_file_nm and test_type[key][1] not in annotation_file_nm:
            flag = "False_airr"
        else:
            flag = "True_airr"
    else:
        print("WARNING, wrong key, functiont takes one of 'imgt', 'mixcr', 'airr'")
        print("Data provided", data_df)

    return flag


def run_count(check_file_end, line_one, files, test_type_key, annotation_dir):
    # Result storage
    sum_all = 0
    files_found = []
    files_notfound = []

    # Perform test
    if "False" in check_file_end:
        sum_all = "NFMD"

    else:

        for item in line_one:
            if item in files:
                # Annotation file found in directory
                files_found.append(item)

                # Proceed to identify which kind of file we are dealing with
                if "mixcr" in test_type_key or "airr" in test_type_key:
                    # Counting lines in annotation file
                    stri = subprocess.check_output(['wc', '-l', annotation_dir + str(item)])

                elif "imgt" in test_type_key:
                    # Extracting zipped files
                    tf = tarfile.open(annotation_dir + item)
                    tf.extractall(annotation_dir + str(item.split(".")[0]) + "/")
                    # Counting lines in annotation file, for 1_summary.txt file only
                    stri = subprocess.check_output(
                        ['wc', '-l', annotation_dir + str(item.split(".")[0]) + "/" + "1_Summary.txt"])

                else:
                    print("ERROR - file type not valid")
                    sys.exit(0)
                
                hold_val = stri.decode().split(' ')
                sum_all = sum_all + int(hold_val[0]) - 1

            else:
                files_notfound.append(item)

    return [files_found, files_notfound, sum_all]





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
        help="Name of annotation tool used to process sequences. Choice between MiXCR, VQuest, airr"
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
    print("DATA PROVENANCE TEST\n")
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
    # Build facet query
    facet_query = base_url + "/airr/v1/rearrangement"

    # Initialize sanity check
    sanity_check = SanityCheck(metadata_df=metadata, repertoire_json=json_input, facet_json=facet_json_input,
                               annotation_dir=annotation_directory, url_api_end_point=query_url,
                               study_id=study_id, mapping_file=mapping_file, output_directory=details_dir, url_facet_query = facet_query)
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
    sanity_check.validate_repertoire_data_airr(validate=False)

    # Annotation count
    print_separators()
    print("ANNOTATION COUNT")
    full_result_suite = []
    for item in unique_items:
        # Delay queries 
        time.sleep(1)

        rowAPI = concat_version[concat_version['repertoire_id']==str(item)]

        rowMD = data_df[data_df[connecting_field]==item]

        ir_file = rowMD["data_processing_files"].tolist()[0]  
        tool = rowMD["ir_rearrangement_tool"].to_list()[0]
            

        # Some entries may be empty - i.e. no files - skip but report 
        if type(rowMD["data_processing_files"].to_list()[0])==float:
            print("FOUND ODD ENTRY: ")
            print(str(data_df["data_processing_files"].tolist()[0]) + "\nrepertoire_id " + str(data_df["repertoire_id"].tolist()[0] ))
            print("Writing 0 on this entry, but be careful to ensure this is correct.\n")

            continue

        # Process each according to the tool used
        else:
            print("Processing annotations using:")
            print("  annotation_tool: %s"%(annotation_tool))
            print("  ir_rearrangement_tool: %s"%(tool))
            ############## CASE 1
            if "vquest" in annotation_tool.lower():
                result_iter = sanity_check.annotation_count(rowMD, rowMD['repertoire_id'].to_list()[0], "imgt")
                full_result_suite.append(result_iter)
                    
            ############## CASE 2            
            elif "airr" in annotation_tool.lower():
                result_iter = sanity_check.annotation_count(rowMD, rowMD['repertoire_id'].to_list()[0], "airr")
                full_result_suite.append(result_iter)
                
            ############## CASE 3                       
            elif "mixcr" in annotation_tool.lower():   
                result_iter = sanity_check.annotation_count(rowMD, rowMD['repertoire_id'].to_list()[0], "mixcr")
                full_result_suite.append(result_iter)
                
            ############### CASE 4
            elif "adaptive" in annotation_tool.lower():
                result_iter = sanity_check.annotation_count(rowMD, rowMD['repertoire_id'].to_list()[0], "airr")
                full_result_suite.append(result_iter)

            else:
                print("OBTAINED ANNOTATION TOOL",annotation_tool.lower())
                print("WARNING: Could not find appropriate annotation tool")
                print("Please specify one of 'MiXCR', 'AIRR', 'VQUEST' or 'Adaptive' in the annotation tool parameter")
                sys.exit(0)
                
    final_result = pd.concat(full_result_suite)
    count_file_name = str(study_id) + "_Facet_Count_curator_count_Annotation_count_"+str(pd.to_datetime('today')) + ".csv"
    final_result.to_csv(details_dir + count_file_name)
    print("For details on sequence count refer to " + count_file_name)

if __name__ == '__main__':
    main()
    
