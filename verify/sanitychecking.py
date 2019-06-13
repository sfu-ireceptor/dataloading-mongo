######### SANITY CHECK PYTHON SCRIPT
######### AUTHOR: LAURA GUTIERREZ FUNDERBURK
######### SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
######### CREATED ON: DECEMBER 20, 2018
######### LAST MODIFIED ON: APRIL 18, 2019

import pandas as pd
import json
import requests
import sys
import math
import os
from xlrd import open_workbook, XLRDError
import subprocess
import tarfile
import numpy
import argparse

##################################
#### FUNCTION DEFINITION AREA ####
##################################

#### Section 1. Verify, read and parse files
# Test I can open file
def test_book(filename):
    
    """This function verifies whether it is possible to open a metadata EXCEL file. 
    
    It returns True if yes, False otherwise"""
    try:
        open_workbook(filename)
    except XLRDError:
        return False
    else:
        return True
    
# Report whether file can be opened or not
def verify_non_corrupt_file(master_metadata_file):
    
    """This function verifies whether test_book returns True or False and prints a message to screen in either case"""
    
    try:
        if test_book(master_metadata_file)==False:
            print("CORRUPT FILE: Please verify master metadata file\n")
            sys.exit()
        
        else:
            print("HEALTHY FILE: Proceed with tests\n")
    except:
        
        print("INVALID INPUT\nInput is an EXCEL metadata file.")
    
        
# Get appropriate metadata sheet        
def get_metadata_sheet(master_metadata_file):
    
    """This function extracts the 'metadata' sheet from an EXCEL metadata file """

    # Tabulate Excel file
    table = pd.ExcelFile(master_metadata_file)#,encoding="utf8")
    # Identify sheet names in the file and store in array
    sheets = table.sheet_names
    # How many sheets does it have
    number_sheets = len(sheets)

    ### Select metadata spreadsheet
    metadata_sheet = ""
    for i in range(number_sheets):
        # Check which one contains the word metadata in the title and hold on to it
        if "Metadata"== sheets[i] or "metadata"==sheets[i]:
            metadata_sheet = metadata_sheet + sheets[i]
            break 
        # Need to design test that catches when there is no metadata spreadsheet ; what if there are multiple metadata sheets?        
        
    # This is the sheet we want
    metadata = table.parse(metadata_sheet)
    
    return metadata

# Parse metadata sheet as pandas dataframe
def get_dataframes_from_metadata(master_MD_sheet):
    
    """This function parses the metadata EXCEL sheet into a pandas dataframe
    
    EXCEL metadata sheets normally have 2 headers: internal-use headers and AIRR header
    
    This function creates a pandas dataframe using only the AIRR headers. This is the dataframe
    
    that the sanity checks will be performed on"""
    
    try:
        # Get the appropriate sheet from EXCEL metadata file
        data_dafr = get_metadata_sheet(master_MD_sheet) 
        
        #grab the first row for the header
        new_header = data_dafr.iloc[0] 
        #take the data less the header row
        data_dafr = data_dafr[1:] 
        #set the header row as the df header
        data_dafr.columns = new_header 
    
        return data_dafr
    except:
        print("INVALID INPUT\nInput is a single variable containing path and name to metadata spreadsheet.")

# Section 2. Sanity Checking        
# Uniqueness and existence of field uniquely identifying each sample in metadata        
def check_uniqueness_ir_rearrangement_nr(master_MD_dataframe,unique_field_id):  
    
    """This function verifies that the unique field used to identify each sample exists and is unique in metadata"""
    
    try:
        print("Existence and uniquenes of " + str(unique_field_id) + " in metadata")

        # Check it exists
        if unique_field_id not in master_MD_dataframe.columns:
            print("WARNING: FIELD NAME DOES NOT EXIST TO UNIQUELY IDENTIFY SAMPLES IN THIS STUDY\n")
            print("Verify the column name exists and contains the correct information in your spreadsheet\n")
            sys.exit(0)

        else:
            # Check it is unique
            if pd.Series(master_MD_dataframe[unique_field_id]).is_unique==False:
                print("FALSE: There are duplicate entries under "+ str(unique_field_id) + " in master metadata\n")

            else:
                print("TRUE: All entries under  "+ str(unique_field_id) + "  in master metadata are unique\n")
    except:
        
        print("INVALID INPUT\nInput is a dataframe containing metadata and a field from metadata which uniquely identifies each sample.")


# Uniqueness and existence of field uniquely identifying each sample in API response                
def check_unique_identifier_exists_API(JSON_DATA_FILE,unique_field):
    
    """This function verifies that the unique field used to identify each sample exists and is unique in API response"""

    
    try:
        print("Existence and uniqueness of " + str(unique_field) + " in API response")
    
        # Check it exists
        if unique_field in pd.DataFrame.from_dict(JSON_DATA_FILE):
            print("TRUE: " + unique_field + " found in API response\n")
            all_ir_rearrangemet_unique = pd.DataFrame.from_dict(DATA)[unique_field].unique()
            all_ir_rearrangemet = pd.DataFrame.from_dict(DATA)[unique_field]

            # Check it is unique
            if len(all_ir_rearrangemet_unique)==len(all_ir_rearrangemet):
                print("TRUE: " + str(unique_field) + " unique in API response\n")
            else:
                print("WARNING: " +  str(unique_field) + " not unique in API response\n")
                summ_odd_entries = list(set(all_ir_rearrangemet).symmetric_difference(set(all_ir_rearrangemet_unique)))
                print("ODD ENTRIES: " + str(summ_odd_entries))

        else:
            print("WARNING: " + str(unique_field) + " not found in API response\n")
            sys.exit(0)
    except:
        print("INVALID INPUT\nInput is a JSON file containing the API response and a field name which uniquely identifies each sample.")
        
# Verify corresponding unique identifier exists in API response
def get_unique_identifier(JSON_DATA_FILE,unique_field_id,ir_rear_number):
    
    """This function obtains the index corresponding to a sample found in API response
    
    This function uses the unique identifies that the user provided, the unique number associated to it
    
    As well as the JSON file name containing API response"""
    
    try:
        # Get total numnber of entries in JSON file containing API response
        no_iterations = len(JSON_DATA_FILE)
        
        # Set up array to store index
        JSON_index = []
        # Iterate over all entries
        for i in range(no_iterations):
            # Check unique identifier is found in the entry 
            if unique_field_id in JSON_DATA_FILE[i].keys():
                # Check value under unique identifier matches the unique identifier for that sample in metadata
                if JSON_DATA_FILE[i][unique_field_id]==ir_rear_number:
                   # if both conditions are met, append the index, otherwise the array will be empty
                    JSON_index.append(i) 

        return JSON_index
    except:
        print("INVALID DATA FORMAT\nEnter a JSON file from API response, a field name which uniquely identifies each sample and an entry uniquely identifying the sample.")

        
def level_one(data_df,DATA,unique_field_id):
    
    count_find =0
    count_not_find =0
    
    no_rows = data_df.shape[0]

    for i in range(no_rows):

        ir_rear_number = data_df.iloc[i][unique_field_id]
        JSON_entry = get_unique_identifier(DATA,unique_field_id,ir_rear_number)
        if not JSON_entry:

            count_not_find +=1

        else:

            count_find +=1

    print(str(study_id) +  " has a total of " + str(no_rows) + " entries\nEntries found in API: " + str(count_find) + "\nEntries not found in API: " + str(count_not_find) + "\n")

def level_two(data_df,DATA,unique_field_id):
    
    no_rows = data_df.shape[0]
    
    count_find =0
    count_not_find =0
    
    no_rows = data_df.shape[0]
    
    store_sanity_check_results = []

    for i in range(no_rows):

        ir_rear_number = data_df.iloc[i][unique_field_id]
        JSON_entry = get_unique_identifier(DATA,unique_field_id,ir_rear_number)

        print(str(unique_field_id) + ": " + str(ir_rear_number))
        print("JSON file index: " + str(JSON_entry)  + "\n")

        pass_a = []
        fail_a = []

        if not JSON_entry:

            print("The " + str(unique_field_id) + " associated to this study was not found in API response\n")

            count_not_find +=1
            
            #store_sanity_check_results.append([ir_rear_number,JSON_entry,[],[],[],[],count_not_find])
            
        else:
            
            count_find +=1

            column_names_JSON = set([item for item in DATA[JSON_entry[0]].keys()])
            column_names_MD = set([item for item in data_df.columns])
            intersection = column_names_JSON.intersection(column_names_MD)
            verify = column_names_JSON.symmetric_difference(column_names_MD) - {'ir_created_at', 'ir_updated_at','_id', 'ir_project_sample_id','ir_sequence_count'}

            in_JSON = [item for item in verify if item in column_names_JSON]
            in_MD = [item for item in verify if item in column_names_MD]

            

            for item in DATA[JSON_entry[0]]:
                if item in intersection:
                    try:
                        if DATA[JSON_entry[0]][item] == data_df.iloc[i][item]:
                            pass_a.append(item)
                                
                                ## HANDLE NANs and NoneTypes 
                        elif DATA[JSON_entry[0]][item]==None and type(data_df.iloc[i][item])==float:
                            x=float(data_df.iloc[i][item])
                            if math.isnan(x):
                                pass_a.append(item)
                            else:
                                fail_a.append(item)
                        elif DATA[JSON_entry[0]][item]==None and type(data_df.iloc[i][item])==numpy.float64:
                            x=float(data_df.iloc[i][item])
                            if math.isnan(x):
                                pass_a.append(item)
                            else:
                                fail_a.append(item)
            # In this case python thinks the items are comparable (of the same type as 
            # far as not generating an exception) but not the same value
                        else:
                            fail_a.append(item)
                    except TypeError:
                        print("UNABLE TO COMPARE ENTRIES")
                  
                            
                else:
                    continue
            store_sanity_check_results.append([ir_rear_number,in_JSON,pass_a,fail_a])
            #store_sanity_check_results.append([ir_rear_number,JSON_entry,pass_a,fail_a,in_MD,in_JSON,count_find])
            # PRINT RESULTS
            print("TEST: FIELD NAMES MATCH\nRESULT --------------------------------------------------------------------------------->" + str(column_names_JSON.issubset(column_names_MD)) + "\n")

            
            print("Summary of non-matching field names \n")

            print("Field names in API response \n")
            for item in in_JSON:
                print(str(item))

            print("\n")
            print("Field names in Metadata \n")
            for item in in_MD:
                print(str(item))
            print("\n")    
            
            if len(fail_a)==0:
                print("TEST: FIELD CONTENT MATCHES\nRESULT --------------------------------------------------------------------------------->TRUE "  + "\n") 
            else:
                print("TEST: FIELD CONTENT MATCHES\nRESULT --------------------------------------------------------------------------------->FALSE "  + "\n") 
           
                print("Summary of non-matching entries \n")
                for item in fail_a:
                    print("ENTRY:  " + str(item))
                    print("METADATA ENTRY RETURNS : "+  str(data_df.iloc[i][item]) + " type: " + str(type(data_df.iloc[i][item])))
                    print("API RESPONSE RETURNS : "+ str(DATA[JSON_entry[0]][item]) + " type: " + str(type(DATA[JSON_entry[0]][item])) + "\n")
                    


        print("END OF ENTRY\n")
        print("-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-\n")
    return store_sanity_check_results
        
def ir_seq_count_imgt(imgt_subdir,data_df,integer,DATA,unique_field_id):
    
    number_lines = []
    sum_all = 0
    files_found = []
    files_notfound = []
    ir_file = data_df["ir_rearrangement_file_name"].tolist()[integer]  
    files = os.listdir(annotation_dir + imgt_subdir)
    
    if "txz" not in ir_file:
        number_lines.append(0)
        sum_all = "NFMD"

    else:   
        line_one = ir_file.split(", ")
        for item in line_one:
            if item in files:
                files_found.append(item)
                tf = tarfile.open(annotation_dir + imgt_subdir + item)
                tf.extractall(annotation_dir  + str(item.split(".")[0]) + "/")
                stri = subprocess.check_output(['wc','-l',annotation_dir  + str(item.split(".")[0])+ "/" + "1_Summary.txt"])
                hold_val = stri.decode().split(' ')
                number_lines.append(hold_val[0])
                sum_all = sum_all + int(hold_val[0]) - 1
                subprocess.check_output(['rm','-r',annotation_dir  + str(item.split(".")[0])+ "/"])
            else:
                files_notfound.append(item)
                

        ir_rea = int(data_df[unique_field_id].tolist()[integer])
        JSON_entry = get_unique_identifier(DATA,unique_field_id,ir_rea)
        if not JSON_entry:
            ir_seq_API = "NINAPI"
        else:
            ir_seq_API = str(DATA[JSON_entry[0]]['ir_sequence_count']) 
            
        if "ir_curator_count" in data_df.columns:
            message_mdf=""
            ir_sec = data_df["ir_curator_count"].tolist()[integer]
        else:
            message_mdf= "ir_curator_count not found in metadata"
            ir_sec = 0
        
        test_flag = set([str(ir_seq_API), str(sum_all), str(int(ir_sec))])
        if len(test_flag)==1:
            test_result = True
        else:
            test_result=False
        
        print("\n")
        print(str(unique_field_id) + ": " + str(ir_rea))
        print("Metadata file names: " + str(line_one))
        print("Files found in server: " + str(files_found))
        print("Files not found in server: " + str(files_notfound))
        print(str(message_mdf))
        print(". . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ")
        print("\t\t\t\tir_sequence_count \t\t\t#Lines Annotation F \tTest Result")
        print(". . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ")
        print("\t\t\t\tAPI Resp \t Metadata Resp")
        print(". . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ")
        print("\t\t\t\t" + str(ir_seq_API) +" \t\t " + str(int(ir_sec)) + "\t\t" + str(sum_all) + "\t\t\t" + str(test_result))
        print("\n")
        print(" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -")
        
def ir_seq_count_igblast(igbl_subdir,data_df,integer,DATA,unique_field_id):     
    
    number_lines = []
    sum_all = 0
    files_found = []
    files_notfound = []
    ir_file = data_df["ir_rearrangement_file_name"].tolist()[integer] 
    files = os.listdir(annotation_dir + igbl_subdir)

    if "fmt" not in ir_file:
        number_lines.append(0)
        sum_all = "NFMD"
    else:   
        line_one = ir_file.split(", ")
        for item in line_one:
            if item in files:
                if "fmt19" in item:
                    files_found.append(item)
                    stri = subprocess.check_output(['wc','-l',annotation_dir + igbl_subdir + str(item)])
                    hold_val = stri.decode().split(' ')
                    number_lines.append(hold_val[0])
                    sum_all = sum_all + int(hold_val[0]) - 1
                else:
                    continue
            else:
                files_notfound.append(item)
                

        ir_rea = int(data_df[unique_field_id].tolist()[integer])
        JSON_entry = get_unique_identifier(DATA,unique_field_id,ir_rea)
        if not JSON_entry:
            ir_seq_API = "NINAPI"
        else:
            ir_seq_API = str(DATA[JSON_entry[0]]['ir_sequence_count']) 
            
        if "ir_curator_count" in data_df.columns:
            message_mdf=""
            ir_sec = data_df["ir_curator_count"].tolist()[integer]
        else:
            message_mdf= "ir_curator_count not found in metadata"
            ir_sec = 0
        
        test_flag = set([str(ir_seq_API), str(sum_all), str(int(ir_sec))])
        if len(test_flag)==1:
            test_result = True
        else:
            test_result=False
        
        print("\n")
        print(str(unique_field_id) + ": " + str(ir_rea))
        print("Metadata file names: " + str(line_one))
        print("Files found in server: " + str(files_found))
        print("Files not found in server: " + str(files_notfound))
        print(str(message_mdf))
        print(". . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ")
        print("\t\t\t\tir_sequence_count \t\t\t#Lines Annotation F \tTest Result")
        print(". . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ")
        print("\t\t\t\tAPI Resp \t Metadata Resp")
        print(". . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ")
        print("\t\t\t\t" + str(ir_seq_API) +" \t\t " + str(int(ir_sec)) + "\t\t" + str(sum_all) + "\t\t\t" + str(test_result))
        print("\n")
        print(" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -")

def ir_seq_count_mixcr(mixr_subdir,data_df,integer,DATA,unique_field_id):
    
    number_lines = []
    sum_all = 0
    files_found = []
    files_notfound = []
    ir_file = data_df["ir_rearrangement_file_name"].tolist()[integer] 
    files = os.listdir(annotation_dir)
    
    if "txt" not in ir_file:
        number_lines.append(0)
        sum_all = "NFMD"

    else:
        line_one = ir_file.split(", ")
        
        for item in line_one:
            if item in files:
                #if "annotation" in item:
                files_found.append(item)
                stri = subprocess.check_output(['wc','-l',annotation_dir + mixr_subdir +str(item)])
                hold_val = stri.decode().split(' ')
                number_lines.append(hold_val[0])
                sum_all = sum_all + int(hold_val[0]) - 1
                #else:
                    #continue
            else:
                files_notfound.append(item)
                

        ir_rea = int(data_df[unique_field_id].tolist()[integer])
        JSON_entry = get_unique_identifier(DATA,unique_field_id,ir_rea)
        if not JSON_entry:
            ir_seq_API = "NINAPI"
        else:
            ir_seq_API = str(DATA[JSON_entry[0]]['ir_sequence_count']) 
            
        if "ir_curator_count" in data_df.columns:
            message_mdf=""
            ir_sec = data_df["ir_curator_count"].tolist()[integer]
        else:
            message_mdf= "ir_curator_count not found in metadata"
            ir_sec = 0
        
        test_flag = set([str(ir_seq_API), str(sum_all), str(int(ir_sec))])
        if len(test_flag)==1:
            test_result = True
        else:
            test_result=False
        
        print("\n")
        print(str(unique_field_id) + ": " + str(ir_rea))
        print("Metadata file names: " + str(line_one))
        print("Files found in server: " + str(files_found))
        print("Files not found in server: " + str(files_notfound))
        print(str(message_mdf))
        print(". . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ")
        print("\t\t\t\tir_sequence_count \t\t\t#Lines Annotation F \tTest Result")
        print(". . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ")
        print("\t\t\t\tAPI Resp \t Metadata Resp")
        print(". . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . . ")
        print("\t\t\t\t" + str(ir_seq_API) +" \t\t " + str(int(ir_sec)) + "\t\t" + str(sum_all) + "\t\t\t" + str(test_result))
        print("\n")
        print(" - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -")
            
def level_three(data_df,annotation_dir,imgt_subdir,mixr_subdir,igbl_subdir,study_id,DATA,unique_field_id):
    
    no_rows = data_df.shape[0]

    # Count number of lines in annotation file     
    
    for i in range(0,no_rows):
        tool = data_df["ir_rearrangement_tool"].tolist()[i]
        ir_file = data_df["ir_rearrangement_file_name"].tolist()[i]           
        ir_rea = data_df[unique_field_id].tolist()[i] 
        
        
        
        if type(ir_file)!=str:
                print(type(ir_file))
                number_lines = []
                sum_all = 0
                print("FOUND ODD ENTRY: " + str(ir_file) + "\nRow index " + str(i) + ", " + str(unique_field_id) + ": " + str(ir_rea) + ". Writing 0 on this entry, but be careful to ensure this is correct.\n")
                number_lines.append(0)
                sum_all = sum_all + 0
     
                continue
        
        else:
            ############## CASE 1
            if tool=="IMGT high-Vquest":
                ir_seq_count_imgt(imgt_subdir,data_df,i,DATA,unique_field_id)
            

            ############## CASE 2            
            elif tool=="igblast":
                ir_seq_count_igblast(igbl_subdir,data_df,i,DATA,unique_field_id)

            ############## CASE 3                       
            elif tool=="MiXCR":   
                ir_seq_count_mixcr(mixr_subdir,data_df,i,DATA,unique_field_id)
                
                
def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )

    # Input file: excel or csv metadata file
    parser.add_argument("input_f")
    # Input file: JSON file containing API response
    parser.add_argument("API_file")
    # Study_id used to identify a given study in sample metadata
    parser.add_argument("study_id")
    # Directory containing annotation files
    parser.add_argument("annotation_dir")
    # Annotations subdirectory containing annotations processed using imgt
    parser.add_argument("imgt")
    # Annotations subdirectory containing annotations processed using mixcr
    parser.add_argument("mixcr")
    # Annotations subdirectory containing annotations processed using igblast
    parser.add_argument("igblast")
    # High level, low level, file level sanity check option
    parser.add_argument("sanity_level")
    # Field used to uniquely identify each sample
    parser.add_argument("unique_id")
    # Verbosity flag
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Run the program in verbose mode.")

    # Parse the command line arguements.
    options = parser.parse_args()
    return options
        
##################################
####### VARIABLE INPUT AREA ######
##################################

if __name__ == "__main__":
    
    # Get the command line arguments.
    options = getArguments()
    input_f = options.input_f
    API_file = options.API_file
    study_id = options.study_id
    annotation_dir = options.annotation_dir
    imgt_subdir = options.imgt
    mixr_subdir = options.mixcr
    igbl_subdir = options.igblast
    given_option = options.sanity_level
    input_unique_field_id = options.unique_id

    # NEW IPA
    with open(API_file) as f:
        DATA= json.load(f)
    f.close()

    print("########################################################################################################")
    print("---------------------------------------VERIFY FILES ARE HEALTHY-----------------------------------------\n")
    print("---------------------------------------------Metadata file----------------------------------------------\n")
    # GET METADATA    
    if "xlsx" in input_f:
        verify_non_corrupt_file(input_f)
        master = get_dataframes_from_metadata(input_f)
    elif "csv" in input_f:
        master = pd.read_csv(input_f ,encoding='utf8')
        master = master.loc[:, ~master.columns.str.contains('^Unnamed')]


    # Get metadata and specific study
    master = master.replace('\n',' ', regex=True)
    master["study_id"] = master["study_id"].str.strip()

    data_df = master.loc[master['study_id'] == study_id]

    # Check entries under unique identifier are  unique
    check_uniqueness_ir_rearrangement_nr(data_df,input_unique_field_id)

    if data_df.empty:
        print("EMPTY DATA FRAME: Cannot find specified study ID\n")
        print(data_df)
        sys.exit(0)

    no_rows = data_df.shape[0]

    print("---------------------------------------------API RESPONSE-----------------------------------------------\n")

    check_unique_identifier_exists_API(DATA,input_unique_field_id)


    if "H" in given_option: 
        print("########################################################################################################")
        print("------------------------------------------HIGH LEVEL SUMMARY--------------------------------------------\n")

        stu_title= list(set(data_df['study_title']))[0]
        sub_by= list(set(data_df['submitted_by']))[0]

        print(str(stu_title) + "\n")
        print(str(sub_by) + "\n")
        print("Study ID " + str(study_id) + "\n")    

        level_one(data_df,DATA,input_unique_field_id)

    if "L" in given_option: 
        print("########################################################################################################")
        print("-----------------------------------------DETAILED SANITY CHECK------------------------------------------\n")
        print("--------------------------BEGIN METADATA AND API FIELD AND CONTENT VERIFICATION-------------------------\n")

        stu_title= list(set(data_df['study_title']))[0]
        sub_by= list(set(data_df['submitted_by']))[0]

        print(str(stu_title) + "\n")
        print(str(sub_by) + "\n")

        print("Study ID " + str(study_id) + "\n")    

        level_two(data_df,DATA,input_unique_field_id)

    if "F" in given_option: 
        print("########################################################################################################")
        print("-------------------------------------------ir_sequence_count-------------------------------------------\n")

        level_three(data_df,annotation_dir,imgt_subdir,mixr_subdir,igbl_subdir,study_id,DATA,input_unique_field_id)

    print("########################################################################################################")
    """TO DO 
    edit code so that metadata boolean is interpreted as equivalent to a str 0 or 1

    add warning 
    """