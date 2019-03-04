
######### SANITY CHECK PYTHON SCRIPT
######### AUTHOR: LAURA GUTIERREZ FUNDERBURK
######### SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
######### CREATED ON: DECEMBER 20, 2018
######### LAST MODIFIED ON: MARCH 4, 2019

import pandas as pd
import json
import requests
import sys
import math
import os
import numpy
from xlrd import open_workbook, XLRDError
import subprocess
import tarfile

##################################
#### FUNCTION DEFINITION AREA ####
##################################


#### Section 1. Verify, read and parse files

def test_book(filename):
    try:
        open_workbook(filename)
    except XLRDError:
        return False
    else:
        return True
    

def verify_non_corrupt_file(master_metadata_file):
    
    if test_book(master_metadata_file)==False:
        print("CORRUPT FILE: Please verify master metadata file\n")
        sys.exit()
        
    else:
        print("HEALTHY FILE: Proceed with tests\n")



def get_unique_identifier(JSON_DATA_FILE,ir_rear_number):
    
    try:
        
        no_iterations = len(JSON_DATA_FILE)
        JSON_index = []
        for i in range(no_iterations):
            
            if 'ir_rearrangement_number' in JSON_DATA_FILE[i].keys():
                if int(JSON_DATA_FILE[i]["ir_rearrangement_number"])==(ir_rear_number):
                    JSON_index.append(i) 

        return JSON_index
    except:
        print("INVALID DATA FORMAT\nEnter a JSON file from API call, and an ir_rearrangement file from metadata spreadsheet.")

def get_dataframes_from_metadata(master_MD_dataframe):
    
    try:
        data_dafr = pd.read_excel(master_MD_dataframe,encoding = 'utf8')
        new_header = data_dafr.iloc[0] #grab the first row for the header
        data_dafr = data_dafr[1:] #take the data less the header row
        data_dafr.columns = new_header #set the header row as the df header
    
        return data_dafr
    except:
        print("INVALID INPUT\nInput is a single variable containing path and name to metadata spreadsheet.")

# Section 2. Sanity Checking        
        
def check_uniqueness_ir_rearrangement_nr(master_MD_dataframe):
    

    print("Uniquenes of ir_rearrangement_number:")
    
    if pd.Series(master_MD_dataframe["ir_rearrangement_number"]).is_unique==False:
        print("FALSE: There are duplicate entries under ir_rearrangement_number in master metadata\n")
       
    else:
        print("TRUE: All entries under ir_rearrangement_number in master metadata are unique\n")
        
        
def level_one(data_df,DATA):
    
    count_find =0
    count_not_find =0
    
    no_rows = data_df.shape[0]

    for i in range(no_rows):

        ir_rear_number = data_df.iloc[i]["ir_rearrangement_number"]
        JSON_entry = get_unique_identifier(DATA,ir_rear_number)
        if not JSON_entry:


            count_not_find +=1

        else:

            count_find +=1

    print(str(study_id) +  " has a total of " + str(no_rows) + " entries\nEntries found in API: " + str(count_find) + "\nEntries not found in API: " + str(count_not_find) + "\n")

def level_two(data_df,DATA):
    
    no_rows = data_df.shape[0]

    for i in range(no_rows):

        ir_rear_number = data_df.iloc[i]["ir_rearrangement_number"]
        JSON_entry = get_unique_identifier(DATA,ir_rear_number)

        print("ir_rearrangement_number: " + str(ir_rear_number))
        print("JSON file index: " + str(JSON_entry)  + "\n")

        if not JSON_entry:

            print("The ir_rearrangement_number associated to this study was not found in API response\n")

        else:

            column_names_JSON = set([item for item in DATA[JSON_entry[0]].keys()])
            column_names_MD = set([item for item in data_df.columns])
            intersection = column_names_JSON.intersection(column_names_MD)
            verify = column_names_JSON.symmetric_difference(column_names_MD)

            in_JSON = [item for item in verify if item in column_names_JSON]
            in_MD = [item for item in verify if item in column_names_MD]

            pass_a = []
            fail_a = []

            for item in DATA[JSON_entry[0]]:
                if item in intersection:
                    if type(DATA[JSON_entry[0]][item]) == type(data_df.iloc[i][item]):
                        if DATA[JSON_entry[0]][item] == data_df.iloc[i][item]:
                            pass_a.append(item)
                        else:
                            fail_a.append(item)

                    else:
                        if DATA[JSON_entry[0]][item]==None and type(data_df.iloc[i][item])==float:
                            x=float(data_df.iloc[i][item])
                            if math.isnan(x):
                                pass_a.append(item)
                            else:
                                fail_a.append(item)

                        else:
                                fail_a.append(item)



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
            print("TEST: FIELD CONTENT MATCHES\nRESULT --------------------------------------------------------------------------------->False" + "\n") 
            print("Summary of non-matching entries \n")
            for item in fail_a:
                print("ENTRY:  " + str(item))
                print("METADATA ENTRY RETURNS : "+  str(data_df.iloc[i][item]) + " type: " + str(type(data_df.iloc[i][item])))
                print("API RESPONSE RETURNS : "+ str(DATA[JSON_entry[0]][item]) + " type: " + str(type(DATA[JSON_entry[0]][item])) + "\n")

        print("END OF ENTRY\n")
        print("-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-_-\n")       
        
        
def level_three(data_df,annotation_dir):
    
    # Count number of lines in annotation file 
    
    with open("outfile_seq_count.csv","w") as f:

        f.write("File names" + "\t" + "Number of lines found in each file" + "\t" + "Sum of all lines" + "\n")
        for i in range(1,no_rows + 1):
            print("Begin iteration \n")
            
            tool = data_df["ir_rearrangement_tool"][i]
            
            if tool=="IMGT high-Vquest":
            
                number_lines = []
                sum_all = 0
                line_one = data_df["ir_rearrangement_file_name"][i].split(", ")
                
                if type(line_one)==float:
                    print("FOUND ODD ENTRY. Row index " + str(i) + ", ir_rearrangement_number: " + str(data_df["ir_rearrangement_number"][i]) + ". Skipping this entry, but be careful to ensure this is correct.\n")
                    continue
                    
                if type(line_one)==str and "txz" not in line_one:
                    print("FOUND ODD ENTRY. Row index " + str(i) + ", ir_rearrangement_number: " + str(data_df["ir_rearrangement_number"][i]) + ". Skipping this entry, but be careful to ensure this is correct.\n")
                    continue
                else:    
                    for item in line_one:
                        #print(item.split(".")[0])
                        tf = tarfile.open(annotation_dir + item)
                        tf.extractall(annotation_dir  + str(item.split(".")[0]) + "/")
                        stri = subprocess.check_output(['wc','-l',annotation_dir  + str(item.split(".")[0])+ "/" + "1_Summary.txt"])
                        hold_val = stri.decode().split(' ')
                        number_lines.append(hold_val[0])
                        sum_all = sum_all + int(hold_val[0]) - 1
                        subprocess.check_output(['rm','-r',annotation_dir  + str(item.split(".")[0])+ "/"])

                    f.write(str(line_one) + "\t" + str(number_lines) + "\t" + str(sum_all) + "\n")
                
                
            #else:
                
                
    f.close()   
        
##################################
####### VARIABLE INPUT AREA ######
##################################

input_f = str(sys.argv[1])
API_file = str(sys.argv[2])
study_id = str(sys.argv[3])
annotation_dir = str(sys.argv[4])
given_option = str(sys.argv[5])

# NEW IPA
with open(API_file) as f:
    DATA= json.load(f)
    
    
print("########################################################################################################")
print("---------------------------------------VERIFY FILES ARE HEALTHY-----------------------------------------\n")
print("---------------------------------------------Metadata file----------------------------------------------\n")
# GET METADATA    
if "xlsx" in input_f:
    verify_non_corrupt_file(input_f)
    master = get_dataframes_from_metadata(input_f)
elif "csv" in input_f:
    master = pd.read_csv(input_f ,encoding='utf8')
    master = master.replace('\n',' ', regex=True)

# Check ir_rearrangement_number is unique
check_uniqueness_ir_rearrangement_nr(master)

# Get metadata and specific study
master = master.replace('\n',' ', regex=True)
master["study_id"] = master["study_id"].str.strip()

data_df = master.loc[master['study_id'] == study_id]



if data_df.empty:
    print("EMPTY DATA FRAME: Cannot find specified study ID\n")
    print(data_df)
    sys.exit(0)
    
    
no_rows = data_df.shape[0]

print("---------------------------------------------API RESPONSE-----------------------------------------------\n")

if "ir_rearrangement_number" in pd.DataFrame.from_dict(DATA):
    print("TRUE: ir_rearrangement_number found in API response\n")
    all_ir_rearrangemet_unique = pd.DataFrame.from_dict(DATA)["ir_rearrangement_number"].unique()
    all_ir_rearrangemet = pd.DataFrame.from_dict(DATA)["ir_rearrangement_number"]

    if len(all_ir_rearrangemet_unique)==len(all_ir_rearrangemet):
        print("TRUE: ir_rearrangement_number unique in API response\n")
    else:
        print("WARNING: ir_rearrangement_number not unique in API response\n")
        summ_odd_entries = list(set(all_ir_rearrangemet).symmetric_difference(set(all_ir_rearrangemet_unique)))
        print("ODD ENTRIES: " + str(summ_odd_entries))
        
else:
    print("WARNING: ir_rearrangement_number not found in API response\n")
    sys.exit(0)
    
    

if "H" in given_option: 
    print("########################################################################################################")
    print("------------------------------------------HIGH LEVEL SUMMARY--------------------------------------------\n")
 

    stu_title= list(set(data_df['study_title']))[0]
    sub_by= list(set(data_df['submitted_by']))[0]

    print(str(stu_title) + "\n")
    print(str(sub_by) + "\n")

    print("Study ID " + str(study_id) + "\n")    

    level_one(data_df,DATA)




if "L" in given_option: 
    print("########################################################################################################")
    print("-----------------------------------------DETAILED SANITY CHECK------------------------------------------\n")
    print("--------------------------BEGIN METADATA AND API FIELD AND CONTENT VERIFICATION-------------------------\n")
    
    stu_title= list(set(data_df['study_title']))[0]
    sub_by= list(set(data_df['submitted_by']))[0]

    print(str(stu_title) + "\n")
    print(str(sub_by) + "\n")

    print("Study ID " + str(study_id) + "\n")    
    
    level_two(data_df,DATA)
    
    
if "F" in given_option: 
    print("########################################################################################################")
    print("-------------------------------------------ir_sequence_count-------------------------------------------\n")


    level_three(data_df,annotation_dir)

 
print("########################################################################################################")
