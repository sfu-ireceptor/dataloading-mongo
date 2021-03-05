# FACET QUERY GENERATOR FOR REPERTOIRE SANITY TESTING PYTHON SCRIPT
# AUTHOR: LAURA GUTIERREZ FUNDERBURK
# SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
# CREATED ON: June 2020
# LAST MODIFIED ON: July 22 2020


from curlairripa import *       # https://test.pypi.org/project/curlairripa/ 
import time                     # time stamps
import pandas as pd
import argparse
import os


def getArguments():
    # Set up the command line parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=""
    )
    
    # Array with URL
    parser.add_argument(
        "base_url",
        help="String containing URL to API server  (e.g. https://airr-api2.ireceptor.org)"
    )
    # Entry point
    parser.add_argument(
        "entry_point",
        help="Options: string 'rearragement' or string 'repertoire'"
    )
    
    
    parser.add_argument(
            "path_to_json",
        help="Enter full path to JSON query containing repertoire ID's for a given study - this must match the value given for study_id"
    )
    
    parser.add_argument(
        "no_filters",
        help="Enter full path to JSON query nofilters"
    )
    
    parser.add_argument(
        "study_id",
        help="Enter study_id"
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


if __name__ == "__main__":

    options = getArguments()
    base_url = options.base_url
    entry_pt = options.entry_point
    path_to_json = options.path_to_json
    no_filters = options.no_filters
    study_id = options.study_id
    verbose = options.verbose
    
    query_url = base_url + "/airr/v1/" + entry_pt
    
    
    # Leave static for now
    expect_pass = True
    force = True

    # Ensure our HTTP set up has been done.
    initHTTP()
    # Get the HTTP header information (in the form of a dictionary)
    header_dict = getHeaderDict()
    
    # Process json file into JSON structure readable by Python
    query_dict = process_json_files(force,verbose,no_filters)
    
    
    # Perform the query. Time it
    start_time = time.time()
    query_json = processQuery(query_url, header_dict, expect_pass, query_dict, verbose, force)
    if len(query_json) == 0:
            print('ERROR: Query failed to %s'%(query_url))
            exit(1)

    total_time = time.time() - start_time
    
    
    st_id = pd.json_normalize(json.loads(query_json),record_path="Repertoire")['study.study_id'].unique()
    
    count = 0
    
    path = study_id + "/"
    if os.path.isdir(str(path_to_json) + str(path))==False:
        if verbose: print("INFO: PATH DOES NOT EXIST")
        os.mkdir(str(path_to_json) + str(path))
    else:
        if verbose: print("INFO: PATH EXISTS")
        
    rep_ids = pd.json_normalize(json.loads(query_json),record_path="Repertoire")['repertoire_id'].to_list()
        
    for repid in rep_ids:
            
        with open(str(path_to_json) + str(path) + "facet_repertoire_id_" +repid + ".json","w" ) as f:
            f.write('{"filters": {"op": "=", "content": {"field": "repertoire_id", "value": "' + str(repid)  + '"}}, "facets": "repertoire_id"}')
        f.close()
