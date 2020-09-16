# ADC API PERFORMANCE TESTING PYTHON SCRIPT
# AUTHORS: Brian Corrie, Laura Gutierrez Funderburk
# SUPERVISOR: JAMIE SCOTT, FELIX BREDEN, BRIAN CORRIE
# CREATED ON: MAY 20, 2019
# LAST MODIFIED ON: November 19, 2019

import urllib.request, urllib.parse
import json
import os, ssl
import time 

#############################################################################
#################### ADC-API (File) Performance Testing #####################
#############################################################################

def processQuery(query_url, header_dict, expect_pass, query_dict={}, verbose=False, force=False):
    # Build the required JSON data for the post request. The user
    # of the function provides both the header and the query data

    # Convert the query dictionary to JSON
    query_json = json.dumps(query_dict)

    # Encode the JSON for the HTTP requqest
    query_json_encoded = query_json.encode('utf-8')

    # Try to connect the URL and get a response. On error return an
    # empty JSON array.
    try:
        # Build the request
        request = urllib.request.Request(query_url, query_json_encoded, header_dict)
        # Make the request and get a handle for the response.
        response = urllib.request.urlopen(request)
        # Read the response
        url_response = response.read()
        # If we have a charset for the response, decode using it, otherwise assume utf-8
        if not response.headers.get_content_charset() is None:
            url_response = url_response.decode(response.headers.get_content_charset())
        else:
            url_response = url_response.decode("utf-8")
        # Return the JSON data
        return url_response
    
    except urllib.error.HTTPError as e:
        if not expect_pass:
            if e.code == 400:
                # correct failure
                return json.loads('[400]')
        print('ERROR: Server could not fullfil the request to ' + query_url)
        print('ERROR: Error code = ' + str(e.code) + ', Message = ', e.read())
        return json.loads('[]')
    except urllib.error.URLError as e:
        print('ERROR: Failed to reach the server')
        print('ERROR: Reason =', e.reason)
        return json.loads('[]')
    except Exception as e:
        print('ERROR: Unable to process response')
        print('ERROR: Reason =' + str(e))
        return json.loads('[]')

def parse_query(url_response,filename):
    # This function takes as input a url_response obtained using the processQuery() function, and a file name 
    # and creates either a JSON or TSV file with the query response 
    try:
        # Check if processQuery returns an empty query and warn the user
        if url_response ==[]:
            print("WARNING: empty query")
        # Save as TSV if values are separated by tabs
        elif '\t' in url_response:
            fname  = str(filename) + ".tsv"
            with open(fname,"w") as f:
                for item in url_response:
                    f.write(item)
            f.close()
        # Alternative case - save as JSON file 
        else:
            fname = str(filename) + "_OUT.json"
            json_data = json.loads(url_response)
            with open(fname, 'w') as f:
                json.dump(json_data, f)
    except:
        print("WARNING: misusing parse_query function!")
    return fname

def getHeaderDict():
    # Set up the header for the post request.
    header_dict = {'accept': 'application/json',
                   'Content-Type': 'application/json'}
    return header_dict

def initHTTP():
    # Deafult OS do not have create cient certificate bundles. It is
    # easiest for us to ignore HTTPS certificate errors in this case.
    if (not os.environ.get('PYTHONHTTPSVERIFY', '') and
        getattr(ssl, '_create_unverified_context', None)):
        ssl._create_default_https_context = ssl._create_unverified_context

def process_json_files(force,verbose,query_file):
        
        # Open the JSON query file and read it as a python dict.
    with open(query_file, 'r') as f:
        try: 
            # Load file
            query_dict = json.load(f)

            if verbose:
                print('INFO: Performing query: ' + str(query_dict))    
            return query_dict
                
        except IOError as error:
            print("ERROR: Unable to open JSON file " + query_file + ": " + str(error))
        except json.JSONDecodeError as error:
            if force:
                print("WARNING: JSON Decode error detected in " + query_file + ": " + str(error))
                with open(query_file, 'r') as f:
                    query_dict = f.read().replace('\n', '')
            else:
                print("ERROR: JSON Decode error detected in " + query_file + ": " + str(error))
        except Exception as error:
            print("ERROR: Unable to open JSON file " + query_file + ": " + str(error))

