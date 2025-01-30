#! /opt/ireceptor/data/bin/python
"""
 add_reactivity.py is a script to add reactivity_id and reactivity_ref 
 values, as well as other related iReceptor specific reactivity information
 to the Rearrangement collection in MongoDB.
"""
import os
import argparse
import json
import math
import time
import sys
import pandas as pd

# AIRR Mapping class.
from airr_map import AIRRMap
# Repository class - hides the DB implementation
from repository import Repository
# Rearrangement loader classes
from rearrangement import Rearrangement
# Parser class
from parser import Parser

# Get the command line arguments...
def getArguments():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Note: for proper data processing, project --samples metadata should\n" +
        "generally be read first into the database before loading other data types."
    )

    parser.add_argument("--version", action="version", version="%(prog)s 2.0")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Run the program in verbose mode. This option will generate a lot of output, but is recommended from a data provenance perspective as it will inform you of how it mapped input data columns into repository columns.")
    parser.add_argument(
        "--skipload",
        action="store_true",
        help="Run the program without actually lodaing data into the repository. This option will allow testing of the entire load process without changing the repository.")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Run the program in append mode rather than replace mode.")

    # Add configuration options 
    config_group = parser.add_argument_group("Configuration file options", "")
    config_group.add_argument(
        "--mapfile",
        dest="mapfile",
        default="ireceptor.cfg",
        help="the iReceptor configuration file. Defaults to 'ireceptor.cfg' in the local directory where the command is run. This file contains the mappings between the AIRR Community field definitions, the annotation tool field definitions, and the fields and their names that are stored in the repository."
    )

    # Database options
    db_group = parser.add_argument_group("database options")
    db_group.add_argument(
        "--host",
        dest="host",
        default="localhost",
        help="MongoDb server hostname. Defaults to 'localhost'."
    )
    db_group.add_argument(
        "--port",
        dest="port",
        default=27017,
        type=int,
        help="MongoDb server port number. Defaults to 27017."
    )
    db_group.add_argument(
        "-u",
        "--user",
        dest="user",
        default=os.environ.get("MONGODB_SERVICE_USER", ""),
        help="MongoDb user name. Defaults to the MONGODB_SERVICE_USER environment variable if set. Defaults to empty string (no user name) otherwise."
    )
    db_group.add_argument(
        "-p",
        "--password",
        dest="password",
        default=os.environ.get("MONGODB_SERVICE_SECRET", ""),
        help="MongoDb service user account password. Defaults to the MONGODB_SERVICE_SECRET environment variable if set. Defaults to empty string (no password) otherwise."
    )
    db_group.add_argument(
        "-d",
        "--database",
        dest="database",
        default=os.environ.get("MONGODB_DB", "ireceptor"),
        help="Target MongoDb database. Defaults to the MONGODB_DB environment variable if set. Defaults to 'ireceptor' otherwise."
    )
    db_group.add_argument(
        "--database_map",
        dest="database_map",
        default="ir_repository",
        help="Mapping to use to map data terms into repository terms. Defaults to ir_repository, which is the mapping for the iReceptor Turnkey repository. This mapping keyword MUST be in the term mapping file as specified by --mapfile"
    )
    db_group.add_argument(
        "--database_chunk",
        dest="database_chunk",
        default=100000,
        type=int,
        help="Number of records to process in a single step when loading rearrangment data into the repository. This is used to reduce the memory footpring of the loading process when very large files are being loaded. Defaults to 100,000"
    )
    db_group.add_argument(
        "--repertoire_collection",
        dest="repertoire_collection",
        default="sample",
        help="The collection to use for storing and searching repertoires (sample metadata). This is the collection that sample metadata is inserted into when the --sample option is specified. Defaults to 'sample', which is the collection in the iReceptor Turnkey repository."
    )
    db_group.add_argument(
        "--rearrangement_collection",
        dest="rearrangement_collection",
        default="sequence",
        help="The collection to use for storing and searching rearrangements (sequence annotations). This is the collection that data is inserted into when the --mixcr, --imgt, and --airr options are used to load files. Defaults to 'sequence', which is the collection in the iReceptor Turnkey repository."
    )
    db_group.add_argument(
        "--clone_collection",
        dest="clone_collection",
        default="clone",
        help="The collection to use for storing and searching clones. This is the collection that data is inserted into when the --mixcr-clone option is used to load files. Defaults to 'clone', which is the collection in the iReceptor Turnkey repository."
    )
    db_group.add_argument(
        "--cell_collection",
        dest="cell_collection",
        default="cell",
        help="The collection to use for storing and searching cells. This is the collection that data is inserted into when the --airr-cell option is used to load files. Defaults to 'cell', which is the collection in the iReceptor Turnkey repository."
    )
    db_group.add_argument(
        "--expression_collection",
        dest="expression_collection",
        default="expression",
        help="The collection to use for storing and searching gene expression. This is the collection that data is inserted into when the --airr-expression option is used to load files. Defaults to 'expression', which is the collection in the iReceptor Turnkey repository."
    )
    db_group.add_argument(
        "--receptor_collection",
        dest="receptor_collection",
        default="receptor",
        help="The collection to use for storing and searching receptors. This is the collection that data is inserted into when the --airr-receptor option is used to load files. Defaults to 'receptor', which is the collection in the iReceptor Turnkey repository."
    )
    db_group.add_argument(
        "--reactivity_collection",
        dest="reactivity_collection",
        default="reactivity",
        help="The collection to use for storing and searching reactivity data. This is the collection that data is inserted into when the --airr-reactivity option is used to load files. Defaults to 'reactivity', which is the collection in the iReceptor Turnkey repository."
    )


    # Application specific command line arguments
    reactivity_group = parser.add_argument_group("reactivity options")
    #parser.add_argument(
    #    "gene_field",
    #    help="Field in which the gene name is searched for, and if found, replaced."
    #)
    #parser.add_argument(
    #    "allele_field",
    #    help="Field in which the allele name is searched for, and if found, replaced."
    #)
    parser.add_argument(
        "reactivity_file",
        help="TSV file that contains the reactivity data to load for a sequence. It assumes that there is a column called 'sequence_id' in the file that contains the ID of the sequence to which the change should occur."
    )

    options = parser.parse_args()

    if options.verbose:
        print('HOST               :', options.host)
        print('PORT               :', options.port)
        print('USER               :', options.user[0] + (len(options.user) - 2) * "*" + options.user[-1] if options.user else "")
        print('PASSWORD           :', options.password[0] + (len(options.password) - 2) * "*" + options.password[-1] if options.password else "")
        print('DATABASE           :', options.database)
        print('DATABASE_MAP       :', options.database_map)
        print('MAPFILE            :', options.mapfile)

    return options

def processRearrangements(reactivity_df, repository, airr_map, rearrangementParser, append, verbose, skipload):
    # Start timing the processing
    t_start = time.perf_counter()
    t_update_total = 0

    # Set the tag for the repository that we are using.
    repository_tag = rearrangementParser.getRepositoryTag()

    # Set the tag for the AIRR Standard
    airr_tag = rearrangementParser.getAIRRTag()

    # Get the tag to use for iReceptor specific mappings
    ireceptor_tag = rearrangementParser.getiReceptorTag()

    # Get the sequence_id field that is used to identify unique
    # sequences in the repository.
    repo_sequence_id_field = airr_map.getMapping("rearrangement_id",
                                            ireceptor_tag, repository_tag,
                                            airr_map.getRearrangementClass())
    # Get the sequence_id field that is used to identify unique
    # sequences in the input_file. This is the AIRR field name
    airr_sequence_id_field = airr_map.getMapping("rearrangement_id",
                                            ireceptor_tag, airr_tag,
                                            airr_map.getRearrangementClass())

    # Get the field in the repository that is used to store data update time
    updated_at_field = airr_map.getMapping("ir_updated_at_rearrangement",
                                           ireceptor_tag, repository_tag)

    reactivity_id_file = "reactivity_id"
    reactivity_id_repo = airr_map.getMapping("reactivity_id",
                                             ireceptor_tag, repository_tag,
                                             airr_map.getRearrangementClass())
    reactivity_ref_file = "reactivity_ref"
    reactivity_ref_repo = airr_map.getMapping("reactivity_ref",
                                              ireceptor_tag, repository_tag,
                                              airr_map.getRearrangementClass())
    reactivity_method_file = "ir_reactivity_method"
    reactivity_method_repo = airr_map.getMapping("ir_reactivity_method",
                                              ireceptor_tag, repository_tag,
                                              airr_map.getIRRearrangementClass())
    epitope_ref_file = "ir_epitope_ref"
    epitope_ref_repo = airr_map.getMapping("ir_epitope_ref",
                                              ireceptor_tag, repository_tag,
                                              airr_map.getIRRearrangementClass())
    antigen_ref_file = "ir_antigen_ref"
    antigen_ref_repo = airr_map.getMapping("ir_antigen_ref",
                                              ireceptor_tag, repository_tag,
                                              airr_map.getIRRearrangementClass())
    v_gene_file = "v_gene"
    v_gene_repo = airr_map.getMapping("ir_vgene_gene",
                                              ireceptor_tag, repository_tag,
                                              airr_map.getIRRearrangementClass())
    j_gene_file = "j_gene"
    j_gene_repo = airr_map.getMapping("ir_jgene_gene",
                                              ireceptor_tag, repository_tag,
                                              airr_map.getIRRearrangementClass())
    junction_aa_file = "junction_aa"
    junction_aa_repo = airr_map.getMapping("junction_aa",
                                              ireceptor_tag, repository_tag,
                                              airr_map.getRearrangementClass())
    if v_gene_repo == None:
        v_gene_repo = v_gene_file
    if j_gene_repo == None:
        j_gene_repo = j_gene_file
    if junction_aa_repo == None:
        junction_aa_repo = junction_aa_file

    if reactivity_id_repo == None:
        reactivity_id_repo = reactivity_id_file
    if reactivity_ref_repo == None:
        reactivity_ref_repo = reactivity_ref_file
    if reactivity_method_repo == None:
        reactivity_method_repo = reactivity_method_file
    if epitope_ref_repo == None:
        epitope_ref_repo = epitope_ref_file
    if antigen_ref_repo == None:
        antigen_ref_repo = antigen_ref_file

    if verbose:
        print("Info: reactivity id field = %s"%(reactivity_id_repo))
        print("Info: reactivity field = %s"%(reactivity_ref_repo))
        print("Info: reactivity method field = %s"%(reactivity_method_repo))
        print("Info: epitope field = %s"%(epitope_ref_repo))
        print("Info: antigen field = %s"%(antigen_ref_repo))

    # Keep track of how many writes we make.
    update_count = 0
    warnings = 0
    errors = 0

    # For each rearrangement in the file, we need to set the sequence fields.
    for index, reactivity_data in reactivity_df.iterrows():
        # Get the sequence_id
        sequence_id = reactivity_data['sequence_id']
        
        # Get the sequence from the repository, skip this sequence if query
        # fails for some reason and generates an exception. 
        query = {repo_sequence_id_field: {'$eq': sequence_id}}
        try:
            rearrangement_cursor = repository.rearrangement.find(query)
        except Exception as err:
            print("ERROR: Could not find rearrangement with sequence_id %s."%(sequence_id))
            print("ERROR: Error message = %s"%(str(err)))
            errors = errors + 1
            continue

        # We should only get one sequence, as the ID should be unique. Check this.
        rearrangement_data = None
        rearrangement_count = 0
        for rearrangement in rearrangement_cursor:
            # Get the first one
            rearrangement_data = rearrangement
            rearrangement_count = rearrangement_count + 1
            # If we are in the loop more than once, break out. This is an error
            # and we report it as such later.
            if rearrangement_count > 1:
                break
        # If we didn't find the sequence, report an error and continue with the next input.
        if rearrangement_data == None:
            print("ERROR: Could not find rearrangement with sequence_id %s, skipping."%(sequence_id))
            errors = errors + 1
            continue
        # If we found more than one sequence, report an error and continue with the next input.
        if rearrangement_count > 1:
            print("ERROR: Found more than one rearrangement with sequence_id %s, skipping."%(sequence_id))
            errors = errors + 1
            continue

        # Check to see if the junction_aa, the v_gene, and the j_gene match.
        # Recall that v_gene and j_gene in the repository are lists.
        #if not(rearrangement_data[v_gene_repo] == reactivity_data[v_gene_file]):
        if reactivity_data[v_gene_file] not in rearrangement_data[v_gene_repo]:
            print("ERROR: V gene different - %s, %s"%(rearrangement_data[v_gene_repo],reactivity_data[v_gene_file]))
            errors = errors + 1
            continue
        #if not(rearrangement_data[j_gene_repo] == reactivity_data[j_gene_file]):
        if reactivity_data[j_gene_file] not in rearrangement_data[j_gene_repo]:
            print("ERROR: J gene different - %s, %s"%(rearrangement_data[j_gene_repo],reactivity_data[j_gene_file]))
            errors = errors + 1
            continue
        if not(rearrangement_data[junction_aa_repo] == reactivity_data[junction_aa_file]):
            print("ERROR: Junction AA different - %s, %s"%(rearrangement_data[junction_aa_repo],reactivity_data[junction_aa_file]))
            errors = errors + 1
            continue

        # We now have a single rearrangement, for which we are going to update the data.
        now_str = rearrangementParser.getDateTimeNowUTC()
        # If we are in append mode, add the new data to the existing data. If not
        # replace the old data with the new data.
        if append:
            print("Info: Appending data for sequence_id %s."%(sequence_id))
            if not reactivity_method_repo in rearrangement_data:
                reactivity_method = []
            else:
                reactivity_method = rearrangement_data[reactivity_method_repo]

            if not reactivity_ref_repo in rearrangement_data:
                reactivity_ref = []
            else:
                reactivity_ref = rearrangement_data[reactivity_ref_repo]

            if not epitope_ref_repo in rearrangement_data:
                epitope_ref = []
            else:
                epitope_ref = rearrangement_data[epitope_ref_repo]

            if not antigen_ref_repo in rearrangement_data:
                antigen_ref = []
            else:
                antigen_ref = rearrangement_data[antigen_ref_repo]

            if not (len(reactivity_method) == len(reactivity_ref) and len(reactivity_ref) == len(epitope_ref) and len(epitope_ref) == len(antigen_ref)):
                print("Warning: Reactivity records not of equal length for sequence_id %s."%(sequence_id))
                warnings = warnings + 1
            
            reactivity_method.extend([reactivity_data[reactivity_method_file]])
            reactivity_ref.extend([json.loads(reactivity_data[reactivity_ref_file])])
            epitope_ref.extend([json.loads(reactivity_data[epitope_ref_file])])
            antigen_ref.extend([json.loads(reactivity_data[antigen_ref_file])])

            update_obj = {"$set": {
                reactivity_method_repo:reactivity_method,
                reactivity_ref_repo:reactivity_ref,
                epitope_ref_repo:epitope_ref,
                antigen_ref_repo:antigen_ref,
                updated_at_field:now_str}
            }
        else:
            print("Info: Loading data for sequence_id %s."%(sequence_id))
            if reactivity_data[reactivity_method_file] == "":
                reactivity_method = []
            else:
                reactivity_method = [reactivity_data[reactivity_method_file]]

            if reactivity_data[reactivity_ref_file] == "":
                reactivity_ref = []
            else:
                reactivity_ref = [json.loads(reactivity_data[reactivity_ref_file])]

            if reactivity_data[epitope_ref_file] == "":
                epitope_ref = []
            else:
                epitope_ref = [json.loads(reactivity_data[epitope_ref_file])]

            if reactivity_data[antigen_ref_file] == "":
                antigen_ref = []
            else:
                antigen_ref = [json.loads(reactivity_data[antigen_ref_file])]

            update_obj = {"$set": {
                #reactivity_method_repo:[json.loads(reactivity_data[reactivity_method_file])],
                #reactivity_ref_repo:[json.loads(reactivity_data[reactivity_ref_file])],
                #epitope_ref_repo:[json.loads(reactivity_data[epitope_ref_file])],
                #antigen_ref_repo:[json.loads(reactivity_data[antigen_ref_file])],
                reactivity_method_repo:reactivity_method,
                reactivity_ref_repo:reactivity_ref,
                epitope_ref_repo:epitope_ref,
                antigen_ref_repo:antigen_ref,
                updated_at_field:now_str}
            }

        # Do the update
        if not skipload:
            repository.rearrangement.update_one( {repo_sequence_id_field:sequence_id}, update_obj)
            update_count = update_count + 1
        #rearrangement_data[reactivity_ref_repo] = reactivity_data[reactivity_ref_file]
        #rearrangement_data[epitope_ref_repo] = reactivity_data[epitope_ref_file]
        #rearrangement_data[antigen_ref_repo] = reactivity_data[antigen_ref_file]
        

    # time end
    print("Info: %d rearrangement database updates made"%(update_count))
    t_end = time.perf_counter()
    print("Info: Finished processing in %f seconds (%f updates/s)"%(
           (t_end - t_start),(update_count/(t_end-t_start))),flush=True)
    print("Info: Total update time = %f seconds (%.2f%% of total)"%
           (t_update_total,t_update_total/(t_end - t_start)*100.0))
    print("Info: Number of errors = %d"%(errors))
    print("Info: Number of warnings = %d"%(warnings))
    return True

if __name__ == "__main__":
    # Get the command line arguments.
    options = getArguments()

    # Create the repository object, which establishes the repository connection.
    repository = Repository(options.user, options.password,
                            options.host, options.port,
                            options.database,
                            options.repertoire_collection,
                            options.rearrangement_collection,
                            options.clone_collection,
                            options.cell_collection,
                            options.expression_collection,
                            options.receptor_collection,
                            options.reactivity_collection,
                            options.skipload, False,
                            options.verbose)
    # Check on the successful creation of the repository
    if repository is None or not repository:
        sys.exit(1)

    # Create the AIRR mapping object, which has the mapping of fields between
    # the various components. This is essentially a mapping between the AIRR
    # standard fields, the fields in the input file being parsed, and the fields
    # that are stored in the repository.
    airr_map = AIRRMap(options.verbose)
    airr_map.readMapFile(options.mapfile)
    if airr_map.getRearrangementMapColumn(options.database_map) is None:
        print("ERROR: Could not find repository mapping %s in AIRR Mappings"%
              (options.database_map))
        sys.exit(1)

    # Create parser objects. We don't actually parse these data, but we use the
    # objects to ensure we use the iReceptor fields in these objects correctly
    rearrangementParser = Rearrangement(options.verbose, options.database_map,
                                        options.database_chunk, airr_map, repository)
    # Get a time point so we can record timing for the update.
    t_total_start = time.perf_counter()
    # Open the gene map file - it has two columns, the gene name to replace and the
    # gene to use as a replacement. 
    try:
        reactivity_df = pd.read_csv(options.reactivity_file, keep_default_na=False, sep='\t')
    except:
        print("Info: Could not open reactivity file %s"%(options.reactivity_file))
        sys.exit(1)

    # Process the rearrangements in the reactivity file.
    processRearrangements(reactivity_df, repository, airr_map, rearrangementParser,
                          options.append, options.verbose, options.skipload)

    # Output timing
    t_total_end = time.perf_counter()
    print("Info: Finished total processing in {:.2f} mins".format((t_total_end - t_total_start) / 60))
    # Check final results and exit. If all are True we are good, otherwise give a ERROR message.
    if True:
        sys.exit(0)
    else:
        print('ERROR: one or more conversions failed.')
        sys.exit(1)
