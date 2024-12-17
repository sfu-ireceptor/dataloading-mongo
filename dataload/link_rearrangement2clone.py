#! /opt/ireceptor/data/bin/python
"""
 link_rearrangement2clone.py is a script to link clone_ids in the
 Rearrangement collection to the unique Clone id for a Clone in the
 Clone collection. This linking is based on files (as specified at load
 time) to identify the correct rearrangements and clones in question. It uses
 the tool annotation clone ID to make the original link and replaces
 clone_id in the Rearrangement collection with the appropriate unique
 clone_id in the repository.
"""
import os
import argparse
import time
import sys
import pandas as pd

# AIRR Mapping class.
from airr_map import AIRRMap
# Repository class - hides the DB implementation
from repository import Repository
# Rearrangement loader classes
from rearrangement import Rearrangement
# Clone loader classes
from clone import Clone

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
        "--update",
        action="store_true",
        help="Run the program in update mode rather than insert mode. This only works for repertoires.")

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


    path_group = parser.add_argument_group("file options")
    parser.add_argument(
        "file_map",
        help="File that contains two columns with headers, first column is a Rearrangement file name used in data loading, the second is a Clone file name used in data loading where the clone_id from the Rearrangement can be looked up in the Clone collection of the repository."
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
        print('FILE               :', options.file_map)

    return options

def processRearrangements(rearrangement_file, clone_file, repository, airr_map,
                          rearrangementParser, cloneParser, options):
    print('Info:', flush=True)
    print('Info: Processing - Rearrangement file = %s, Clone file = %s'%(rearrangement_file, clone_file), flush=True)
    # Start timing the processing
    t_start = time.perf_counter()

    # Set the tag for the repository that we are using.
    repository_tag = rearrangementParser.getRepositoryTag()

    # Get the tag to use for iReceptor specific mappings
    ireceptor_tag = rearrangementParser.getiReceptorTag()

    # Get the fields to use for finding repertoire IDs, either using those IDs
    # directly or by looking for a repertoire ID based on a annotation file
    # name.
    repertoire_link_field = rearrangementParser.getRepertoireLinkIDField()
    repertoire_file_field = rearrangementParser.getRepertoireFileField()

    # Get the sample ID of the data we are processing. We use the file name for
    # this at the moment, but this may not be the most robust method.
    file_field = airr_map.getMapping(repertoire_file_field,
                                     ireceptor_tag, repository_tag)
    print("Info: repertoire file field = %s"%(file_field), flush=True)
    print("Info: repertoire link field = %s"%(repertoire_link_field), flush=True)

    # Get the list of repertoires that are associated with the Rearrangement file. There
    # should only be one, if more than on this is an error.
    repertoires = repository.getRepertoires(file_field, rearrangement_file)
    if not len(repertoires) == 1:
        print("ERROR: Could not find unique repertoire for file %s"%(rearrangement_file), flush=True)
        return False
    repertoire = repertoires[0]

    # Check to make sure we have the link field that links repertoire and rearrangement
    # data in the repertoire object. If so, get the link ID that we use to link to
    # the rearrangements for this file. This is what we use to look up rearrangements
    if not repertoire_link_field in repertoire:
        print("ERROR: Could not find Rearrangement link field %s"%(repertoire_link_field), flush=True)
        return False
    rearrangement_link_id = repertoire[repertoire_link_field]

    # Get the list of repertoire that are associated with the Clone file. Again, there should
    # only be one.
    repertoires = repository.getRepertoires(file_field, clone_file)
    if not len(repertoires) == 1:
        print("ERROR: Could not find unique repertoire for file %s"%(clone_file), flush=True)
        return False
    repertoire = repertoires[0]

    # Check to make sure we have a link field from the repertoire, and if we do get it.
    # This is what we use to look up Clones.
    if not repertoire_link_field in repertoire:
        print("ERROR: Could not find Clone link field %s"%(repertoire_link_field), flush=True)
        return False
    clone_link_id = repertoire[repertoire_link_field]
    
    # Get the related link fields for the Rearrangement and Clone collections
    rearrangement_link_field = rearrangementParser.annotation_linkid_field 
    clone_link_field = cloneParser.annotation_linkid_field

    # Get the counts for these fields and output some info.
    rearrangement_count = repository.countRearrangements(rearrangement_link_field,
                                                         rearrangement_link_id)
    clone_count = repository.countClones(clone_link_field, clone_link_id)

    print("Info: rearrangement link id = %s (%d)"%(rearrangement_link_id, rearrangement_count ), flush=True)
    print("Info: clone link id = %s (%d)"%(clone_link_id,clone_count), flush=True)
    
    # Get the field names for the AIRR field (which is our unique ID) and the annotation tool field
    # which we use to find relevant clones from the rearrangements (typically a barcode).
    airr_clone_field = airr_map.getMapping("clone_id_clone",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getCloneClass())
    tool_clone_field = airr_map.getMapping("ir_clone_id_clone",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getIRCloneClass())

    # Create a dictionary, indexed by the annotation tools clone_id field. This is typically
    # a barcode. We want the dictionary keyed on barcode, because we are going to look up clone
    # barcodes for each rearrangement.
    clone_id_dict = dict()
    # Create a dictionary keyed on unique repository clone id to keep track of sequences for each
    # clone
    clone_seq_dict = dict()

    # Execute the query to find all clones in the Clone collection that are from the clone file
    # provided. Note this DOES NOT look at the file, it looks in the database to find all Clones
    # that are associated with the file.
    query = {clone_link_field: {'$eq': clone_link_id}}
    clone_cursor = repository.clone.find(query)
    # For each clone
    for clone in clone_cursor:
        # For each clone (keyed by the barcode), keep track of the repository clone_id (which
        # is unique to the repository and a list of sequences related to that clone (empty for now).
        clone_id_dict[clone[tool_clone_field]] = clone[airr_clone_field]
        clone_seq_dict[clone[airr_clone_field]] = []
        #print("Info:     %s = %s"%(clone[tool_clone_field],clone[airr_clone_field]))

    print("Info: Clones found = %d (%s)"%(len(clone_id_dict), clone_count), flush=True)
    print("Info: Rearrangements found = %d"%(rearrangement_count), flush=True)

    # Get the field names for the AIRR field (which we overwrite) and the annotation tool field
    # which we preserve.
    airr_sequence_id_field = airr_map.getMapping("rearrangement_id",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getRearrangementClass())
    airr_clone_id_field = airr_map.getMapping("clone_id",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getRearrangementClass())
    tool_clone_field = airr_map.getMapping("ir_clone_id_rearrangement",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getIRRearrangementClass())
    # Get the field in the repository that is used to store data update time
    updated_at_field = airr_map.getMapping("ir_updated_at_rearrangement",
                                           ireceptor_tag,
                                           repository_tag)

    print("Info: Looking up %s in Clone, setting %s in Rearrangement"%(
           tool_clone_field, airr_clone_id_field), flush=True)

    # Execute the query to find all rearrangemetns in the Rearrangement collection that are
    # associated with the rearrangement link ID (associated with the file). Note this DOES NOT
    # look at the file, it looks in the database to find all Rearrangements that are
    # associated with the file.
    query = {rearrangement_link_field: {'$eq': rearrangement_link_id}}
    rearrangement_cursor = repository.rearrangement.find(query)
    # Keep track of the number of updates as we iterate over the cursor.
    update_count = 0
    for rearrangement in rearrangement_cursor:
        #print("Info:     %s,%s,%s"%(
        #        rearrangement[airr_sequence_id_field],
        #        rearrangement[tool_clone_field],
        #        rearrangement[airr_clone_id_field]))
        # Sequence ID - needed to update this sequence in the repository
        this_sequence_id = rearrangement[airr_sequence_id_field]
        # Get the AIRR clone ID field, this is the field we overwrite.
        this_clone_id = rearrangement[airr_clone_id_field]
        # The clone dictionary is keyed on the tool clone ID, which is not unique in the DB.
        # If the rearrangement has a tool clone ID in the DB unique clone ID field, we are not
        # unique. If so we need to update the clone ID field in the rearrangement collection
        # with the unique clone id field which is in the dictionary. 
        if this_clone_id in clone_id_dict:
            # Get the Clone collection unique ID from the dictionary.
            repository_clone_id = clone_id_dict[this_clone_id]
            # Set the rearrangement clone_id to be the unqique clone_id from the Clone object.
            repository.updateRearrangementField(airr_sequence_id_field, this_sequence_id,
                                                airr_clone_id_field, repository_clone_id,
                                                updated_at_field)
            # Add the sequence ID to the clone list of rearrangements.
            clone_seq_dict[repository_clone_id].append(this_sequence_id)
            # Update our count.
            update_count = update_count + 1
        else:
            # In this case we can't find the rearrangement clone ID in the dictionary. Why?
            if this_clone_id in clone_id_dict.values():
                # Check whether the dictionary contains this_clone_id in its values. If it does,
                # then it is likely that the rearrangement clone_id has already been set to be
                # the repository unique clone_id.
                print("Warning: Clone id for sequence %s already set (clone_id = %s)"%(this_sequence_id,this_clone_id), flush=True)
            else:
                # If nothing then we could not find a clone for a sequence, print a warning.
                print("Warning: Could not find a Clone for sequence %s (%s)"%(this_sequence_id,rearrangement['v_call']), flush=True)
    # If we want to store rearrangement object in the Clone collection, we can do so by looping
    # over the sequence dictionary, but we need to check what is there, append, and make unique
    # so we don't have any duplicates. Not necessary so leaving out for now.
    #for repository_clone_id, sequence_list in clone_seq_dict.items():
    #    print("Info: %s %s"%(repository_clone_id,sequence_list))



    # time end
    print("Info: Update of %d rearrangements (%.2f%%)"%(update_count, (update_count/rearrangement_count)*100.0), flush=True)
    t_end = time.perf_counter()
    print("Info: Finished processing in %f seconds (%f updates/s)"%(
           (t_end - t_start),(update_count/(t_end-t_start))),flush=True)
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
                            options.clone_collection,
                            options.expression_collection,
                            options.receptor_collection,
                            options.reactivity_collection,
                            options.skipload, options.update,
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
    cloneParser = Clone(options.verbose, options.database_map,
                      options.database_chunk, airr_map, repository)

    t_total_start = time.perf_counter()

    # Open the file map - it has two columns, one for Rearrangement files and one for Clone files.
    files_df = pd.read_csv(options.file_map, sep='\t')
    if not 'Rearrangement' in files_df.columns or not 'Clone' in files_df.columns:
        print("ERROR: Could not find 'Rearrangement' or 'Clone' column in file %s"%
                (options.file_map))
        sys.exit(1)
    # For each row, call processRearrangements with two file names along with the other required
    # objects (repository, airr_map, and rearrangement and clone parsers. We get back a list with
    # True or False for each row processed.
    result_list = [processRearrangements(rearrangement_file, clone_file, repository, airr_map, rearrangementParser, cloneParser, options) for rearrangement_file, clone_file in zip(files_df['Rearrangement'], files_df['Clone'])]

    # Output timing
    t_total_end = time.perf_counter()
    print("Info: Finished total processing in {:.2f} mins".format((t_total_end - t_total_start) / 60))
    # Check final results and exit. If all are True we are good, otherwise give a ERROR message.
    if all(result_list):
        sys.exit(0)
    else:
        print('ERROR: one or more conversions failed.')
        sys.exit(1)
