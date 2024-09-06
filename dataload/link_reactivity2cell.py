#! /opt/ireceptor/data/bin/python
"""
 link_reactivity2cell.py is a script to link cell_ids in the
 Reactivity collection to the unique Cell id for a Cell in the
 Cell collection. This linking is based on files (as specified at load
 time) to identify the correct reactivity and cells in question. It uses
 the tool annotation cell ID to make the original link and replaces
 cell_id in the Reactivity collection with the appropriate unique
 cell_id in the repository.
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
# Reactivity loader classes
from airr_reactivity import AIRR_Reactivity
# Cell loader classes
from cell import Cell

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
        help="The collection to use for storing and searching receptor. This is the collection that data is inserted into when the --airr-receptor option is used to load files. Defaults to 'receptor', which is the collection in the iReceptor Turnkey repository."
    )
    db_group.add_argument(
        "--reactivity_collection",
        dest="reactivity_collection",
        default="reactivity",
        help="The collection to use for storing and searching reactivity. This is the collection that data is inserted into when the --airr-reactivity option is used to load files. Defaults to 'reactivity', which is the collection in the iReceptor Turnkey repository."
    )

    path_group = parser.add_argument_group("file options")
    parser.add_argument(
        "file_map",
        help="File that contains two columns with headers, first column is a Reactivity file name used in data loading, the second is a cell file name used in data loading where the cell_id from the Reactivity collection can be looked up in the Cell collection of the repository."
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

def processReactivity(reactivity_file, cell_file, repository, airr_map,
               reactivityParser, cellParser, options):
    print('Info:',flush=True)
    print('Info: Processing - Reactivity file = %s, Cell file = %s'%(reactivity_file, cell_file),flush=True)
    # Start timing the processing
    t_start = time.perf_counter()

    # Set the tag for the repository that we are using.
    repository_tag = reactivityParser.getRepositoryTag()

    # Get the tag to use for iReceptor specific mappings
    ireceptor_tag = reactivityParser.getiReceptorTag()

    # Get the fields to use for finding repertoire IDs, either using those IDs
    # directly or by looking for a repertoire ID based on a annotation file
    # name.
    repertoire_link_field = reactivityParser.getRepertoireLinkIDField()
    repertoire_file_field = reactivityParser.getRepertoireFileField()

    # Get the sample ID of the data we are processing. We use the file name for
    # this at the moment, but this may not be the most robust method.
    file_field = airr_map.getMapping(repertoire_file_field,
                                     ireceptor_tag, repository_tag)
    print("Info: repertoire file field = %s"%(file_field), flush=True)
    print("Info: repertoire link field = %s"%(repertoire_link_field), flush=True)

    # Get the list of repertoires that are associated with the Reactivity file. There
    # should only be one, if more than on this is an error.
    repertoires = repository.getRepertoires(file_field, reactivity_file)
    if not len(repertoires) == 1:
        print("ERROR: Could not find unique repertoire for file %s"%(reactivity_file), flush=True)
        return False
    repertoire = repertoires[0]

    # Check to make sure we have the link field that links repertoire and reactivity
    # data in the repertoire object. If so, get the link ID that we use to link to
    # the Reactivity for this file. This is what we use to look up Reactivity
    if not repertoire_link_field in repertoire:
        print("ERROR: Could not find Reactivity link field %s"%(repertoire_link_field), flush=True)
        return False
    reactivity_link_id = repertoire[repertoire_link_field]

    # Get the list of repertoire that are associated with the Cell file. Again, there should
    # only be one.
    repertoires = repository.getRepertoires(file_field, cell_file)
    if not len(repertoires) == 1:
        print("ERROR: Could not find unique repertoire for file %s"%(cell_file), flush=True)
        return False
    repertoire = repertoires[0]

    # Check to make sure we have a link field from the repertoire, and if we do get it.
    # This is what we use to look up Cells.
    if not repertoire_link_field in repertoire:
        print("ERROR: Could not find Cell link field %s"%(repertoire_link_field), flush=True)
        return False
    cell_link_id = repertoire[repertoire_link_field]
    
    # Get the related link fields for the Reactivity and Cell collections
    reactivity_link_field = reactivityParser.annotation_linkid_field 
    cell_link_field = cellParser.annotation_linkid_field

    # Get the counts for these fields and output some info.
    reactivity_count = repository.countReactivity(reactivity_link_field, reactivity_link_id)
    cell_count = repository.countCells(cell_link_field, cell_link_id)
    print("Info: reactivity link field = %s"%(reactivity_link_field), flush=True)
    print("Info: reactivity link id = %s (%d)"%(reactivity_link_id, reactivity_count ), flush=True)
    print("Info: cell link field = %s"%(cell_link_field), flush=True)
    print("Info: cell link id = %s (%d)"%(cell_link_id,cell_count), flush=True)
    
    # Get the field names for the AIRR field (which is our unique ID) and the annotation tool field
    # which we use to find relevant cells from the reactivity (typically a barcode).
    airr_cell_field = airr_map.getMapping("cell_id_cell",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getCellClass())
    tool_cell_field = airr_map.getMapping("ir_cell_id_cell",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getIRCellClass())

    # Create a dictionary, indexed by the annotation tools cell_id field. This is typically
    # a barcode. We want the dictionary keyed on barcode, because we are going to look up cell
    # barcodes for each reactivity.
    cell_id_dict = dict()

    # Execute the query to find all cells in the Cell collection that are from the cell file
    # provided. Note this DOES NOT look at the file, it looks in the database to find all Cells
    # that are associated with the file.
    query = {cell_link_field: {'$eq': cell_link_id}}
    cell_cursor = repository.cell.find(query)
    # For each cell
    cell_duplicates = 0
    for cell in cell_cursor:
        # For each cell (keyed by the barcode), keep track of the repository cell_id (which
        # is unique to the repository 
        if cell[tool_cell_field] in cell_id_dict:
            print("Warning: cell %s already in dictionary"%(
                  cell[tool_cell_field]), flush=True)
            cell_duplicates = cell_duplicates + 1
        cell_id_dict[cell[tool_cell_field]] = cell[airr_cell_field]
        #print("Info:     %s = %s"%(cell[tool_cell_field],cell[airr_cell_field]))

    print("Info: Cells found = %d, unique = %d, duplicates = %d"%(
           cell_count, len(cell_id_dict), cell_duplicates), flush=True)
    print("Info: Reactivities found = %d"%(reactivity_count), flush=True)

    # Get the field names for the AIRR field (which we overwrite) and the annotation tool field
    # which we preserve.
    airr_reactivity_id_field = airr_map.getMapping("_id",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getReactivityClass())
    airr_reactivity_id_field = '_id'
    airr_cell_id_field = airr_map.getMapping("cell_id_reactivity",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getReactivityClass())
    tool_cell_field = airr_map.getMapping("ir_cell_id_reactivity",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getIRReactivityClass())
    print("Info: Looking up %s in Cell, setting %s, %s in Reactivity"%(
           tool_cell_field, airr_cell_id_field, tool_cell_field), flush=True)

    # Execute the query to find all Reactivity in the Reactivity collection that are
    # associated with the reactivity link ID (associated with the file). Note this DOES NOT
    # look at the file, it looks in the database to find all Reactivity that are
    # associated with the file.
    query = {reactivity_link_field: {'$eq': reactivity_link_id}}
    reactivity_cursor = repository.reactivity.find(query)
    # Keep track of the number of updates as we iterate over the cursor.
    update_count = 0
    for reactivity in reactivity_cursor:
        #print("Info:     %s,%s,%s"%(
        #        reactivity[airr_reactivity_id_field],
        #        reactivity[tool_cell_field],
        #        reactivity[airr_cell_id_field]))
        # Reactivity ID - needed to update this reactivity in the repository
        this_reactivity_id = reactivity[airr_reactivity_id_field]
        # Get the AIRR cell ID, this is the field we overwrite. We want to keep track
        # of it because we want to provide provenance. We store in tool_cell_field.
        tool_cell_id = reactivity[airr_cell_id_field]
        # The cell dictionary is keyed on the tool cell ID, which is not unique in the DB.
        # If the reactivity has a tool cell ID in the DB unique cell ID field, we are not
        # unique. If so we need to update the cell ID field in the reactivity collection
        # with the unique cell id field which is in the dictionary. 
        if tool_cell_id in cell_id_dict:
            # Get the Cell collection unique ID from the dictionary. This is the 
            # the value that we want to store in the reactivity Cell ID field.
            repository_cell_id = cell_id_dict[tool_cell_id]
            # Set the reactivity cell_id to be the unqique cell_id from the Cell object.
            #print("%s %s %s %s %s %s"%(airr_reactivity_id_field,this_reactivity_id,airr_cell_id_field,repository_cell_id, tool_cell_field, tool_cell_id))
            repository.updateReactivityField(airr_reactivity_id_field, this_reactivity_id,
                                             airr_cell_id_field, repository_cell_id)
            repository.updateReactivityField(airr_reactivity_id_field, this_reactivity_id,
                                             tool_cell_field, tool_cell_id)
            # Update our count.
            update_count = update_count + 1
            if update_count % 10000 == 0:
                print("Info: Total records so far = %d (%.2f %%)"%(update_count,(update_count/reactivity_count)*100), flush=True)

        else:
            # In this case we can't find the reactivity cell ID in the dictionary. Why?
            if tool_cell_id in cell_id_dict.values():
                # Check whether the dictionary contains tool_cell_id in its values. If it does,
                # then it is likely that the reactivity cell_id has already been set to be
                # the repository unique cell_id.
                print("Warning: Cell id for Reactivity %s already set (cell_id = %s)"%(
                      this_reactivity_id,tool_cell_id), flush=True)
            else:
                # If nothing then we could not find a cell for a Reactivity, print a warning.
                print("Warning: Could not find a Cell for Reactivity element %s"%(
                      this_reactivity_id), flush=True)

    # time end
    print("Info: Update of %d reactivity records"%(update_count), flush=True)
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
                            options.cell_collection,
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
    if airr_map.getReactivityMapColumn(options.database_map) is None:
        print("ERROR: Could not find repository mapping %s in AIRR Mappings"%
              (options.database_map))
        sys.exit(1)

    # Create parser objects. We don't actually parse these data, but we use the
    # objects to ensure we use the iReceptor fields in these objects correctly
    reactivityParser = AIRR_Reactivity(options.verbose, options.database_map,
                           options.database_chunk, airr_map, repository)
    cellParser = Cell(options.verbose, options.database_map,
                      options.database_chunk, airr_map, repository)

    t_total_start = time.perf_counter()

    # Open the file map - it has two columns, one for Reactivity files and one for Cell files.
    files_df = pd.read_csv(options.file_map, sep='\t')
    if not 'Reactivity' in files_df.columns or not 'Cell' in files_df.columns:
        print("ERROR: Could not find 'Reactivity' or 'Cell' column in file %s"%
                (options.file_map))
        sys.exit(1)

    # For each row, call processReactivity with two file names along with the other required
    # objects (repository, airr_map, and reactivity and cell parsers. We get back a list with
    # True or False for each row processed.
    result_list = [processReactivity(reactivity_file, cell_file, repository, airr_map, reactivityParser, cellParser, options) for reactivity_file, cell_file in zip(files_df['Reactivity'], files_df['Cell'])]

    # Output timing
    t_total_end = time.perf_counter()
    print("Info: Finished total processing in {:.2f} mins".format((t_total_end - t_total_start) / 60))
    # Check final results and exit. If all are True we are good, otherwise give a ERROR message.
    if all(result_list):
        sys.exit(0)
    else:
        print('ERROR: one or more conversions failed.')
        sys.exit()
