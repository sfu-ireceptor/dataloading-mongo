#! /opt/ireceptor/data/bin/python
"""
 ireceptor_data_loader.py is a batch loading script for loading
 iReceptor repertoire metadata and sequence rearrangement annotations
 
"""
import os
import argparse
import time
import sys

# AIRR Mapping class.
from airr_map import AIRRMap
# Repository class - hides the DB implementation
from repository import Repository
# Rearrangement loader classes
from rearrangement import Rearrangement
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

    path_group = parser.add_argument_group("file options")
    path_group.add_argument(
        "--rearrangement_file",
        dest="rearrangement_file",
        default="",
        help="Name of the Rearrangement file to use for linking."
    )
    path_group.add_argument(
        "--cell_file",
        dest="cell_file",
        default="",
        help="Name of the Cell file to use for linking."
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
        print('REARRANGEMENT_NAME :', options.rearrangement_file)
        print('CELL_NAME          :', options.rearrangement_file)

    return options

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

    
    # Start timing the processing
    t_start = time.perf_counter()

    # Create parser objects. We don't actually parse these data, but we use the
    # objects to ensure we use the iReceptor fields in these objects correctly
    rearrangementParser = Rearrangement(options.verbose, options.database_map,
                                        options.database_chunk, airr_map, repository)
    cellParser = Cell(options.verbose, options.database_map,
                      options.database_chunk, airr_map, repository)
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
    print("Info: repertoire file field = %s"%(str(file_field)))
    print("Info: link field = %s"%(repertoire_link_field))

    # Get the list of repertoires that are associated with the Rearrangement file. There
    # should only be one, if more than on this is an error.
    repertoires = repository.getRepertoires(file_field, options.rearrangement_file)
    if not len(repertoires) == 1:
        print("ERROR: Could not find unique repertoire for file %s"%(options.rearrangement_file))
        sys.exit(1)
    repertoire = repertoires[0]

    # Check to make sure we have the link field that links repertoire and rearrangement
    # data in the repertoire object. If so, get the link ID that we use to link to
    # the rearrangements for this file. This is what we use to look up rearrangements
    if not repertoire_link_field in repertoire:
        print("ERROR: Could not find Rearrangement link field %s"%(repertoire_link_field))
        sys.exit(1)
    rearrangement_link_id = repertoire[repertoire_link_field]

    # Get the list of repertoire that are associated with the Cell file. Again, there should
    # onlybe one.
    repertoires = repository.getRepertoires(file_field, options.cell_file)
    if not len(repertoires) == 1:
        print("ERROR: Could not find unique repertoire for file %s"%(options.cell_file))
        sys.exit(1)
    repertoire = repertoires[0]

    # Check to make sure we have a link field from the repertoire, and if we do get it.
    # This is what we use to look up Cells.
    if not repertoire_link_field in repertoire:
        print("ERROR: Could not find Cell link field %s"%(repertoire_link_field))
        sys.exit(1)
    cell_link_id = repertoire[repertoire_link_field]
    
    # Get the related link fields for the Rearrangement and Cell collections
    rearrangement_link_field = rearrangementParser.annotation_linkid_field 
    cell_link_field = cellParser.annotation_linkid_field

    # Get the counts for these fields and output some info.
    rearrangement_count = repository.countRearrangements(rearrangement_link_field,
                                                         rearrangement_link_id)
    cell_count = repository.countCells(cell_link_field, cell_link_id)
    print("Info: rearrangement link id = %s (%d)"%(rearrangement_link_id, rearrangement_count ))
    print("Info: cell link id = %s (%d)"%(cell_link_id,cell_count))
    
    # Get the field names for the AIRR field (which is our unique ID) and the annotation tool field
    # which we use to find relevant cells from the rearrangements (typically a barcode).
    airr_cell_field = airr_map.getMapping("cell_id_cell",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getCellClass())
    tool_cell_field = airr_map.getMapping("ir_cell_id_cell",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getIRCellClass())

    # Create a dictionary, indexed by the annotation tools cell_id field. This is typically
    # a barcode. We want the dictionary keyed on barcode, because we are going to look up cell
    # barcodes for each rearrangement.
    cell_id_dict = dict()
    # Create a dictionary keyed on unique repository cell id to keep track of sequences for each
    # cell
    cell_seq_dict = dict()

    # Execute the query to find all cells in the Cell collection that are from the cell file
    # provided. Note this DOES NOT look at the file, it looks in the database to find all Cells
    # that are associated with the file.
    query = {cell_link_field: {'$eq': cell_link_id}}
    cell_cursor = repository.cell.find(query)
    # For each cell
    for cell in cell_cursor:
        # For each cell (keyed by the barcode), keep track of the repository cell_id (which
        # is unique to the repository and a list of sequences related to that cell (empty for now).
        cell_id_dict[cell[tool_cell_field]] = cell[airr_cell_field]
        cell_seq_dict[cell[airr_cell_field]] = []
        #print("Info:     %s = %s"%(cell[tool_cell_field],cell[airr_cell_field]))

    print("Info: Cells found = %d (%s)"%(len(cell_id_dict), cell_count))
    print("Info: Rearrangements found = %d"%(rearrangement_count))

    # Get the field names for the AIRR field (which we overwrite) and the annotation tool field
    # which we preserve.
    airr_sequence_id_field = airr_map.getMapping("rearrangement_id",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getRearrangementClass())
    airr_cell_id_field = airr_map.getMapping("cell_id",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getRearrangementClass())
    tool_cell_field = airr_map.getMapping("ir_cell_id_rearrangement",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getIRRearrangementClass())
    print("Info: Looking up %s in Cell, setting %s in Rearrangement"%(
           tool_cell_field, airr_cell_id_field))

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
        #        rearrangement[tool_cell_field],
        #        rearrangement[airr_cell_id_field]))
        # Sequence ID - needed to update this sequence in the repository
        this_sequence_id = rearrangement[airr_sequence_id_field]
        # Get the AIRR cell ID field, this is the field we overwrite.
        this_cell_id = rearrangement[airr_cell_id_field]
        # The cell dictionary is keyed on the tool cell ID, which is not unique in the DB.
        # If the rearrangement has a tool cell ID in the DB unique cell ID field, we are not
        # unique. If so we need to update the cell ID field in the rearrangement collection
        # with the unique cell id field which is in the dictionary. 
        if this_cell_id in cell_id_dict:
            # Get the Cell collection unique ID from the dictionary.
            repository_cell_id = cell_id_dict[this_cell_id]
            # Set the rearrangement cell_id to be the unqique cell_id from the Cell object.
            repository.updateRearrangementField(airr_sequence_id_field, this_sequence_id,
                                                airr_cell_id_field, repository_cell_id)
            # Add the sequence ID to the cell list of rearrangements.
            cell_seq_dict[repository_cell_id].append(this_sequence_id)
            # Update our count.
            update_count = update_count + 1
        else:
            # In this case we can't find the rearrangement cell ID in the dictionary. Why?
            if this_cell_id in cell_id_dict.values():
                # Check whether the dictionary contains this_cell_id in its values. If it does,
                # then it is likely that the rearrangement cell_id has already been set to be
                # the repository unique cell_id.
                print("Warning: Cell id for sequence %s already set (cell_id = %s)"%(this_sequence_id,this_cell_id))
            else:
                # If nothing then we could not find a cell for a sequence, print a warning.
                print("Warning: Could not find a Cell for sequence %s"%(this_sequence_id))
    # If we want to store rearrangement object in the Cell collection, we can do so by looping
    # over the sequence dictionary, but we need to check what is there, append, and make unique
    # so we don't have any duplicates. Not necessary so leaving out for now.
    #for repository_cell_id, sequence_list in cell_seq_dict.items():
    #    print("Info: %s %s"%(repository_cell_id,sequence_list))



    # time end
    t_end = time.perf_counter()
    print("Info: Update of %d rearrangements"%(update_count))
    print("Info: Finished processing in {:.2f} mins".format((t_end - t_start) / 60))
    sys.exit(0)
