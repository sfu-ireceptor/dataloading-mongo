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
# Repertoire loader classes 
from ir_repertoire import IRRepertoire
from airr_repertoire import AIRRRepertoire
# Rearrangement loader classes
from rearrangement import Rearrangement
from imgt import IMGT
from mixcr import MiXCR
from airr_tsv import AIRR_TSV
from adaptive import Adaptive
# Clone loader classes
from mixcr_clone import MiXCR_Clone
from airr_clone import AIRR_Clone
# Cell loader classes
from airr_cell import AIRR_Cell
# Gene Expression loader classes
from airr_expression import AIRR_Expression

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

    
    # Start timing the file loading
    t_start = time.perf_counter()

    parser = Rearrangement(options.verbose, options.database_map, options.database_chunk,
                           airr_map, repository)
    # Set the tag for the repository that we are using. Note this should
    # be refactored so that it is a parameter provided so that we can use
    # multiple repositories.
    repository_tag = parser.getRepositoryTag()

    # Get the tag to use for iReceptor specific mappings
    ireceptor_tag = parser.getiReceptorTag()

    # Get the fields to use for finding repertoire IDs, either using those IDs
    # directly or by looking for a repertoire ID based on a annotation file
    # name.
    repertoire_link_field = parser.getRepertoireLinkIDField()
    repertoire_file_field = parser.getRepertoireFileField()

    # Get the sample ID of the data we are processing. We use the file name for
    # this at the moment, but this may not be the most robust method.
    file_field = airr_map.getMapping(repertoire_file_field,
                                     ireceptor_tag, repository_tag)

    # We get the link id for rearrangement file of interest
    #db.sample.find({data_processing_files:"ERS1-IGH.tsv"},{repertoire_id:1,ir_annotation_set_metadata_id:1})
    #{ "_id" : ObjectId("635af09d3891ed552fe4dc9d"), "repertoire_id" : "PRJCA002413-ERS1-IGH", "ir_annotation_set_metadata_id" : "635af09d3891ed552fe4dc9d" }

    #rearrangement_repertoire_info = parser.getRepertoireInfo(options.rearrangement_file)
    #cell_repertoire_info = parser.getRepertoireInfo(options.cell_file)
    #print("Info: rearrangement info = %s"%(str(rearrangement_repertoire_info)))
    #print("Info: cell info = %s"%(str(cell_repertoire_info)))
    print("Info: repertoire file field = %s"%(str(file_field)))
    print("Info: link field = %s"%(repertoire_link_field))

    repertoires = repository.getRepertoires(file_field, options.rearrangement_file)
    if not len(repertoires) == 1:
        print("ERROR: Could not find unique repertoire for file %s"%(options.rearrangement_file))
        sys.exit(1)
    repertoire = repertoires[0]
    if not repertoire_link_field in repertoire:
        print("ERROR: Could not find Rearrangement link field %s"%(repertoire_link_field))
        sys.exit(1)
    rearrangement_link_id = repertoire[repertoire_link_field]

    #cell_repertoire_info = parser.getRepertoireInfo(options.cell_file)
    #print("Info: cell info = %s"%(str(cell_repertoire_info)))

    #sys.exit(0)

    # We get the link id for rearrangement file of interest
    #db.sample.find({data_processing_files:"ERS1-vdj_b-cells.json"},{repertoire_id:1,ir_annotation_set_metadata_id:1})
    repertoires = repository.getRepertoires(file_field, options.cell_file)
    if not len(repertoires) == 1:
        print("ERROR: Could not find unique repertoire for file %s"%(options.cell_file))
        sys.exit(1)
    repertoire = repertoires[0]
    if not repertoire_link_field in repertoire:
        print("ERROR: Could not find Cell link field %s"%(repertoire_link_field))
        sys.exit(1)
    cell_link_id = repertoire[repertoire_link_field]
    cell_link_field = 'ir_annotation_set_metadata_id_cell'
    print("Info: rearrangement link id = %s (%d)"%(rearrangement_link_id, repository.countRearrangements(parser.annotation_linkid_field,rearrangement_link_id)))
    print("Info: cell link id = %s (%d)"%(str(cell_link_id),repository.countCells(cell_link_field,cell_link_id)))
    
    cell_dictionary = dict()
    query = {cell_link_field: {'$eq': cell_link_id}}
    cell_cursor = repository.cell.find(query)
    for cell in cell_cursor:
        cell_dictionary[cell['adc_annotation_cell_id']] = {"repo_cell_id":cell['cell_id'],"sequences":[]}
        #print("Info:     %s = %s"%(cell['adc_annotation_cell_id'],cell['cell_id']))

    print("Info: Cells found = %d"%(len(cell_dictionary)))

    # Get the number of rearrangements to process:
    #db.sequence.find({ir_annotation_set_metadata_id_rearrangement:"635af09d3891ed552fe4dc9d"}).count()
    # Get all the sequences: 
    #db.sequence.find({ir_annotation_set_metadata_id_rearrangement:"635af09d3891ed552fe4dc9d"})
    query = {parser.annotation_linkid_field: {'$eq': rearrangement_link_id}}
    rearrangement_cursor = repository.rearrangement.find(query)
    update_count = 0
    for rearrangement in rearrangement_cursor:
        #print("Info:     %s,%s,%s,%s"%(
                #rearrangement['sequence_id'],
                #rearrangement['adc_annotation_cell_id'],
                #rearrangement['cell_id'],
                #cell_dictionary[rearrangement['cell_id']]))
        this_sequence_id = rearrangement['sequence_id']
        this_cell_id = rearrangement['cell_id']
        if this_cell_id in cell_dictionary:
            cell_dict = cell_dictionary[this_cell_id]
            repository_cell_id = cell_dict['repo_cell_id']
            repository.updateRearrangementField('sequence_id',this_sequence_id,
                                                'cell_id',repository_cell_id)
            update_count = update_count + 1
        else:
            cell_info_array = cell_dictionary.values()
            cell_updated_info = list(filter(lambda cell_info: cell_info["repo_cell_id"] == this_cell_id, cell_info_array))
            #print("This cell id = %s"%(this_cell_id))
            #print("Cell updated info = %s"%(str(cell_updated_info)))
            if len(cell_updated_info) > 0:
                print("Warning: Cell id for sequence %s already set (cell_id = %s)"%(this_sequence_id,this_cell_id))
            else:
                print("Warning: Could not find a Cell for sequence %s"%(this_sequence_id))


    print("Info: Update of %d rearrangements"%(update_count))
    sys.exit(1)
    rearrangements = repository.getRearrangements(rearrangement_set_id)
    for rearrangement in rearrangements:
        rearrangement_id = rearrangement['sequence_id']
        rearrangement_cell_id = rearrangement['cell_id']
        if rearrangement_cell_id in cell_dictionary:
            repository.updateRearrangementField('sequence_id',rearrangement_id,
                                                'cell_id', cell_dictionary[rearrangement_cell_id])
    # We are done!!!
    # time end
    t_end = time.perf_counter()
    print("Info: Finished processing in {:.2f} mins".format((t_end - t_start) / 60))
    sys.exit(1)




    # We can only update for Repertoires
    if (options.update and not 
           (options.type == "iReceptor Repertoire" or 
            options.type == "AIRR Repertoire")):
        print("Error: Update is only possible on Repertoire metadata")
        sys.exit(1)

    if options.type == "iReceptor Repertoire":
        # process iReceptor Repertoire metadata 
        print("Info: Processing iReceptor repertoire metadata file: {}".format(options.filename))
        parser = IRRepertoire(options.verbose, options.database_map, options.database_chunk,
                              airr_map, repository)
    elif options.type == "AIRR Repertoire":
        # process AIRR Repertoire metadata
        print("Info: Processing AIRR repertoire metadata file: {}".format(options.filename))
        parser = AIRRRepertoire(options.verbose, options.database_map, options.database_chunk,
                                airr_map, repository)
    elif options.type == "IMGT V-Quest":
        # process imgt
        print("Info: Processing IMGT data file: {}".format(options.filename))
        parser = IMGT(options.verbose, options.database_map, options.database_chunk,
                      airr_map, repository)
    elif options.type == "MiXCR":
        # process mixcr
        print("Info: Processing MiXCR data file: {}".format(options.filename))
        parser = MiXCR(options.verbose, options.database_map, options.database_chunk,
                       airr_map, repository)
    elif options.type == "MiXCR-v3":
        # process mixcr
        print("Info: Processing MiXCR data file: {}".format(options.filename))
        parser = MiXCR(options.verbose, options.database_map, options.database_chunk,
                       airr_map, repository)
        parser.setFileMapping("mixcr_v3")
    elif options.type == "AIRR TSV":
        # process AIRR TSV
        print("Info: Processing AIRR TSV annotation data file: ", options.filename)
        parser = AIRR_TSV(options.verbose, options.database_map, options.database_chunk,
                          airr_map, repository)
    elif options.type == "Adaptive":
        # process Adaptive
        print("Info: Processing Adaptive annotation data file: ", options.filename)
        parser = Adaptive(options.verbose, options.database_map, options.database_chunk,
                          airr_map, repository)
    elif options.type == "ir_general":
        # process a general file (non annotation tool specific)
        print("Info: Processing a general TSV annotation data file: ", options.filename)
        parser = AIRR_TSV(options.verbose, options.database_map, options.database_chunk,
                          airr_map, repository)
        # Override the default file mapping that the parser subclass sets. This allows us
        # to map an arbitrary set of fields in a file to the repository. This requires that
        # an ir_general column exists in the AIRR Mapping file.
        parser.setFileMapping(options.type)
    elif options.type == "MiXCR Clone":
        # process mixcr clone data
        print("Info: Processing MiXCR Clone data file: {}".format(options.filename))
        parser = MiXCR_Clone(options.verbose, options.database_map,
                             options.database_chunk, airr_map, repository)
    elif options.type == "AIRR Clone":
        # process AIRR clone data
        print("Info: Processing AIRR Clone data file: {}".format(options.filename))
        parser = AIRR_Clone(options.verbose, options.database_map,
                            options.database_chunk, airr_map, repository)
    elif options.type == "AIRR Cell":
        # process AIRR Cell JSON data
        print("Info: Processing AIRR JSON Cell data file: {}".format(options.filename))
        parser = AIRR_Cell(options.verbose, options.database_map,
                           options.database_chunk, airr_map, repository)
    elif options.type == "AIRR Expression":
        # process AIRR Expression JSON data
        print("Info: Processing AIRR JSON Gene Expression data file: {}".format(options.filename))
        parser = AIRR_Expression(options.verbose, options.database_map,
                                 options.database_chunk, airr_map, repository)
    else:
        print("ERROR: unknown data type '{}'".format(options.type))
        sys.exit(4)

    # Check for a valid parser.
    if not parser.checkValidity():
        print("ERROR: Parser not contructed correctly, exiting...")
        sys.exit(4)

    # Override what the default annotation tool that the Parser subclass set by default.
    if not options.annotation_tool == "":
        parser.setAnnotationTool(options.annotation_tool)

    parse_ok = parser.process(options.filename)
    operation = "loaded"
    if options.update:
        operation = "updated"
    if parse_ok:
        print("Info: %s file %s %s successfully"%(options.type,options.filename,operation))
    else:
        print("ERROR: %s file %s not %s successfully"%(options.type,options.filename,operation))

    # time end
    t_end = time.perf_counter()
    print("Info: Finished processing in {:.2f} mins".format((t_end - t_start) / 60))

    # Return success
    if parse_ok:
        sys.exit(0)
    else:
        sys.exit(1)
