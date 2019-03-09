#! /opt/ireceptor/data/bin/python
"""
 ireceptor_data_loader.py is a batch loading script for loading
 iReceptor sample metadata and sequence annotation
 
"""
import os
import pandas as pd
import urllib.parse
import pymongo
import json
import argparse
import time
import sys

from sample import Sample
from imgt import IMGT
from mixcr import MiXCR
from airr_tsv import AIRR_TSV
from airr_map import AIRRMap

_type2ext = {
    "sample": "csv",
    "imgt": "zip",  # assume a zip archive
    "mixcr": "zip",  # assume a zip archive
}

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

    # Add configuration options 
    config_group = parser.add_argument_group("Configuration file options", "")
    config_group.add_argument(
        "--mapfile",
        dest="mapfile",
        default="ireceptor.cfg",
        help="the iReceptor configuration file. Defaults to 'ireceptor.cfg' in the local directory where the command is run. This file contains the mappings between the AIRR Community field definitions, the annotation tool field definitions, and the fields and their names that are stored in the repository."
    )

    type_group = parser.add_argument_group("data type options", "")
    type_group = type_group.add_mutually_exclusive_group()

    # Processing sample metadata
    type_group.add_argument(
        "-s",
        "--sample",
        action="store_const",
        const="sample",
        dest="type",
        default="",
        help="The file to be loaded is a sample/repertoire metadata file (a 'csv' file with standard iReceptor/AIRR column headers)."
    )

    # Processing IMGT VQuest data, in the form of a zip archive
    type_group.add_argument(
        "-i",
        "--imgt",
        action='store_const',
        const="imgt",
        dest="type",
        help="The file to be loaded is a compressed archive files as provided by the IMGT HighV-QUEST annotation tool."
    )

    # Processing MiXCR data
    type_group.add_argument(
        "-m",
        "--mixcr",
        action='store_const',
        const="mixcr",
        dest="type",
        help="The file to be loaded is a text (or compressed text) annotation file as produced by the MiXCR annotation tool."
    )

    # Processing AIRR TSV annotation data, typically (but not limited to) from igblast
    type_group.add_argument(
        "-a",
        "--airr",
        action='store_const',
        const="airr",
        dest="type",
        help="The file to be loaded is a text (or compressed text) annotation file in the AIRR TSV rearrangement format. This format is used to load annotation files produced by igblast (and other tools) that can produce AIRR TSV rearrangement files."
    )

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
        default="ir_turnkey",
        help="Mapping to use to map data terms into repository terms. Defaults to ir_turnkey, which is the mapping for the iReceptor Turnkey repository. This mapping keyword MUST be in the term mapping file as specified by --mapfile"
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

    path_group = parser.add_argument_group("file options")
    path_group.add_argument(
        "-f",
        "--filename",
        dest="filename",
        default="",
        help="Name of the file to load. It is assumed that the filename provided is in the appropiate format that matches either the --sample, --imgt, --mixcr, or --airr command line options. An error will be reported if the formats do not match."
    )

    options = parser.parse_args()

    validate_filename(options.filename)
    set_path(options)

    if options.verbose:
        print('HOST         :', options.host)
        print('PORT         :', options.port)
        print('USER         :', options.user[0] + (len(options.user) - 2) * "*" + options.user[-1] if options.user else "")
        print('PASSWORD     :', options.password[0] + (len(options.password) - 2) * "*" + options.password[-1] if options.password else "")
        print('DATABASE     :', options.database)
        print('DATABASE_MAP :', options.database_map)
        print('MAPFILE      :', options.mapfile)
        print('DATA_TYPE    :', options.type)
        print('FILE_NAME    :', options.filename)
        print('FILE_PATH    :', options.path)

    return options

# determine path to a data file or a directory of data files
def set_path(options):
    options.path = options.filename

def validate_filename(filename_path):
    if filename_path:
        if os.path.isdir(filename_path):
            print("ERROR: file '{0}' is not a file?".format(filename_path))
            raise SystemExit(1)

class Context:
    def __init__(self, mapfile, type, filename, path, samples, sequences, database_map, database_chunk, verbose):
        """Create an execution context with various info.

        Keyword arguments:
        
        type -- the type of data file. e.g. imgt

        filename -- name of the data file

        path -- path to the data file

        samples -- the mongo collection named 'sample'

        sequences -- the mongo collection named 'sequence'

        verbose -- make output verbose
        """

        # Keep track of the data for this instance.
        self.mapfile = mapfile
        self.type = type
        self.filename = filename
        self.path = path
        self.samples = samples
        self.sequences = sequences
        self.verbose = verbose
        # Create the AIRR Mapping object from the mapfile.
        self.airr_map = AIRRMap(self.verbose)
        self.airr_map.readMapFile(self.mapfile)
        # Keep track of the repository map key from the mapping file.
        self.repository_tag = database_map
        # Keep track of the repository chunk size for loading datasets.
        self.repository_chunk = database_chunk

    @classmethod
    def getContext(cls, options):

        # Connect with Mongo db
        username = urllib.parse.quote_plus(options.user)
        password = urllib.parse.quote_plus(options.password)
        if len(username) == 0 and len(password) == 0:
            uri = 'mongodb://%s:%s' % (options.host, options.port)
            print("Info: Connecting to Mongo with no username/password on '%s:%s'" %
                (options.host, options.port))
        else:
            uri = 'mongodb://%s:%s@%s:%s' % (username, password, options.host, options.port)
            print("Info: Connecting to Mongo as user '%s' on '%s:%s'" %
                (username, options.host, options.port))

        # Connect to the Mongo server and return if not able to connect.
        try:
            mng_client = pymongo.MongoClient(uri)
        except pymongo.errors.ConfigurationError as err:
            print("ERROR: Unable to connect to %s:%s - %s"
                    % (options.host, options.port, err))
            return None

        # Constructor doesn't block - need to check to see if the connection works.
        try:
            # We need to check that we can perform a real operation on the collection
            # at this time. We want to check for connection errors, authentication
            # errors. We want to let through the case that there is an empty repository
            # and the cursor comes back empty.
            mng_db = mng_client[options.database]
            mng_sample = mng_db[options.repertoire_collection]

            cursor = mng_sample.find( {}, { "_id": 1 } ).sort("_id", -1).limit(1)
            record = cursor.next()
        except pymongo.errors.ConnectionFailure:
            print("ERROR: Unable to connect to %s:%s, Mongo server not available"
                    % (options.host, options.port))
            return None
        except pymongo.errors.OperationFailure as err:
            print("ERROR: Operation failed on %s:%s, %s"
                    % (options.host, options.port, str(err)))
            return None
        except StopIteration:
            # This exception is not an error. The cursor.next() raises this exception when it has no more
            # data in the cursor. In this case, this would mean that the database is empty,
            # but the database was opened and the query worked. So this is not an error case as it
            # OK to have an empty database.
            pass


        # Set Mongo db name
        mng_db = mng_client[options.database]

        return cls(options.mapfile, options.type, options.filename, options.path,
                    mng_db[options.repertoire_collection], mng_db[options.rearrangement_collection], 
                    options.database_map, options.database_chunk,
                    options.verbose)

    @staticmethod
    def checkValidity(context):
        # Check any runtime consistency issues for the context, and return False if
        # something is not valid.

        # Check to see if the AIRR mappings are valid.
        if not context.repository_tag in context.airr_map.airr_mappings:
            print("ERROR: Could not find repository mapping " + context.repository_tag + " in AIRR Mappings")
            return False
        return True

def load_file(context):
    # time start
    t_start = time.perf_counter()

    if context.type == "sample":
        # process samples
        print("Info: Processing repertoire metadata file: {}".format(context.filename))
        parser = Sample(context)
    elif context.type == "imgt":
        # process imgt
        print("Info: Processing IMGT data file: {}".format(context.filename))
        parser = IMGT(context)
    elif context.type == "mixcr":
        # process mixcr
        print("Info: Processing MiXCR data file: {}".format(context.filename))
        parser = MiXCR(context)
    elif options.type == "airr":
        # process AIRR TSV
        print("Info: Processing AIRR TSV annotation data file: ", context.filename)
        parser = AIRR_TSV(context)
    else:
        print("ERROR: unknown data type '{}'".format(context.type))
        return False

    parse_ok = parser.process()
    if parse_ok:
        print("Info: " + options.type + " file " + context.filename + " loaded successfully")
    else:
        print("ERROR: " + options.type + " file " + context.filename + " not loaded correctly")

    # time end
    t_end = time.perf_counter()
    print("Info: Finished processing in {:.2f} mins".format((t_end - t_start) / 60))
    return parse_ok

if __name__ == "__main__":
    # Get the command line arguments.
    options = getArguments()
    # Create the context given the options.
    context = Context.getContext(options)

    # Check on the successful creation of the context and its validity.
    if not context:
        sys.exit(1)
    if not Context.checkValidity(context):
        sys.exit(1)

    # Try to load the file, return an error if not...
    if not load_file(context):
        sys.exit(4)

    # Return success
    sys.exit(0)
