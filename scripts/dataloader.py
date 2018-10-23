#! /opt/ireceptor/data/bin/python
"""
 ireceptor_data_loader.py is a batch loading script for loading
 iReceptor sample metadata and sequence annotation
 
 The current version accepts 
 1) Sample metadata in the form of csv files with AIRR compliant tagged column headers
 
 2) IMGT annotation txt files, currently assumed bundled into tgz archives, 
    themselves wrapped inside a zip files.
 
 Running the ireceptor_data_loader.py script with 
 the -h flag publishes the usage of the script.
 
"""
import os
# from os.path import exists, isfile, basename, join

import pandas as pd
import urllib.parse
import pymongo
import json
import argparse
import time

from sample import Sample
from imgt import IMGT
from mixcr import MiXCR
from airr_tsv import AIRR_TSV
from airr_map import AIRRMap
from ireceptor_indexes import indexes

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
        help="print out the list of options given to this script")

    # Add configuration options 
    config_group = parser.add_argument_group("Configuration file options", "")
    config_group.add_argument(
        "--mapfile",
        dest="mapfile",
        default="ireceptor.cfg",
        help="iReceptor configuration file. Defaults to 'ireceptor.cfg' in the local directory where the command is run."
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
        help="Load a sample metadata file (a 'csv' file with standard iReceptor column headers)."
    )

    # Processing IMGT VQuest data, in the form of a zip archive
    type_group.add_argument(
        "-i",
        "--imgt",
        action='store_const',
        const="imgt",
        dest="type",
        help="Load a zip archive of IMGT analysis results."
    )

    # Processing MiXCR data
    type_group.add_argument(
        "-m",
        "--mixcr",
        action='store_const',
        const="mixcr",
        dest="type",
        help="Load a zip archive of MiXCR analysis results."
    )

    # Processing AIRR TSV annotation data, typically (but not limited to) from igblast
    type_group.add_argument(
        "-a",
        "--airr",
        action='store_const',
        const="airr",
        dest="type",
        help="Load data from AIRR TSV analysis results."
    )

    counter_group = parser.add_argument_group(
        "sample counter reset options",
        "options to specify whether or not the sample sequence counter should be reset or incremented during a current annotated sequence data loading run. Has no effect on sample metadata loading (Default: 'reset').")
    counter_group = counter_group.add_mutually_exclusive_group()
    counter_group.add_argument(
        "--reset",
        action="store_const",
        const="reset",
        dest="counter",
        default="reset",
        help="Reset sample counter when loading current annotated sequence data set."
    )
    counter_group.add_argument(
        "--increment",
        action="store_const",
        const="increment",
        dest="counter",
        help="Increment sample counter when loading current annotated sequence data set."
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
        default=os.environ.get("MONGODB_SERVICE_USER", "admin"),
        help="MongoDb service user name. Defaults to the MONGODB_SERVICE_USER environment variable if set. Defaults to 'admin' otherwise."
    )
    db_group.add_argument(
        "-p",
        "--password",
        dest="password",
        default=os.environ.get('MONGODB_SERVICE_SECRET', ''),
        help="MongoDb service user account secret ('password'). Defaults to the MONGODB_SERVICE_SECRET environment variable if set. Defaults to empty string otherwise."
    )
    db_group.add_argument(
        "-d",
        "--database",
        dest="database",
        default=os.environ.get("MONGODB_DB", "ireceptor"),
        help="Target MongoDb database. Defaults to the MONGODB_DB environment variable if set. Defaults to 'ireceptor' otherwise."
    )

    path_group = parser.add_argument_group("file path options")
    # making the default value to "" instead of "." creates the possiblity of the path being empty, therefore skip display error message to the user
    path_group.add_argument(
        "-l",
        "--library",
        dest="library",
        default="",
        help="Path to 'library' directory of data files."
    )
    path_group.add_argument(
        "-f",
        "--filename",
        dest="filename",
        default="",
        help="Name of file to load. Defaults to a data file with the --type name as the root name (appropriate file format and extension assumed)."
    )

    index_group = parser.add_argument_group("index control options")
    index_group.add_argument(
        "--drop",
        dest="drop_index",
        action="store_true",
        default=False,
        help="Drop the set of standard iReceptor indexes on the sequence level."
    )
    index_group.add_argument(
        "--build",
        dest="build_index",
        action="store_true",
        default=False,
        help="Build the set of standard iReceptor indexes on the sequence level."
    )
    index_group.add_argument(
        "--rebuild",
        dest="rebuild_index",
        action="store_true",
        default=False,
        help="Rebuild the set of standard iReceptor indexes on the sequence level. This is the same as running with the '--drop --build' options."
    )

    options = parser.parse_args()

    validate_library(options.library)
    validate_filename(options.filename)
    set_path(options)

    # # If we have a type and the type isn't a sample then we are processing sequences
    # # If we are doing a reset on the sequences, confirm that we really want to do it.
    # if options.type and options.type != 'sample' and options.counter == 'reset':
    #     prompt_counter(options)

    if options.drop_index and options.build_index:
        options.rebuild_index = True

    if options.rebuild_index:
        options.drop_index = True
        options.build_index = True

    if options.verbose:
        # if options.type != 'sample':
        #     print('SAMPLE SEQUENCE COUNTER:', options.counter)
        print('HOST         :', options.host)
        print('PORT         :', options.port)
        print('USER         :', options.user[0] + (len(options.user) - 2) * "*" + options.user[-1])
        print('PASSWORD     :', options.password[0] + (len(options.password) - 2) * "*" + options.password[-1] if options.password else "")
        print('DATABASE     :', options.database)
        print('MAPFILE      :', options.mapfile)
        print('DATA_TYPE    :', options.type)
        print('LIBRARY_PATH :', options.library)
        print('FILE_NAME    :', options.filename)
        print('FILE_PATH    :', options.path)
        print('DROP_INDEX   :', options.drop_index)
        print('BUILD_INDEX  :', options.build_index)
        print('REBUILD_INDEX:', options.rebuild_index)

    return options

def prompt_counter(context):
    while True:
        decision = input("### WARNING: reset the sample sequence counter to zero? (Yes/No): ")
        if decision.upper().startswith('Y'):
            context.counter = "reset"
            break
        elif decision.upper().startswith('N'):
            context.counter = "increment"
            break

# determine path to a data file or a directory of data files
def set_path(options):
    path = ""
    if options.library or options.filename:
        if options.library and options.filename:
            path = os.path.join(options.library, options.filename)
        elif options.library and not options.filename:
            path = options.library
        else:
            path = options.filename
            options.library = "."
    options.path = path

def validate_filename(filename_path):
    if filename_path:
        if os.path.isdir(filename_path):
            print("error: file '{0}' is not a file?".format(filename_path))
            raise SystemExit(1)

def validate_library(library_path):
    if library_path:
        if os.path.exists(library_path):
            if not os.path.isdir(library_path):
                print("error: library '{0}' is not a directory?".format(library_path))
                raise SystemExit(1)
        else:
            print("error: library '{0}' does not exist?".format(library_path))
            raise SystemExit(1)


class Context:
    def __init__(self, mapfile, type, library, filename, path, samples, sequences, counter, verbose, drop_index=False, build_index=False, rebuild_index=False):
        """Create an execution context with various info.


        Keyword arguments:
        
        type -- the type of data file. e.g. imgt

        library -- path to 'library' directory of data files. Defaults to the current working directory.

        filename -- name of the data file

        path -- path to the data file

        samples -- the mongo collection named 'sample'

        sequences -- the mongo collection named 'sequence'

        counter -- sequence counter

        verbose -- make output verbose
        """

        # Keep track of the data for this instance.
        self.mapfile = mapfile
        self.type = type
        self.library = library
        self.filename = filename
        self.path = path
        self.samples = samples
        self.sequences = sequences
        self.counter = counter
        self.verbose = verbose
        self.drop_index = drop_index
        self.build_index = build_index
        self.rebuild_index = rebuild_index
        # Create the AIRR Mapping object from the mapfile.
        self.airr_map = AIRRMap(mapfile)

    @classmethod
    def getContext(cls, options):

        # Connect with Mongo db
        username = urllib.parse.quote_plus(options.user)
        password = urllib.parse.quote_plus(options.password)
        uri = 'mongodb://%s:%s@%s:%s' % (username, password, options.host, options.port)
        print("Connecting to Mongo as user '%s' on '%s:%s'" %
                (username, options.host, options.port))

        # Connect to the Mongo server and return if not able to connect.
        try:
            mng_client = pymongo.MongoClient(uri)
        except pymongo.errors.ConfigurationError as err:
            print("Unable to connect to %s:%s - %s"
                    % (options.host, options.port, err))
            return None

        # Constructor doesn't block - need to check to see if the connection works.
        try:
            # The ismaster command is cheap and does not require auth.
            mng_client.admin.command('ismaster')
        except pymongo.errors.ConnectionFailure:
            print("Unable to connect to %s:%s, Mongo server not available"
                    % (options.host, options.port))
            return None

        # Set Mongo db name
        mng_db = mng_client[options.database]

        return cls(options.mapfile, options.type, options.library, options.filename, options.path,
                    mng_db['sample'], mng_db['sequence'], options.counter,
                    options.verbose, options.drop_index, 
                    options.build_index, options.rebuild_index)

# load a directory of files or a single file depending on 'context.path'
def load_data(context):
    if os.path.isdir(context.path):
        # skip directories
        filenames = [f for f in os.listdir(context.path) if not os.path.isdir(f)]
        filenames.sort()
        prog_name = os.path.basename(__file__)
        for filename in filenames:
            # skip loading this program itself
            if not prog_name in filename:
                context.filename = filename
                context.path = os.path.join(context.library, filename)
                prompt_and_load(filename, context)
    else:
        load_file(context)

# prompts the user whether to load the data file
def prompt_and_load(filename, context):
    while True:
        load = input("load '{0}' into database? (Yes/No): ".format(filename))
        if load.upper().startswith('Y'):
            load_file(context)
            break
        elif load.upper().startswith('N'):
            while True:
                cancel = input("**Are you sure to skip loading '{0}'? (Yes/No): ".format(filename))
                if cancel.upper().startswith('Y'):
                    print("skipped '{0}'!".format(filename))
                    break
                elif cancel.upper().startswith('N'):
                    prompt_and_load(filename, context)
                    break
            break

def load_file(context):
    # time start
    t_start = time.perf_counter()

    if context.type == "sample":
        # process samples
        print("processing Sample metadata file: {}".format(context.filename))
        sample = Sample(context)
        if sample.process():
            print("Sample metadata file loaded")
        else:
            print("ERROR: Sample input file not found?")
    elif context.type == "imgt":
        # process imgt
        print("processing IMGT data file: {}".format(context.filename))
        #prompt_counter(context)
        imgt = IMGT(context)
        if imgt.process():
            print("IMGT data file loaded")
    elif context.type == "mixcr":
        # process mixcr
        print("Processing MiXCR data file: {}".format(context.filename))
        #prompt_counter(context)
        mixcr = MiXCR(context)
        if mixcr.process():
            print("MiXCR data file loaded")
        else:
            print("ERROR: MiXCR data file not found?")
    elif options.type == "airr":
        # process AIRR TSV
        print("Processing AIRR TSV annotation data file: ", context.filename)
        #prompt_counter(context)
        airr = AIRR_TSV(context)
        if airr.process():
            print("AIRR TSV data file loaded")
        else:
            print("ERROR: AIRR TSV data file", context.filename, "not loaded correctly")
    else:
        print("Error: unknown data type '{}'".format(context.type))

    # time end
    t_end = time.perf_counter()
    print("finished processing in {:.2f} mins".format((t_end - t_start) / 60))

if __name__ == "__main__":
    options = getArguments()
    context = Context.getContext(options)

    if not context:
        raise SystemExit(1)

    # drop any indexes first, then load data and build indexes
    if context.drop_index or context.rebuild_index:
        print("Dropping indexes on sequence level...")
        context.sequences.drop_indexes()

    # load data files if path is provided by user
    if context.path:
        if os.path.exists(context.path):
                load_data(context)
        else:
            print("error: {1} data file '{0}' does not exist?".format(context.path, context.type))

    # build indexes
    if context.build_index or context.rebuild_index:
        print("Building indexes on sequence level...")
        for index in indexes:
            print("Now building index: {0}".format(index))
            t_start = time.perf_counter()
            context.sequences.create_index(index)
            t_end = time.perf_counter()
            print("Finished processing index in {:.2f} mins".format((t_end - t_start) / 60))
