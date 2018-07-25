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
from os.path import exists

import pandas as pd
import urllib.parse
import pymongo
import json
import argparse
import time

from sample import Sample
from imgt import IMGT
from mixcr import MiXCR
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

    type_group = parser.add_argument_group("data type options", "(Default: 'sample')")
    type_group = type_group.add_mutually_exclusive_group()
    type_group.add_argument(
        "-s",
        "--sample",
        action="store_const",
        const="sample",
        dest="type",
        default='sample',
        help="Load a sample metadata file (a 'csv' file with standard iReceptor column headers)."
    )
    type_group.add_argument(
        "-i",
        "--imgt",
        action='store_const',
        const="imgt",
        dest="type",
        help="Load a zip archive of IMGT analysis results."
    )
    type_group.add_argument(
        "-m",
        "--mixcr",
        action='store_const',
        const="mixcr",
        dest="type",
        help="Load a zip archive of MiXCR analysis results."
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
    path_group.add_argument(
        "-l",
        "--library",
        dest="library",
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

    # if not options.filename:
    #     options.filename = options.type + "." + _type2ext[options.type]

    if options.type != 'sample' and options.counter == 'reset':
        while True:
            decision = input("### WARNING: you are resetting the sample sequence counter to zero? (Yes/No):")
            if decision.upper().startswith('Y'):
                break
            elif decision.upper().startswith('N'):
                options.counter = 'increment'
                break

    if options.drop_index and options.build_index:
        options.rebuild_index = True

    if options.rebuild_index:
        options.drop_index = True
        options.build_index = True

    if options.verbose:
        if options.type != 'sample':
            print('SAMPLE SEQUENCE COUNTER:', options.counter)
        print('DATA_TYPE    :', options.type)
        print('HOST         :', options.host)
        print('PORT         :', options.port)
        print('USER         :', options.user[0] + (len(options.user) - 2) * "*" + options.user[-1])
        print('PASSWORD     :', options.password[0] + (len(options.password) - 2) * "*" + options.password[-1] if options.password else "")
        print('DATABASE     :', options.database)
        print('LIBRARY      :', options.library)
        print('FILENAME     :', options.filename)
        print('DROP_INDEX   :', options.drop_index)
        print('BUILD_INDEX  :', options.build_index)
        print('REBUILD_INDEX:', options.rebuild_index)

    return options


class Context:
    def __init__(self, type, library, filename, path, samples, sequences, counter, verbose, drop_index=False, build_index=False, rebuild_index=False):
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

    @classmethod
    def getContext(cls, options):
        if options.library:
            path = options.library + "/" + options.filename
        else:
            path = options.filename
            
        # Connect with Mongo db
        username = urllib.parse.quote_plus(options.user)
        password = urllib.parse.quote_plus(options.password)
        uri = 'mongodb://%s:%s@%s:%s' % (username, password, options.host, options.port)
        print("Connecting to Mongo as user '%s' on '%s:%s'" %
                (username, options.host, options.port))

        mng_client = pymongo.MongoClient(uri)
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

        return cls(options.type, options.library, options.filename, path,
                    mng_db['sample'], mng_db['sequence'], options.counter,
                    options.verbose, options.drop_index, 
                    options.build_index, options.rebuild_index)

def load_data(context):
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
        imgt = IMGT(context)
        if imgt.process():
            # dataloaded = True
            print("IMGT data file loaded")
    elif context.type == "mixcr":
        # process mixcr
        print("Processing MiXCR data file: {}".format(context.filename))
        mixcr = MiXCR(context)
        if mixcr.process():
            # dataloaded = True
            print("MiXCR data file loaded")
        else:
            print("ERROR: MiXCR data file not found?")
    else:
        print("Unknown data type: {}".format(context.type))

if __name__ == "__main__":
    options = getArguments()
    context = Context.getContext(options)

    # drop any indexes first, then load data and build indexes
    if context.drop_index or context.rebuild_index:
        print("Dropping indexes on sequence level...")
        context.sequences.drop_indexes()

    # load data files
    if exists(context.path):
        load_data(context)

    # build indexes
    if context.build_index or context.rebuild_index:
        print("Building indexes on sequence level...")
        t_start = time.perf_counter()
        for index in indexes:
            context.sequences.create_index(index)
        t_end = time.perf_counter()
        print("Done. It took {:.2f} seconds to build the indexes.".format(t_end - t_start))
