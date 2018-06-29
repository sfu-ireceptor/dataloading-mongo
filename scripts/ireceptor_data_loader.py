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
import optparse

from sample import Sample
from imgt import IMGT
from mixcr import MiXCR

_type2ext = {
    "sample": "csv",
    "imgt": "zip",  # assume a zip archive
    "mixcr": "zip",  # assume a zip archive
}


def inputParameters():

    parser = optparse.OptionParser(
        usage="%prog [options]\n\n" +
        "Note: for proper data processing, project --samples metadata should\n" +
        "generally be read first into the database before loading other data types.",
        version='1.0',
    )

    mode_opts = optparse.OptionGroup(
        parser,
        'Data Type Options',
        'Options to specify the type of data to load.',
    )

    mode_opts.add_option(
        '--sample',
        action="store_const",
        const='sample',
        dest='type',
        default='sample',
        help="Load a sample metadata file (a 'csv' file with standard iReceptor column headers)."
    )

    mode_opts.add_option(
        '--imgt',
        action='store_const',
        const='imgt',
        dest='type',
        help="Load a zip archive of IMGT analysis results.")

    mode_opts.add_option(
        '--mixcr',
        action='store_const',
        const='mixcr',
        dest='type',
        help="Load a zip archive of MiXCR analysis results.")

    parser.add_option_group(mode_opts)

    counter_reset_opts = optparse.OptionGroup(
        parser, 'Sample Counter Reset Options',
        "Options to specify whether or not the sample sequence counter should be reset or incremented during a current annotated sequence data loading run. Has no effect on sample metadata loading (Default: 'reset')."
    )

    counter_reset_opts.add_option(
        '--reset',
        action="store_const",
        const='reset',
        dest='counter',
        default='reset',
        help="Reset sample counter when loading current annotated sequence data set."
    )

    counter_reset_opts.add_option(
        '--increment',
        action="store_const",
        const='increment',
        dest='counter',
        help="Increment sample counter when loading current annotated sequence data set."
    )

    parser.add_option_group(counter_reset_opts)

    db_opts = optparse.OptionGroup(
        parser, 'Database Connection Options',
        'These options control access to the database.')

    db_opts.add_option(
        '--host',
        dest="host",
        default='localhost',
        help="MongoDb server hostname. Defaults to 'localhost'.")

    db_opts.add_option(
        '--port',
        dest="port",
        default=27017,
        type="int",
        help="MongoDb server port number. Defaults to 27017.")

    default_user = os.environ.get('MONGODB_SERVICE_USER', 'admin')

    db_opts.add_option(
        '-u',
        '--user',
        dest="user",
        default=default_user,
        help="MongoDb service user name. Defaults to the MONGODB_SERVICE_USER environment variable if set. Defaults to 'admin' otherwise."
    )

    default_password = os.environ.get('MONGODB_SERVICE_SECRET', '')

    db_opts.add_option(
        '-p',
        '--password',
        dest="password",
        default=default_password,
        help="MongoDb service user account secret ('password'). Defaults to the MONGODB_SERVICE_SECRET environment variable if set. Defaults to empty string otherwise."
    )

    default_database = os.environ.get('MONGODB_DB', 'ireceptor')

    db_opts.add_option(
        '-d',
        '--database',
        dest="database",
        default=default_database,
        help="Target MongoDb database. Defaults to the MONGODB_DB environment variable if set. Defaults to 'ireceptor' otherwise."
    )

    parser.add_option_group(db_opts)

    data_opts = optparse.OptionGroup(
        parser,
        'Data Source Options',
        'These options specify the identity and location of data files to be loaded.',
    )

    data_opts.add_option(
        '-l',
        '--library',
        dest="library",
        default=".",
        help="Path to 'library' directory of data files. Defaults to the current working directory."
    )

    data_opts.add_option(
        '-f',
        '--filename',
        dest="filename",
        default="",
        help="Name of file to load. Defaults to a data file with the --type name as the root name (appropriate file format and extension assumed)."
    )

    parser.add_option_group(data_opts)

    parser.add_option(
        '-v',
        '--verbose',
        dest="verbose",
        default=False,
        action="store_true",
    )

    options, remainder = parser.parse_args()

    if not options.filename:
        options.filename = options.type + "." + _type2ext[options.type]

    if options.type != 'sample' and options.counter == 'reset':
        while True:
            decision = input("### WARNING: you are resetting the sample sequence counter to zero? (Yes/No):")
            if decision.upper().startswith('Y'):
                break
            elif decision.upper().startswith('N'):
                options.counter = 'increment'
                break

    if options.verbose:
        print('INPUT TYPE:', options.type)
        if options.type != 'sample':
            print('SAMPLE SEQUENCE COUNTER:', options.counter)
        print('HOST      :', options.host)
        print('PORT      :', options.port)
        print('USER      :', options.user[0] + (len(options.user) - 2) * "*" + options.user[-1])
        print('PASSWORD  :', options.password[0] + (len(options.password) - 2) * "*" + options.password[-1] if options.password else "")
        print('DATABASE  :', options.database)
        print('LIBRARY   :', options.library)
        print('FILENAME  :', options.filename)

    return options


class Context:
    def __init__(self, type, library, filename, path, samples, sequences, counter, verbose):
        self.type = type
        self.library = library
        self.filename = filename
        self.path = path
        self.samples = samples
        self.sequences = sequences
        self.counter = counter
        self.verbose = verbose

    @classmethod
    def getContext(cls, options):

        if not options.filename:
            return False

        if options.library:
            path = options.library + "/" + options.filename
        else:
            path = options.filename

        if not exists(path):
            print("Input data file '" + path +
                  "' does not exist? No data to load?")
            return None

        else:

            # Connect with Mongo db
            username = urllib.parse.quote_plus(options.user)
            password = urllib.parse.quote_plus(options.password)
            uri = 'mongodb://%s:%s@%s:%s' % (username, password, options.host, options.port)
            print("Connecting to Mongo as user %s on %s:%s" %
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
                       options.verbose)


if __name__ == "__main__":
    options = inputParameters()
    context = Context.getContext(options)

    if context:

        if options.type == "sample":
            # process samples
            print("processing Sample metadata file: ", context.filename)

            sample = Sample(context)

            if sample.process():
                print("Sample metadata file loaded")
            else:
                print("ERROR: Sample input file not found?")

        elif options.type == "imgt":

            # process imgt

            print("processing IMGT data file: ", context.filename)

            imgt = IMGT(context)

            if imgt.process():
                print("IMGT data file loaded")
            else:
                print("ERROR: IMGT data file not found?")

        elif options.type == "mixcr":

            # process mixcr

            print("Processing MiXCR data file: ", context.filename)

            mixcr = MiXCR(context)

            if mixcr.process():
                print("MiXCR data file loaded")
            else:
                print("ERROR: MiXCR data file not found?")

        else:
            print("ERROR: unknown input data type:", context.type)
