#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
 Parse a folder of gzipped fasta files
 Add each sequence in fasta files to the corresponding MongoDB document (using sequence id) 
 To check it worked: db.sequence.findOne({seq_name:'SRR873440.14976 GIMRZBB09FS80E length=224'})
"""

import sys
import os
from os import listdir
from os.path import isfile, join
import argparse

import re
import time
import math

import gzip
import pymongo

from Bio import SeqIO
from Bio import Seq

def load_file(file_path, collection):
    print('-> Processing file: ' + file_path)
    start_time = time.time()
    lap_start_time = start_time

    i = 0
    nb_matched = 0
    nb_modified = 0

    bulk_size = 10000
    log_interval = 200000

    # initialize bulk update
    bulk = collection.initialize_unordered_bulk_op()
    with gzip.open(file_path, 'rt') as handle:
        for record in SeqIO.parse(handle, 'fasta'):
            i += 1

            # extract header and sequence from file
            header = record.description
            imgt_header = re.sub(r'\s', '_', header)[0:50]
            sequence = str(record.seq)

            # generate update query
            bulk.find({'$or':[{'seq_name': header},{'seq_name': imgt_header}]}).update({'$set': {'sequence': sequence}})           

            # execute update queries in bulk
            if (i % bulk_size == 0):
                bulk_result = bulk.execute()
                bulk = collection.initialize_unordered_bulk_op()
                nb_matched += bulk_result['nMatched']
                nb_modified += bulk_result['nModified']

            # periodic log
            if i % log_interval == 0:
                lap_end_time = time.time()
                lap_duration = math.ceil((lap_end_time - lap_start_time)/60)
                print('Processed ' + str(i) + ' lines, last {} lines in {} minutes'.format(log_interval, lap_duration))
                lap_start_time = time.time()

    # execute remaining update queries
    if (i % bulk_size != 0):
        bulk_result = bulk.execute()
        nb_matched += bulk_result['nMatched']
        nb_modified += bulk_result['nModified']

    end_time = time.time()
    duration = math.ceil((end_time - start_time)/60)

    # final log
    print(' Read ' + str(i) + ' sequences in file.')
    print(' Found ' + str(nb_matched) + ' corresponding documents in database')
    print(' Added sequence to ' + str(nb_modified) + ' documents')
    print('It took {} minutes '.format(duration))


def main(database, collection, files_folder):
    # connect to MongoDB
    mongodb_client = pymongo.MongoClient('localhost', 27017)
    mongodb_collection = mongodb_client[database][collection]

    # generate list of files
    file_list = [os.path.join(files_folder, f) for f in listdir(files_folder) if isfile(join(files_folder, f))]

    # process each file
    for file_path in file_list:
        load_file(file_path, mongodb_collection)

if __name__ == '__main__':
    # define CLI arguments
    parser = argparse.ArgumentParser()

    parser.add_argument('folder', help='folder of fasta files')

    # get arguments
    args = parser.parse_args()
   
    folder = args.folder

    # temporary, for convenience
    database = 'mydb'
    collection = 'sequence'

    main(database, collection, folder)