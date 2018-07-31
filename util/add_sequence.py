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

import gzip
import pymongo

from Bio import SeqIO
from Bio import Seq

def load_file(file_path, collection):
    print('-> Processing file: ' + file_path)

    i = 0
    nb_matched = 0
    nb_modified = 0

    bulk_size = 10000

    # initialize bulk update
    bulk = collection.initialize_unordered_bulk_op()
    with gzip.open(file_path, 'rt') as handle:
        for record in SeqIO.parse(handle, 'fasta'):
            i += 1

            header = record.description
            imgt_header = re.sub(r'\s', '_', header)[0:50]
            sequence = str(record.seq)

            # do update query
            # update_query = collection.update_many({'$or':[{'seq_name': header},{'seq_name': imgt_header}]}, {'$set': {'sequence': sequence}}) 
            bulk.find({'$or':[{'seq_name': header},{'seq_name': imgt_header}]}).update({'$set': {'sequence': sequence}})           
            # nb_matched += update_query.matched_count
            # nb_modified += update_query.modified_count

            if (i % bulk_size == 0):
                bulk_result = bulk.execute()
                bulk = collection.initialize_ordered_bulk_op()
                nb_matched += bulk_result['nMatched']
                nb_modified += bulk_result['nModified']

            if i % 200000 == 0:
                print('Processed ' + str(i) + ' lines')

    if (i % bulk_size != 0):
        bulk_result = bulk.execute()
        nb_matched += bulk_result['nMatched']
        nb_modified += bulk_result['nModified']

    print(' Read ' + str(i) + ' sequences in file.')
    print(' Found ' + str(nb_matched) + ' corresponding documents in database')
    print(' Added sequence to ' + str(nb_modified) + ' documents')


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
