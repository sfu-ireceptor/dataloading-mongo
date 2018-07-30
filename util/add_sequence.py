#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
 Parse a folder of gzipped fasta files
 Add each sequence in fasta files to the corresponding MongoDB document (using sequence id) 
 To check it worked: db.sequence.findOne({seq_name:'SRR873440.14976 GIMRZBB09FS80E length=224'})
"""

import re

import os
import sys
from os import listdir
from os.path import isfile, join
import argparse

import tarfile
import zipfile
import gzip

import shutil
import pymongo

from Bio import SeqIO
from Bio import Seq

def load_file(file_path, collection):
    filelist = []

#    if fullname.endswith(".zip"):
#        f= zipfile.ZipFile(fullname)
#        filelist = f.namelist()
        # f.extractall()
#    elif fullname.endswith(".gz"):
#        f= tarfile.open(fullname, 'r')
#        filelist = f.getnames()
#        #f.extractall()
#    elif fullname.endswith(".fasta"):
#        filelist.push(fullname)
#    else:
#        print("Unknown file")
#        return();

    print('-> Processing file: ' + file_path)
    tempfile = '/tmp/temp.fasta'
    with gzip.open(file_path) as f:
        with open(tempfile, 'wb') as out:
            shutil.copyfileobj(f, out)
        i = 1
        nb_matched = 0
        nb_modified = 0
        for record in SeqIO.parse(tempfile, 'fasta'):
            header = record.description
            sequence = str(record.seq)
            update_query = collection.update_many({'seq_name': header}, {'$set': {'sequence': sequence}})

            if update_query.matched_count == 0:
                # redo an update query with an IMGT-style header
                imgt_header = re.sub(r'\s', '_', header)
                imgt_header = imgt_header[0:50]
                update_query = collection.update_many({'seq_name': imgt_header}, {'$set': {'sequence': sequence}})
                if update_query.matched_count == 0:
                    print ('Header + ' + header + ' converted to ' + imgt_header + ' not found!')

            nb_matched += update_query.matched_count
            nb_modified += update_query.modified_count

            if i % 200000 == 0:
                print('Processed ' + str(i) + ' lines')
            i += 1
        print('Done. Stats:')
        print(' Read ' + str(i) + ' sequences in file.')
        print(' Found ' + str(nb_matched) + ' corresponding documents in database')
        print(' Added sequence  to ' + str(nb_modified) + ' documents')

    os.remove(tempfile)


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

    # parser.add_argument('database', help='database name')
    # parser.add_argument('collection', help='collection name')
    parser.add_argument('folder', help='folder of fasta files')

    # get arguments
    args = parser.parse_args()
   
    # database = args.database
    # collection = args.collection
    folder = args.folder

    # temporary, for convenience
    database = 'mydb'
    collection = 'sequence'

    main(database, collection, folder)
