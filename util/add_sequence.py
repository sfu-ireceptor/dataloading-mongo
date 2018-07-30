#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
 Parse a folder of gzipped fasta files
 Add each sequence in fasta files to the corresponding MongoDB document (using sequence id) 
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

    print('Processing file: ' + file_path)
    counter = 0
    tempfile = '/tmp/temp.fasta'
    with gzip.open(file_path) as f:
        with open(tempfile, 'wb') as out:
            shutil.copyfileobj(f, out)
        for record in SeqIO.parse(tempfile, 'fasta'):
            header = record.description
            imgt_header = re.sub(r'\s', '_', header)
            imgt_header = imgt_header[0:50]
            sequence = str(record.seq)
            update_query = collection.update({'seq_name': header},
                    {'$set': {'sequence': sequence}})
            if update_query['nModified'] == 0:
                update_query = \
                    collection.update({'seq_name': imgt_header},
                        {'$set': {'sequence': sequence}})
                if update_query['nModified'] == 0:
                    print ('Header + ' + header + ' converted to ' \
                        + imgt_header + ' not found!')
            counter += 1
            if counter % 200000 == 0:
                print('Processed ' + str(counter) + ' lines')

    os.remove(tempfile)


def main(database, collection, files_folder):
    # connect to MongoDB
    mongodb_client = pymongo.MongoClient('localhost', 27017)
    mongodb_collection = mongodb_client[database][collection]

    # generate list of files
    file_list = [f for f in listdir(files_folder) if isfile(join(files_folder, f))]

    # process each file
    for file_name in file_list:
        file_path = files_folder + file_name
        load_file(file_path, mongodb_collection)

if __name__ == '__main__':
    # define CLI arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('database', help='database name')
    parser.add_argument('collection', help='collection name')
    parser.add_argument('folder', help='folder of fasta files')

    # get arguments
    args = parser.parse_args()
   
    database = args.database
    collection = args.collection
    folder = args.folder

    main(database, collection, folder)
