# Overview

This directory contains the core python code for loading data into the iReceptor Platform. It consists of a single data loading script that can load either repertoire metadata files or rearrangement annotation files. It supports a simple UTF-8 encoded Comma Separated Values (CSV) file for repertoire metadata loading as well as a supporting the loading of rearrangement files as produced by a number of widely used annotation tools (IMGT HighV-QUEST, MiXCR, and igblast).

# Usage

Usage of dataloader.py is quite straight forward. You run it as a python script, providing command line arguments to minimally describe the type of file that you are loading and the file that you want to load. A minimal command line would be:

- python dataloader.py --sample -f PRJNA248411_Palanichamy_2018-12-18.csv
- python dataloader.py --imgt -f SRR1298731.txz

The first command above would load a repertoire metadata file, in this case the repertoire metadata from a study "Immunoglobulin class-switched B cells provide an active immune axis between CNS and periphery in multiple sclerosis" by Palanichamy et. al. (https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4176763/).

The second command would load a single rearrangement file, in the IMGT HighV-QUEST format, for that same study. It is important to note that the data loader utilizes the rearrangement file name (SRR1298731.txz) to link the rearrangements in the file to the appropriate repertoire in the study. This means that:

1) The repertoire metadata file must be loaded before the rearrangement file is loaded, and
2) The rearrangement file name MUST appear in the field ir_rearrangement_file_name in one, and only one, of the rows in the repertoire metadata file.

If this is not the case, the dataloader will produce an error message and will refuse to load the rearrangement file.

# Command Line Arguments

The iReceptor Data Loader takes various classes of options.

## General Options

- --verbose (-v): Run the program in verbose mode. This option will generate a lot of output, but is recommended from a data provenance perspective as it will inform you of how it mapped input data columns into repository columns.
- --mapfile: the iReceptor configuration file. Defaults to 'ireceptor.cfg' in the local directory where the command is run. This file contains the mappings between the AIRR Community field definitions, the annotation tool field definitions, and the fields and their names that are stored in the repository. The most recent version of the AIRR Mapping file for the iReceptor Project can be found here: https://github.com/sfu-ireceptor/config. In general, if you are using the iReceptor Data Loading tools as part of an iReceptor component (e.g. the iReceptor Turnkey - https://github.com/sfu-ireceptor/turnkey-service-php) the correct configuration file should be provided by the component you are using. Downloading the configuration file should not be necessary unless you are using this git repository and software as a standalone component?

## File Options

- --sample (-s): The file to be loaded is a sample/repertoire metadata file (a 'csv' file with standard iReceptor/AIRR column headers)
- --imgt (-i): The file to be loaded is a compressed archive files as provided by the IMGT HighV-QUEST annotation tool.
- --mixcr (-m): The file to be loaded is a text (or compressed text) annotation file as produced by the MiXCR annotation tool.
- --airr (-a): The file to be loaded is a text (or compressed text) annotation file in the AIRR TSV rearrangement format. This format is used to load annotation files produced by igblast (and other tools) that can produce AIRR TSV rearrangement files.
- --filename=filename - Name of the file to load. It is assumed that the filename provided is in the appropriate format that matches either the --sample, --imgt, --mixcr, or --airr command line options. An error will be reported if the formats do not match.

## Repository Options

- --host=host - MongoDb server hostname. Defaults to 'localhost'.
- --port=port - MongoDb server port number. Defaults to 27017.
- --user=user - MongoDb  user name. Defaults to the MONGODB_SERVICE_USER environment variable if set. Defaults to empty string (no user name) otherwise."
- --password=password - MongoDb user account password. Defaults to the MONGODB_SERVICE_SECRET environment variable if set. Defaults to empty string otherwise (no password)"
- --database=db - Target MongoDb database. Defaults to the MONGODB_DB environment variable if set. Defaults to 'ireceptor' otherwise."
- --database_map - Mapping to use to map data terms into repository terms. Defaults to ir_turnkey, which is the mapping for the iReceptor Turnkey repository. This mapping keyword MUST be in the term mapping file as specified by --mapfile
- --database_chunk=250000 - Number of records to process in a single step when loading rearrangement data into the repository. This is used to reduce the memory footprint of the loading process when very large files are being loaded. Defaults to 100,000"
- --repertoire_collection=sample - The collection to use for storing and searching repertoires (sample metadata). This is the collection that sample metadata is inserted into when the --sample option is specified. Defaults to 'sample', which is the collection in the iReceptor Turnkey repository.
- --rearrangement_collection=sequence - The collection to use for storing and searching rearrangements (sequence annotations). This is the collection that data is inserted into when the --mixcr, --imgt, and --airr options are used to load files. Defaults to 'sequence', which is the collection in the iReceptor Turnkey repository.

# Requirements

The iReceptor python data loader has the following requirements:

- Python 3 or later
- Pandas (https://pandas.pydata.org/pandas-docs/stable/)
- AIRR python library (https://github.com/airr-community/airr-standards/tree/master/lang/python)

If you are using the iReceptor data loading module through one of the iReceptor provided services (the iReceptor Turnkey Repository) then these requirements are satisfied through the docker containers used for those services.

# Data loading performance caveats

Data loading into a Mongo repository is faster if you load the data without indexes, in particular without indexes on some of the larger and more complicated fields. As a result, it is faster to load data with only the indexes that are used by the data loading process itself. To maximize data loading performance, you should perform the data loading with all indexes dropped except the index on "ir_project_sample_id". The index on ir_project_sample_id is used to count the number of rearrangements loaded, and without this index the count process can be quite slow. There are several caveats to data loading in this configuration:

1) After data loading is complete, it is necessary to rebuild all of the indexes. Although this process takes a long time in itself, it is more efficient to drop the indexes (except the ir_project_sample_id index), load the data, and then rebuild the indexes.
2) During data loading, without the indexes, your repository will perform very poorly on searches. If it is necessary to keep your repository in production with fast searching, it will be necessary to load the data with the indexes in place.
