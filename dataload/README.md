# Overview

This directory contains the core python code for loading data into the iReceptor Platform. It consists of a single data loading script that can load either repertoire metadata files or rearrangement annotation files. It supports a UTF-8 encoded [iReceptor Comma Separated Values (CSV)](https://github.com/sfu-ireceptor/dataloading-curation/tree/master/metadata) file format and the [AIRR Reperotire](https://docs.airr-community.org/en/stable/datarep/metadata.html) file format for repertoire metadata loading. It also supports the loading of rearrangement files as produced by a number of widely used annotation tools (IMGT HighV-QUEST, MiXCR, and igblast). The iReceptor data loader directly supports the loading of [AIRR TSV files](https://docs.airr-community.org/en/stable/datarep/rearrangements.html), and uses the AIRR TSV output of igblast to load igblast rearrangement files.

# Usage

Usage of dataloader.py is quite straight forward. You run it as a python script, providing command line arguments to describe the type of file that you are loading and the file that you want to load. A minimal command line would be:

- python dataloader.py --ireceptor -f PRJNA248411_Palanichamy_2018-12-18.csv
- python dataloader.py --imgt -f SRR1298731.txz

The first command above would load an iReceptor repertoire metadata file, in this case the repertoire metadata from a study "Immunoglobulin class-switched B cells provide an active immune axis between CNS and periphery in multiple sclerosis" by Palanichamy et. al. (https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4176763/).

The second command would load a single rearrangement file, in the IMGT HighV-QUEST format, for that same study. It is important to note that the data loader utilizes the rearrangement file name (SRR1298731.txz) to link the rearrangements in the file to the appropriate repertoire in the study. This means that:

1) The repertoire metadata file must be loaded before the rearrangement file is loaded, and
2) The rearrangement file name MUST appear in the field `data_processing_files` field in one, and only one, of the rows in the repertoire metadata file.

If this is not the case, the dataloader will produce an error message and will refuse to load the rearrangement file.

# Command Line Arguments

The iReceptor Data Loader takes various classes of options.

## General Options

- --verbose (-v): Run the program in verbose mode. This option will generate a lot of output, but is recommended from a data provenance perspective as it will inform you of how it mapped input data columns into repository columns.
- --mapfile: the iReceptor configuration file. Defaults to 'ireceptor.cfg' in the local directory where the command is run. This file contains the mappings between the AIRR Community field definitions, the annotation tool field definitions, and the fields and their names that are stored in the repository. The most recent version of the AIRR Mapping file for the iReceptor Project can be found here: https://github.com/sfu-ireceptor/config. In general, if you are using the iReceptor Data Loading tools as part of an iReceptor component (e.g. the iReceptor Turnkey - https://github.com/sfu-ireceptor/turnkey-service-php) the correct configuration file should be provided by the component you are using. Downloading the configuration file should not be necessary unless you are using this git repository and software as a standalone component?

## File Options

- --ireceptor (-s): The file to be loaded is a sample/repertoire metadata file (a 'csv' file with standard iReceptor/AIRR column headers)
- --repertoire (-s): The file to be loaded is a AIRR Repertoire metadata file (a 'json' file in the AIRR Repertoire format)
- --imgt (-i): The file to be loaded is a compressed archive files as provided by the IMGT HighV-QUEST annotation tool.
- --mixcr (-m): The file to be loaded is a text (or compressed text) annotation file as produced by the MiXCR annotation tool.
- --mixcr_v3: The file to be loaded is a text (or compressed text) annotation file as produced by the MiXCR (v3.0) annotation tool.
- --airr (-a): The file to be loaded is a text (or compressed text) annotation file in the AIRR TSV rearrangement format. This format is used to load annotation files produced by igblast (and other tools) that can produce AIRR TSV rearrangement files.
- --mixcr-clone: The file to be loaded is a text (or compressed text) clone file as produced by the MiXCR clone annotation tool
- --filename=filename (-f) - Name of the file to load. It is assumed that the filename provided is in the appropriate format that matches either the --sample, --imgt, --mixcr, or --airr command line options. An error will be reported if the formats do not match.

## Repository Options

- --skipload: Run the program without actually lodaing data into the repository. This option will allow testing of the entire load process without changing the repository.
- --update: Run the program in update mode rather than insert mode. This only works for repertoires.
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

Data loading into a Mongo repository is faster if you load the data without indexes, in particular without indexes on some of the larger and more complicated fields. As a result, it is faster to load data with only the indexes that are used by the data loading process itself. To maximize data loading performance, you should perform the data loading with all indexes dropped except the index on "ir_annotation_set_metadata_id_rearrangement". The index on ir_annotation_set_metadata_id_rearrangement is used to count the number of rearrangements loaded, and without this index the count process can be quite slow. There are several caveats to data loading in this configuration:

1) After data loading is complete, it is necessary to rebuild all of the indexes. Although this process takes a long time in itself, it is more efficient to drop the indexes (except the ir_annotation_set_metadata_id_rearrangement index), load the data, and then rebuild the indexes.
2) During data loading, without the indexes, your repository will perform very poorly on searches. If it is necessary to keep your repository in production with fast searching, it will be necessary to load the data with the indexes in place.

Note that if you are using the iRecepetor Turnkey's data loading scripts, the dropping and creation of indexes for efficient bulk data loading of large rearragnement data sets is handled for you.

# Data loading examples

The [iReceptor Data Loading and Curation github repository](https://github.com/sfu-ireceptor/dataloading-curation) has examples on how to load the various types of Repertoire and Rearrangement data sets.

# Data loading help

```
user:/app/dataload# python dataloader.py --help
usage: dataloader.py [-h] [--version] [-v] [--skipload] [--mapfile MAPFILE]
                     [--annotation_tool ANNOTATION_TOOL]
                     [--ireceptor | --repertoire | --imgt | --mixcr | --adaptive | --airr | --general]
                     [--host HOST] [--port PORT] [-u USER] [-p PASSWORD]
                     [-d DATABASE] [--database_map DATABASE_MAP]
                     [--database_chunk DATABASE_CHUNK]
                     [--repertoire_collection REPERTOIRE_COLLECTION]
                     [--rearrangement_collection REARRANGEMENT_COLLECTION]
                     [-f FILENAME]

optional arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  -v, --verbose         Run the program in verbose mode. This option will
                        generate a lot of output, but is recommended from a
                        data provenance perspective as it will inform you of
                        how it mapped input data columns into repository
                        columns.
  --skipload            Run the program without actually lodaing data into the
                        repository. This option will allow testing of the
                        entire load process without changing the repository.

Configuration file options:

  --mapfile MAPFILE     the iReceptor configuration file. Defaults to
                        'ireceptor.cfg' in the local directory where the
                        command is run. This file contains the mappings
                        between the AIRR Community field definitions, the
                        annotation tool field definitions, and the fields and
                        their names that are stored in the repository.
  --annotation_tool ANNOTATION_TOOL
                        The annotation tool to be noted for each rearrangment.
                        This defaults to the tool that is chosen (IgBLAST,
                        MiXCR, V-Quest) but can be overridden by the user if
                        desired. This is most useful for AIRR files which can
                        come from a variety of annotation tools (the default
                        being IgBLAST).

data type options:

  --ireceptor           The file to be loaded is an iReceptor
                        sample/repertoire metadata file (a 'csv' file with
                        standard iReceptor/AIRR column headers).
  --repertoire          The file to be loaded is an AIRR repertoire metadata
                        file (a 'JSON' file that adheres to the AIRR
                        Repertoire standard).
  --imgt                The file to be loaded is a compressed archive files as
                        provided by the IMGT HighV-QUEST annotation tool.
  --mixcr               The file to be loaded is a text (or compressed text)
                        annotation file as produced by the MiXCR annotation
                        tool.
  --adaptive            The file to be loaded is a text (or compressed text)
                        annotation file as produced by the Adaptive
                        ImmuneAccess platform.
  --airr                The file to be loaded is a text (or compressed text)
                        annotation file in the AIRR TSV rearrangement format.
                        This format is used to load annotation files produced
                        by IgBLAST (and other tools) that can produce AIRR TSV
                        rearrangement files.
  --general             The file to be loaded is a text (or compressed text)
                        annotation file that uses a 'general' mapping that is
                        non specific to an annotation tool. This feature
                        allows for the creation of a generic rearrangement
                        loading capability. This requires that an ir_general
                        mapping be present in the AIRR Mapping configuration
                        file being used by the data loader (see the --mapfile
                        option)

database options:
  --host HOST           MongoDb server hostname. Defaults to 'localhost'.
  --port PORT           MongoDb server port number. Defaults to 27017.
  -u USER, --user USER  MongoDb user name. Defaults to the
                        MONGODB_SERVICE_USER environment variable if set.
                        Defaults to empty string (no user name) otherwise.
  -p PASSWORD, --password PASSWORD
                        MongoDb service user account password. Defaults to the
                        MONGODB_SERVICE_SECRET environment variable if set.
                        Defaults to empty string (no password) otherwise.
  -d DATABASE, --database DATABASE
                        Target MongoDb database. Defaults to the MONGODB_DB
                        environment variable if set. Defaults to 'ireceptor'
                        otherwise.
  --database_map DATABASE_MAP
                        Mapping to use to map data terms into repository
                        terms. Defaults to ir_repository, which is the mapping
                        for the iReceptor Turnkey repository. This mapping
                        keyword MUST be in the term mapping file as specified
                        by --mapfile
  --database_chunk DATABASE_CHUNK
                        Number of records to process in a single step when
                        loading rearrangment data into the repository. This is
                        used to reduce the memory footpring of the loading
                        process when very large files are being loaded.
                        Defaults to 100,000
  --repertoire_collection REPERTOIRE_COLLECTION
                        The collection to use for storing and searching
                        repertoires (sample metadata). This is the collection
                        that sample metadata is inserted into when the
                        --sample option is specified. Defaults to 'sample',
                        which is the collection in the iReceptor Turnkey
                        repository.
  --rearrangement_collection REARRANGEMENT_COLLECTION
                        The collection to use for storing and searching
                        rearrangements (sequence annotations). This is the
                        collection that data is inserted into when the
                        --mixcr, --imgt, and --airr options are used to load
                        files. Defaults to 'sequence', which is the collection
                        in the iReceptor Turnkey repository.

file options:
  -f FILENAME, --filename FILENAME
                        Name of the file to load. It is assumed that the
                        filename provided is in the appropiate format that
                        matches either the --sample, --imgt, --mixcr, or
                        --airr command line options. An error will be reported
                        if the formats do not match.

```
