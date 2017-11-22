# dataloading-mongo

# Prerequisites

To directly run the python data loading scripts in this module outside of Jupyter, one needs
to install some requirements. The scripts are assume the use of Python 3.

To install the necessary Python3 libraries, you need to [install pip3](https://pip.pypa.io/en/stable/installing/). For Ubuntu:

```
$ sudo apt-get install python3-pip
$ pip3 install --upgrade pip # gets the latest version
```

If you are running the scripts directly on your host system, you should consider running
them within a [virtualenv](https://virtualenv.pypa.io/en/stable/installation/) which sets Python3 as the default.

# Dependencies

The data loading scripts use a number Python libraries. These are listed in the pip 'requirements.txt' file and may be installed as follows:

```
$ sudo pip3 install -r requirements.txt
```

# Running the loading script

The data loading may be run inside a terminal session. The -h flag will document the command usage as follows:

```
$ python3 ireceptor_data_loader.py -h  

Usage: ireceptor_data_loader.py [options]

Note: for proper data processing, project --samples metadata should
generally be read first into the database before loading other data types.

Options:
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -v, --verbose         

  Data Type Options:
    Options to specify the type of data to load.

    --sample            Load a sample metadata file (a 'csv' file with
                        standard iReceptor column headers).
    --imgt              Load a zip archive of IMGT analysis results.

  Database Connection Options:
    These options control access to the database.

    --host=HOST         MongoDb server hostname. If the MONGODB_HOST
                        environment variable is set, it is used. Defaults to
                        'localhost' otherwise.
    --port=PORT         MongoDb server port number. Defaults to 27017.
    -u USER, --user=USER
                        MongoDb service user name. Defaults to the
                        MONGODB_USER environment variable if set. Defaults to
                        'admin' otherwise.
    -p PASSWORD, --password=PASSWORD
                        MongoDb service user account secret ('password').
                        Defaults to the MONGODB_PASSWORD environment variable
                        if set. Defaults to empty string otherwise.
    -d DATABASE, --database=DATABASE
                        Target MongoDb database. Defaults to the MONGODB_DB
                        environment variable if set. Defaults to 'ireceptor'
                        otherwise.

  Data Source Options:
    These options specify the identity and location of data files to be
    loaded.

    -l LIBRARY, --library=LIBRARY
                        Path to 'library' directory of data files. Defaults to
                        the current working directory.
    -f FILENAME, --filename=FILENAME
                        Name of file to load. Defaults to a data file with the
                        --type name as the root name (appropriate file format
                        and extension assumed).

```

Note that if the file's mode is set to executable, then it may also be run directly:

```
$ chmod u+x ireceptor_data_loader.py
./ireceptor_data_loader.py
```

Note also that the default parameters for this script may be set as Linux environment variables set the  *export MONGODB_<tag>=value* protocol, where <tag> is one of the variable name suffixes noted in the -h usage output above.

The ireceptor_data_loader currently accepts iReceptor sample metadata csv files and zip archives of IMGT data file output.

**Test Data**

The 'testdata' subfolder in here contains some sample data documented in a README file, which may be read in by the available scripts, to test the node.
