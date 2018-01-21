# dataloading-mongo

Here we give an overview of iReceptor node data loading configuration and operation. It is assumed that you type in these commands and run them within a Linux terminal (the '$' designates the command line prompt... yours may look different!).

# Prerequisites

If you are running the scripts directly on your host system, you should consider running
them within a [virtualenv](https://virtualenv.pypa.io/en/stable/installation/)
which sets Python3 as the default, since the data loading scripts are written to 
run under that version of Python.

To actually run the python data loading scripts (within or outside of virtualenv), 
certain Python3 library dependencies need to be installed. To do this, you need to 
[install pip3](https://pip.pypa.io/en/stable/installing/).

For Ubuntu:

```
$ sudo apt-get install python3-pip
$ pip3 install --upgrade pip # gets the latest version
```

If you are using another version of Linux, consult your respective operating system documentation for pip3 installation details.

# Dependencies

The data loading scripts use a number Python libraries. These are listed in the pip 'requirements.txt' file and may be installed as follows:

```
$ sudo pip3 install -r requirements.txt
```

# Running the loading script

The data loading is run inside a Linux terminal session on the machine which has your systems MongoDb database. 

Note that the data loading script accesses the database using the 'service' (**NOT** the 'guest') account username and password ("secret") that you will have specified while setting up the MongoDb database.  You need to specify these either as options on the command line or set as environment variables (see below).  You can use the -h / --help flag to display the data loader usage, as follows:

```
$ ./ireceptor_data_loader.py -h  

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

If this doesn't automatically work, then check first if the file's mode is set to mode 'executable':

```
$ chmod u+x ireceptor_data_loader.py
```

Then try again.

## Linux Environment Variables

Note also that the default parameters for this script may also be set as Linux environment variables, e.g.

```
$ export MONGODB_DB=ireceptor
$ export MONGODB_USER=<your-ireceptor-service-account-username>
$ export MONGODB_PASSWORD=<your-ireceptor-service-account-password>

```

The MONGODB_HOST variable defaults to 'localhost' which is normally ok (though you can change it if you wish to point to another MONGO instance outside of the docker one...).

If environment variables are set, then the corresponding command line parameters may be omitted while running the script.

# What kind of data can be loaded?

The ireceptor_data_loader currently accepts iReceptor sample metadata csv files and zip archives of IMGT data file output.

Assuming that your data files use the default names, then:

```
$ ./ireceptor_data_loader.py -v
```

will default to the --sample flag which loads a properly formatted *sample.csv* file into the database.

```
$ ./ireceptor_data_loader.py -v --imgt
```

will load a properly formatted *imgt.zip* sequence annotation into the database.

The expected data formats are described in more detail on the [iReceptor Data Curation repository site](https://github.com/sfu-ireceptor/dataloading-curation).


# Test Data

The 'testdata' subfolder in here contains test data files - sample.csv and imgt.zip - as documented in a README file and which may be read in by the available scripts, to test the node.
