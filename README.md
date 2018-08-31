# dataloading-mongo

Here we give an overview of iReceptor node data loading configuration and operation. 
It is assumed that you type in these commands and run them within a Linux terminal
running within the Linux machine which has your Mongo database. 
(note that the '$' designates the terminal command line prompt... your prompt may look different!).

This tutorial assumes that you are running a Linux version like Ubuntu
(adjust the installation instructions to suit your particular Linux flavor...).

## Prerequisites

Choose either of the following methods to setup the appropriate packages:

- **setup configurations automatically using a provided script**

simply execute the script `setup.sh` in the root directory of this project in your terminal:

```
$ ./setup.sh
```

- **setup configurations manually**

Follow the [manual configuration instructions](./MANUAL.md).

## (Optional) Linux Environment Variables

Note that, For added convenience for running of the data loading script, some of the default parameters
of the data loading script may be specified in operating system environment variables. (this is only temporary per terminal session)

For example, under Ubuntu Linux, you may set the Mongo database, user and passwords as follows:

```
$ export MONGODB_DB=<your-database-name>
$ export MONGODB_SERVICE_USER=<your-ireceptor-service-account-username>
$ export MONGODB_SERVICE_SECRET=<your-ireceptor-service-account-secret-password>
```

There is convenience script `export.sh` that you can execute the variables automatically in the root directory of the [turnkey-service](https://github.com/sfu-ireceptor/turnkey-service) project.

**Note**: You must enter the *variables names* exactly as shown, since the dataloading script will use the exact same spelling (e.g. 'MONGODB_DB') to run its logic.

See the script usage (below) for additional options that may be set this way. Note that some of these options 
may have reasonable default values. For example, the MONGODB_HOST variable defaults to 'localhost' which is normally ok 
(though you can change it if you wish to point to another MONGO instance running on another machine perhaps).

If environment variables are set, then the corresponding command line parameters may be omitted while running the script.

## Running the loading script

You are now ready to run the data loading script. 

Note that the data loading script accesses the database using the `service` (**NOT** the `guest`) account username and password (i.e. secret) that you will have specified while setting up the MongoDb database.  

You need to specify these either as options on the command line or set as environment variables (see below). 
 You can use the -h / --help flag to display the data loader usage, as follows:

```
$ ./dataloader.py -h  

usage: dataloader.py [-h] [--version] [-v] [-s | -i | -m | -a]
                     [--reset | --increment] [--host HOST] [--port PORT]
                     [-u USER] [-p PASSWORD] [-d DATABASE] [-l LIBRARY]
                     [-f FILENAME] [--drop] [--build] [--rebuild]

Note: for proper data processing, project --samples metadata should
generally be read first into the database before loading other data types.

optional arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  -v, --verbose         print out the list of options given to this script

data type options:

  -s, --sample          Load a sample metadata file (a 'csv' file with
                        standard iReceptor column headers).
  -i, --imgt            Load a zip archive of IMGT analysis results.
  -m, --mixcr           Load a zip archive of MiXCR analysis results.
  -a, --airr            Load data from AIRR TSV analysis results.

sample counter reset options:
  options to specify whether or not the sample sequence counter should be reset or incremented during a current annotated sequence data loading run. Has no effect on sample metadata loading (Default: 'reset').

  --reset               Reset sample counter when loading current annotated
                        sequence data set.
  --increment           Increment sample counter when loading current
                        annotated sequence data set.

database options:
  --host HOST           MongoDb server hostname. Defaults to 'localhost'.
  --port PORT           MongoDb server port number. Defaults to 27017.
  -u USER, --user USER  MongoDb service user name. Defaults to the
                        MONGODB_SERVICE_USER environment variable if set.
                        Defaults to 'admin' otherwise.
  -p PASSWORD, --password PASSWORD
                        MongoDb service user account secret ('password').
                        Defaults to the MONGODB_SERVICE_SECRET environment
                        variable if set. Defaults to empty string otherwise.
  -d DATABASE, --database DATABASE
                        Target MongoDb database. Defaults to the MONGODB_DB
                        environment variable if set. Defaults to 'ireceptor'
                        otherwise.

file path options:
  -l LIBRARY, --library LIBRARY
                        Path to 'library' directory of data files.
  -f FILENAME, --filename FILENAME
                        Name of file to load. Defaults to a data file with the
                        --type name as the root name (appropriate file format
                        and extension assumed).

index control options:
  --drop                Drop the set of standard iReceptor indexes on the
                        sequence level.
  --build               Build the set of standard iReceptor indexes on the
                        sequence level.
  --rebuild             Rebuild the set of standard iReceptor indexes on the
                        sequence level. This is the same as running with the '
                        --drop --build' options.
```

If this doesn't automatically work, then check first if the file's mode is set to mode 'executable':

```
$ chmod +x dataloader.py
```

Then try again.

## Test Data

To use the data loader, we obviously need some data!

If you don't already have some suitably formatted data on hand but need to test your 
(Mongo) iReceptor node installation, you may use some test data files that we provide in the `data` submodule folder. A README file in that submodule describes what is available. For example,

```
$ ./dataloader.py -v --sample -u <serviceName> -p <serviceSecret> -d ireceptor -f ../data/test/imgt/imgt_sample.csv
```

loads the `imgt_sample.csv` sample metadata file. Next, run:

``` 
$ ./dataloader.py -v --imgt -u <serviceName> -p <serviceSecret> -d ireceptor -f ../data/test/imgt/imgt.zip
```
to load the `imgt.zip` data file.

You need to replace `<serviceName>`, `<serviceSecret>` with the Mongodb *service username* and *service password* respectively during the Turnkey configuration step.

## What kind of data can be loaded?

The ireceptor_data_loader currently accepts iReceptor sample metadata csv files and zip archives of IMGT data file output.

Assuming that your data files use the default names, then:

```
$ ./dataloader.py -v
```

will default to the --sample flag which loads a properly formatted *sample.csv* file into the database.

```
$ ./dataloader.py -v --imgt
```

will load a properly formatted *imgt.zip* sequence annotation into the database.

The expected data formats are described in more detail on the [iReceptor Data Curation repository site](https://github.com/sfu-ireceptor/dataloading-curation).