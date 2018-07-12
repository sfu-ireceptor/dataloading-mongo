# dataloading-mongo

Here we give an overview of iReceptor node data loading configuration and operation. 
It is assumed that you type in these commands and run them within a Linux terminal
running within the Linux machine which has your Mongo database. 
(note that the '$' designates the terminal command line prompt... your prompt may look different!).

This tutorial assumes that you are running a Linux version like Ubuntu
(adjust the installation instructions to suit your particular Linux flavor...).

## Prerequisites

If you are running the scripts directly on your host system, you should consider running
them within a [virtualenv](https://virtualenv.pypa.io/en/stable/installation/).
To install virtualenv, you will need to first install the regular (i.e. Python 2) version
of pip. Since the data loading script is written to run under the release 3 of Python, you should
ensure that it is also installed on your system. You will also need to install the latest version of [pip3](https://pip.pypa.io/en/stable/installing/) - the Python 3 variant of pip.

```
$ sudo apt install python-pip
```

Also install Python3 if it is not already pre-installed by your Linux OS version. (**Note:** if you are already using `Ubuntu 18.04 LTS` or newer, then python3 should be already installed by default.)

```
$ sudo apt install python3
```

Then, install pip3 and upgrade it to the latest version:

```
$ sudo apt install python3-pip
$ pip3 install --upgrade pip
```

After this point, you can install python packages using:

```
$ sudo python3 -m pip install SomePackage
```

Install `virtualenv`:

```
$ sudo python3 -m pip install virtualenv
```

If you are using another version of Linux, consult your respective operating system documentation for pip3 installation details.

## Running Virtualenv

The full [user guide](https://virtualenv.pypa.io/en/stable/userguide/) for virtualenv is available, but for our purposes, the required operation is simply to create a suitable location and initialize it with the tool. The one important detail to remember is to make Python3 the default Python interpreter used by the environment:

```
$ cd /opt/ireceptor
```

Create a new virual environment named `data` and make sure to specify Python3 as the default:

```
$ sudo python3 -m virtualenv --python=python3 data
```

Make sure your regular Linux account, not root, owns the directory:

```
$ sudo chown -R user:user /opt/ireceptor/data
```

To find out what is the value of `user`, type `whoami` or `id -un` in the terminal. 

Activate the virtualenv:

```
$ cd data
$ source bin/activate
```

You should now be running within a virtual environment inside a directory called *data*. Note that the command line prompt will change to something like the following:

```
(data) user@host:/opt/ireceptor/data$
```

To exit the virtualenv, type the following:

```
$ deactivate
```

You should now be back to your normal Linux system shell prompt.
To re-enter virtualenv, rerun the *source bin/activate* command as above,
from within the */opt/ireceptor/data* directory

For convenience, if you haven't already done so, it is also helpful to configure a Linux symbolic file link nearby, pointing to your local git cloned copy of the turnkey-service repository.something like:

```
$ sudo ln -s /path/to/your/cloned/turnkey-service /opt/ireceptor/turnkey-service
```

We assume this aliased location of the turnkey code in some of our commands which follow below.
(Modify those commands to suit the actual turnkey-service code (symbolic link) location that you decide to use).

## Installing Dependencies

The data loading scripts use several Python 3 libraries. These are listed in the 'requirements.txt' file and may be installed as follows (if you are using virtualenv, make sure that it is activated):

```
$ cd /opt/ireceptor/data/
$ source bin/activate
(data) user@host:/opt/ireceptor/data$ cd /opt/ireceptor/turnkey-service/dataloading-mongo
(data) user@host:/opt/ireceptor/data$ pip install -r requirements.txt
```
**Note:** Packages installed in this environment will live under `ENV/lib/pythonX.X/site-packages/`, where `ENV` is a directory to place the new virtual environment (in this case, `ENV` is the directory `data` located under `/opt/ireceptor/`).

## (Optional) Linux Environment Variables

Note that, For added convenience for running of the data loading script, some of the default parameters
of the data loading script may be specified in operating system environment variables. (this is only temporary per terminal session)

For example, under Ubuntu Linux, you may set the Mongo database, user and passwords as follows:

```
$ export MONGODB_DB=<your-database-name>
$ export MONGODB_SERVICE_USER=<your-ireceptor-service-account-username>
$ export MONGODB_SERVICE_SECRET=<your-ireceptor-service-account-secret-password>
```
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
    --mixcr             Load a zip archive of MiXCR analysis results.

  Sample Counter Reset Options:
    Options to specify whether or not the sample sequence counter should
    be reset or incremented during a current annotated sequence data
    loading run. Has no effect on sample metadata loading (Default:
    'reset').

    --reset             Reset sample counter when loading current annotated
                        sequence data set.
    --increment         Increment sample counter when loading current
                        annotated sequence data set.

  Database Connection Options:
    These options control access to the database.

    --host=HOST         MongoDb server hostname. Defaults to 'localhost'.
    --port=PORT         MongoDb server port number. Defaults to 27017.
    -u USER, --user=USER
                        MongoDb service user name. Defaults to the
                        MONGODB_SERVICE_USER environment variable if set.
                        Defaults to 'admin' otherwise.
    -p PASSWORD, --password=PASSWORD
                        MongoDb service user account secret ('password').
                        Defaults to the MONGODB_SERVICE_SECRET environment
                        variable if set. Defaults to empty string otherwise.
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

## Test Data

To use the data loader, we obviously need some data!

If you don't already have some suitably formatted data on hand but need to test your 
(Mongo) iReceptor node installation, you may use some test data files that we provide in the `data` submodule folder. A README file in that submodule describes what is available. For example,

```
$ ./ireceptor_data_loader.py -v --sample -u <serviceName> -p <serviceSecret> -d ireceptor -f ../data/test/imgt/imgt_sample.csv
```

loads the `imgt_sample.csv` sample metadata file. Next, run:

``` 
$ ./ireceptor_data_loader.py -v --imgt -u <serviceName> -p <serviceSecret> -d ireceptor -f ../data/test/imgt/imgt.zip
```
to load the `imgt.zip` data file.

You need to replace `<serviceName>`, `<serviceSecret>` with the Mongodb *service username* and *service password* respectively during the Turnkey configuration step.

## What kind of data can be loaded?

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