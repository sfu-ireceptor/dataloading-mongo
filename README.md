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
$ pip3 install -r requirements.txt
```

# Running the loading script

The metadata loading script may be run as:

```
$ python3 metadata_loader.py -h  # -h flag will document the command usage
```

If the file's mode is set to executable, then it may also be run directly:

```
$ chmod u+x metadata_loader.py
./metadata_loader.py
```

Note that the default parameters for this script may be set as Linux environment variables set the  *export VARIABLE=value* protocol. These variables are documented in the usage command above.

**Test Data**

The 'testdata' subfolder in here contains some sample data documented in a README file, which may be read in by the available scripts, to test the node.

**Jupyter Docker (deprecated?)**

When the turnkey node is running, one of the Docker containers which is running is a Jupyter Notebook 
which is a sandbox of script development to facilitate data loading into the turnkey node's MongoDb database. This Notebook may be accessed using a web browser; however, the default is for the notebook to be protected by an authentication token. To see the full local link for this token, you need to look inside the log file of the Jupyter container. This is easily done as follows:

```
$ sudo docker logs irdn-notebook  # where 'irdn-notebook' is the default name of the Jupyter Notebook
```
Clicking on this link should give you the notebook home page with README and a *work* directory with data loading scripts under development.

Note, however, that the Python loading scripts in the Notebook may be out-of-date and rather, the scripts documented above are likely the preferred channel for data loading.

