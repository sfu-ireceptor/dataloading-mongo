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
$ python3 metadata_loader.py
```

If the file's mode is set to executable, then it may be run directly:

```
$ chmod u+x metadata_loader.py
./metadata_loader.py
```


