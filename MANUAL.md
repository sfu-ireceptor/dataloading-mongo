# Manual Configuration of dataloading-mongo

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

## Activate the virtual environment

After setting up all the necessary packages, activate your python virtualenv as follows:

```
$ source /opt/ireceptor/data/bin/activate
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

## Installing Dependencies

The data loading scripts use several Python 3 libraries. These are listed in the 'requirements.txt' file and may be installed as follows (if you are using virtualenv, make sure that it is activated):

```
$ cd /opt/ireceptor/data/
$ source bin/activate
(data) user@host:/opt/ireceptor/data$ cd /opt/ireceptor/turnkey-service/dataloading-mongo
(data) user@host:/opt/ireceptor/data$ pip install -r requirements.txt
```
**Note:** Packages installed in this environment will live under `ENV/lib/pythonX.X/site-packages/`, where `ENV` is a directory to place the new virtual environment (in this case, `ENV` is the directory `data` located under `/opt/ireceptor/`).
