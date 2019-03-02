#!/bin/bash
# A script to help automating the setup process for the dataloading

YELLOW="93"
IRECEPTOR="/opt/ireceptor"

# Colors the rest of the parameter using the color given by the first parameter ($1)
color() {
    CODE=$1
    shift
    echo -e "\e[${CODE}m$@\e[0m"
}

# check for python 3
if ! [ -x "$(command -v python3)" ]; then
    echo "$(color $YELLOW python3) is not detected on your system."
    exit 1
fi

# https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04
# Ubuntu 16.04 ships with both Python 3 and Python 2 pre-installed. Make sure that our versions are up-to-date:
sudo apt-get update
sudo apt-get -y upgrade

# install python's software package manager 'pip':
sudo apt-get install -y python3-pip
 
# install the venv module, part of the standard Python 3 library, so that we can create virtual environments:
sudo apt-get install -y python3-venv

# create python virtual environment
sudo python3 -m venv ${IRECEPTOR}/data

# make sure your regular Linux account, not root, owns the directory:
sudo chown -R $USER: ${IRECEPTOR}/data

# activate venv
cd ${IRECEPTOR}/data/
source bin/activate

# install dependencies
cd ${IRECEPTOR}/turnkey-service/dataloading-mongo
pip install --upgrade pip
pip install -r requirements.txt
