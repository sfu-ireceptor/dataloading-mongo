This directory contains a small number of data files to use to test load the MongoDb database using the project Python scripts. They are generally excerpted from the public AIRR study by Palanichamy et al. (2014). Note that the data file is a comma delimited file with headers following the standard template (Excel) spreadsheet AIRR compliant data table format of the iReceptor project.

This data should not be used in production deployments of the turnkey but only for local testing of the installation and interface. Once the system is tested, you should re-initialize the database to a new empty database, then load your own data, suitably formatted.

The files provided are:

1) test_metadata.csv - which may be loaded by the script 'metadata_loader.py'
