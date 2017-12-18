For sample data importing:

For example, dbname:mydb, collectionname:sample_test

python sample_data_loading.py <dbname> <collectionname> <datapath> 

python sample_data_loading.py mydb sample_test master_metadata_20171031.csv


=============================================================================

For  sequence data importing:

For example, dbname:mydb, collectionname:sample_test sequencename:seq_test

python sequenceLoading.py <dbname> <sequencename> <sample> <datapath>

python sequenceLoading.py mydb seq_test sample_test /mnt/data/smalltest

#note: Please import sample first, since it will update some records in sample data while importing sequence data.