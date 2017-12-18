For sample data importing:
For example, dbname:mydb, collectionname:sample_test
There are two methods:
1. the command line method
If using a CSV file, you can use:
mongoimport -d <dbname> -c <collectionname> --type csv --file <datapath> --headerline --port 27017
mongoimport -d mydb -c sample_test --type csv --file master_metadata_processed_20171214.csv --headerline --port 27017
#master_metadata_processed_20171214.csv is processed from the metadata bojan gave me.

2. the python script method
python sample_data_loading.py <dbname> <collectionname> <datapath> 
python sample_data_loading.py mydb sample_test master_metadata_20171031.csv

For  sequence data importing:
python sequenceLoading.py <dbname> <sequencename> <sample> <datapath>
python sequenceLoading.py mydb seq_test sample_test /mnt/data/smalltest
#note: Please import sample first, since it will update some records in sample data while importing sequence data.