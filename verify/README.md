# iReceptor Data Verification

In order to ensure that data is loaded into an iReceptor repository correctly
we provide you with a number of verification tests that you can perform on the
repository after you have loaded the data. 

The following example assumes the following.
- That you have checked out the iReceptor dataloading-curation git repository
- That you have loaded the example MiXCR data set into an example iReceptor Turnkey repository "turnkey-test.ireceptor.org"
- That the Turnkey is running


The verify_dataload.sh script takes as input:

* the name of a CSV or EXCEL file containing sample metadata
* the URL associated to the Turnkey
* the study ID uniquely identifying the study
* the full path to a directory containing annotation files for sequences processed using either MIXCR, IMGT or 
IGBLAST
* a field name within the metadata uniquely idenfitying each sample

And as output it generates a report covering points 1-4. 

## Positional arguments:

```
  metadata_file      The EXCEL or CSV file containing sample metadata for a
                     study.
  API_url_address    The URL associated to your Turnkey, or the URL associated
                     to the API containing sample metadata.
  study_id           String value uniquely identifying study. Example:
                     PRJEB1234, PRJNA1234.
  annotation_dir     Full path to directory containing annotation files for
                     sequences processed using either IMGT, MIXCR and IGBLAST
                     annotations.
  sanity_level       This option let's you choose the level: H for short
                     summary, L for details on field name and content, F for
                     details on number of lines in annotation files against
                     what is found both in metadata spreadsheet and API
                     response.
  unique_identifier  Choose a field name from the sample metadata spreadsheet
                     which UNIQUELY identifies each sample.

optional arguments:
  -h, --help         show this help message and exit

```
## Sample Usage

An example with positional arguments

```
verify_dataload.sh /PATH/TO/metadata_file API_url_address study_id annotation_dir sanity_level unique_identifier
```
A working example using specific filename for sample metadata, Turnkey URL http://localhost/v2/samples, study ID PRJEB1234, generic path to annotation files, option LHF and unique identifier field name unique_sample_ID. 

```
verify_dataload.sh dataloading-curation/test/mixcr/PRJNA330606_Wang_One_Sample.csv http://localhost/v2/samples PRJEB1234 dataloading-curation/test/mixcr/ LHF unique_sample_ID
```

