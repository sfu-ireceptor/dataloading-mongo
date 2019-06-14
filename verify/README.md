# iReceptor Data Verification

In order to ensure that data is loaded into an iReceptor repository correctly
we provide you with a number of verification tests that you can perform on the
repository after you have loaded the data. 

The following example assumes the following.
- That you have checked out the iReceptor dataloading-curation git repository
- That you have loaded the example MiXCR data set into an example iReceptor Turnkey repository "turnkey-test.ireceptor.org"
- That the Turnkey is running

Example usage:

verify_dataload.sh dataloading-curation/test/mixcr/PRJNA330606_Wang_One_Sample.csv samples.json PRJNA330606 dataloading-curation/test/mixcr/

where the samples.json file is produced by a command like wget on the API for a repository that contains the study in question.

wget http://turnkey-test.ireceptor.org/v2/samples -O samples.json

