// modified to run a query type for all samples ids and THEN move to another query type 

var collection = '', sample_id_list = [], queries = [], results = [];

var start_time, query_setup_time, sample_id_time, end_time;

start_time = new Date();

/****************************************************************************************
 config */

// Set the collection being used.
collection = 'sequence';

queries['total'] = {};
// Junction Length
queries['equals_9'] = {'ir_junction_aa_length': 9};
queries['equals_15'] = {'ir_junction_aa_length': 15};
// Junction substring
queries['junction'] = {'ir_substring': 'CASSQVGTGVY'};
// B-Cell queries
// B-Cell V-Gene
queries['vgene_family_igh'] = {ir_vgene_family: 'IGHV2'};
queries['vgene_gene_igh'] = {ir_vgene_gene: 'IGHV2-5'};
queries['v_call_igh'] = {v_call: 'IGHV2-5*08'};
// B-Cell D-Gene
queries['dgene_family_igh'] = {ir_dgene_family: 'IGHD2'};
queries['dgene_gene_igh'] = {ir_dgene_gene: 'IGHD2-21'};
queries['d_call_igh'] = {d_call: 'IGHD2-21*02'};
// B-Cell J-Gene
queries['jgene_family_igh'] = {ir_jgene_family: 'IGHJ4'};
queries['jgene_gene_igh'] = {ir_jgene_gene: 'IGHJ4'};
queries['j_call_igh'] = {j_call: 'IGHJ4*02'};
// T-Cell queries
// T-Cell V-Gene
queries['vgene_family_trb'] = {ir_vgene_family: 'TRBV20'};
queries['vgene_gene_trb'] = {ir_vgene_gene: 'TRBV20-1'};
queries['v_call_trb'] = {v_call: 'TRBV20-1*01'};
// T-Cell D-Gene
queries['dgene_family_trb'] = {ir_dgene_family: 'TRBD2'};
queries['dgene_gene_trb'] = {ir_dgene_gene: 'TRBD2'};
queries['d_call_trb'] = {d_call: 'TRBD2*01'};
// T-Cell J-Gene
queries['jgene_family_trb'] = {ir_jgene_family: 'TRBJ2'};
queries['jgene_gene_trb'] = {ir_jgene_gene: 'TRBJ2-3'};
queries['j_call_trb'] = {j_call: 'TRBJ2-3*01'};

// Get the time when query set up was finished.
query_setup_time = new Date();


/****************************************************************************************
 MAIN */

// Get the cache status before we start doing our queries. We want to track the 
// number of pages read into cache and the number of pages requested from cache
cacheData = db.serverStatus().wiredTiger.cache;
cachePagesReadStart = cacheData["pages read into cache"];
cachePagesRequestedStart = cacheData["pages requested from the cache"];
print("Start - Cache pages read = "+cachePagesReadStart + ", Cache pages requested = " + cachePagesRequestedStart);

// Get a list of all of the sample IDs in the repository. 
sample_id_list = db[collection].distinct('ir_project_sample_id');
sample_id_time = new Date();

// Execute the query for each query key in the list for all of the samples.
for (var key in queries) {           
       results[key] = do_query_for_all_samples(sample_id_list, queries[key]);
}
end_time = new Date();

// Dump out the output from the queries...
// First print headers line
print_headers(queries);

// For each result, print results
sample_id_list.forEach(function(sample_id, i) {
       var s = '' + sample_id + '\t';
       // For each query, print out how long the query took as well as 
       // the number of results returned from the query.
       for (var key in queries) {
              s+= results[key][sample_id]['duration'];
              s+= '\t';
              s+= results[key][sample_id]['count'];
              s+= '\t';
       }
       print(s);
});

// For each result, we also want to print the results from the Mongo
// explain results. For each result we gathered the query that was
// executed as well as the query plan used. In general, to achieve
// good performance in Mongo, Mongo should be using the COUNT_SCAN
// query type. This is where we can see if the correct query type
// is being used.
sample_id_list.forEach(function(sample_id, i) {
       for (var key in queries) {
              print(sample_id + ", " + key);
              print(results[key][sample_id]['parsedQuery']);
              print(results[key][sample_id]['winningPlan']);
       }
});

// Print timing
print("Total run time = " + (end_time - start_time)/1000);
print("Query setup time = " + (query_setup_time - start_time)/1000);
print("Sample ID time = " + (sample_id_time - query_setup_time)/1000);
print("Primary query time = " + (end_time - sample_id_time)/1000);
// Print cache performance
cacheData = db.serverStatus().wiredTiger.cache;
cachePagesReadEnd = cacheData["pages read into cache"];
cachePagesRequestedEnd = cacheData["pages requested from the cache"];
print("End - Cache pages read = " + cachePagesReadEnd + ", Cache pages requested = " + cachePagesRequestedEnd);
cachePageReads = cachePagesReadEnd - cachePagesReadStart;
cachePageRequests = cachePagesRequestedEnd - cachePagesRequestedStart;
cacheHitRatio = 1.0 - (cachePageReads/cachePageRequests);

print("Cache pages read = " + cachePageReads + ", Cache pages requested = " + cachePageRequests);
print("Cache hit ratio = " + cacheHitRatio);


/****************************************************************************************
 functions */

// Perform a query for a single key across all of the sample ids provided.
function do_query_for_all_samples(sample_id_list, filters) {
       var data = [];
       
       // For each sample id in our list, perform the query.
       sample_id_list.forEach(function(sample_id, i) {
              var t = [];
              
              // Perform the query for this sample for each of the filters.
              filters['ir_project_sample_id'] = sample_id;
              // Capture the resutls in the query indexed by the sample id
              data[sample_id] = do_query(filters);
       });

       // Return the data
       return data;
}

// Perform all of the queries for a single sample
function do_query(filters) {
       var t0, t1, data = [];

       // Get the count for each 
       t0 = new Date();
       data['count'] = db[collection].count(filters);
       t1 = new Date();
       // Calculate how long the query took.
       data['duration'] = (t1  - t0)/1000;
       // Ask Mongo to "explain" the query, and capture two key fields from the
       // query explanation, the query that was performed and the query plan that
       // was used. This allows us to track the query that was performed and the
       // indexes used to perform it. This will help to diagnose any performance
       // problems if a query performs poorly.
       explanation = db[collection].explain(true).count(filters);
       data['parsedQuery'] = JSON.stringify(explanation["queryPlanner"]["parsedQuery"])
       data['winningPlan'] = JSON.stringify(explanation["queryPlanner"]["winningPlan"])
       return data;
}

// Print out the header line, which consists of a header for each column
// in the query set.
function print_headers(queries) {
       var header_line = 'sample_id\t';
       for (var key in queries) {
              header_line+= key + '_time';
              header_line+= '\t';
              header_line+= key + '_results';
              header_line+= '\t';
       }
       print(header_line);   
}
