// modified to run a query type for all samples ids and THEN move to another query type 

var collection = '', sample_id_list = [], queries = [], results = [];

var start_time, query_setup_time, sample_id_time, end_time;

start_time = new Date();

/****************************************************************************************
 config */

// Change the collection based on which repository being used...
// This needs to be fixed!
// On big-mongo and Turnkey repository has a sequence collection (no s)
// collection = 'sequences';
// On mini-mongo repository has a sequences collection (with an s)
collection = 'sequence';

queries['total'] = {};
// Junction Length
queries['equals_9'] = {'junction_aa_length': 9};
queries['equals_15'] = {'junction_aa_length': 15};
// Junction substring
queries['junction'] = {'substring': 'CASSQVGTGVY'};
// B-Cell queries
// B-Cell V-Gene
queries['vgene_family_igh'] = {vgene_family: 'IGHV2'};
queries['vgene_gene_igh'] = {vgene_gene: 'IGHV2-5'};
queries['v_call_igh'] = {v_call: 'IGHV2-5*08'};
// B-Cell D-Gene
queries['dgene_family_igh'] = {dgene_family: 'IGHD2'};
queries['dgene_gene_igh'] = {dgene_gene: 'IGHD2-21'};
queries['d_call_igh'] = {d_call: 'IGHD2-21*02'};
// B-Cell J-Gene
queries['jgene_family_igh'] = {jgene_family: 'IGHJ4'};
queries['jgene_gene_igh'] = {jgene_gene: 'IGHJ4'};
queries['j_call_igh'] = {j_call: 'IGHJ4*02'};
// T-Cell queries
// T-Cell V-Gene
queries['vgene_family_trb'] = {vgene_family: 'TRBV20'};
queries['vgene_gene_trb'] = {vgene_gene: 'TRBV20-1'};
queries['v_call_trb'] = {v_call: 'TRBV20-1*01'};
// T-Cell D-Gene
queries['dgene_family_trb'] = {dgene_family: 'TRBD2'};
queries['dgene_gene_trb'] = {dgene_gene: 'TRBD2'};
queries['d_call_trb'] = {d_call: 'TRBD2*01'};
// T-Cell J-Gene
queries['jgene_family_trb'] = {jgene_family: 'TRBJ2'};
queries['jgene_gene_trb'] = {jgene_gene: 'TRBJ2-3'};
queries['j_call_trb'] = {j_call: 'TRBJ2-3*01'};
// Old deprecated queries
//queries['vregex'] = {'v_call': {'$regex': '^TRBV20-1\\*01'}};
//queries['jregex'] = {'j_call': {'$regex': '^TRBJ1-5\\*02'}};
//queries['dregex'] = {'d_call': {'$regex': '^TRBD2\\*01'}};
//queries['range'] = {'v_call': {"$gte":'TRBV20-1*01', "$lt":'TRBV20-1*02'}};
//queries['range_string'] = {'v_call_string':{"$gte":'IGHV2-5*08', "$lt":'IGHV2-5*09'}};
query_setup_time = new Date();


/****************************************************************************************
 MAIN */

// get samples ids directly from sequences collection
//sample_id_list = [110];
sample_id_list = db[collection].distinct('ir_project_sample_id');
sample_id_time = new Date();

// execute queries
for (var key in queries) {           
       results[key] = do_query_for_all_samples(sample_id_list, queries[key]);
}
end_time = new Date();
// print headers line
print_headers(queries);

// print results
sample_id_list.forEach(function(sample_id, i) {
       var s = '' + sample_id + '\t';
       for (var key in queries) {
              s+= results[key][sample_id]['duration'];
              s+= '\t';
              s+= results[key][sample_id]['count'];
              s+= '\t';
       }
       print(s);
});

// print explanations
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


/****************************************************************************************
 functions */

function do_query_for_all_samples(sample_id_list, filters) {
       var data = [];
       
       sample_id_list.forEach(function(sample_id, i) {
              var t = [];
              
              filters['ir_project_sample_id'] = sample_id;
              data[sample_id] = do_query(filters);
       });

       return data;
}

function do_query(filters) {
       var t0, t1, data = [];

       t0 = new Date();
       data['count'] = db[collection].count(filters);
       t1 = new Date();
       data['duration'] = (t1  - t0)/1000;
       explanation = db[collection].explain(true).count(filters);
       data['parsedQuery'] = JSON.stringify(explanation["queryPlanner"]["parsedQuery"])
       data['winningPlan'] = JSON.stringify(explanation["queryPlanner"]["winningPlan"])
       return data;
}

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
