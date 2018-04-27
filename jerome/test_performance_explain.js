// modified to run a query type for all samples ids and THEN move to another query type 

var collection = '', sample_id_list = [], queries = [], results = [];

/****************************************************************************************
 config */

collection = 'sequences';

queries['total'] = {};
queries['equals'] = {'junction_aa_length': 6};
queries['substring'] = {'substring': 'CASSQVGTGVY'};
//queries['vregex'] = {'v_call': {'$regex': '^TRBV20-1\\*01'}};
//queries['jregex'] = {'j_call': {'$regex': '^TRBJ1-5\\*02'}};
//queries['dregex'] = {'d_call': {'$regex': '^TRBD2\\*01'}};
queries['range'] = {'v_call': {"$gte":'TRBV20-1*01', "$lt":'TRBV20-1*02'}};

/****************************************************************************************
 MAIN */

// get samples ids directly from sequences collection
//sample_id_list = [110];
sample_id_list = db[collection].distinct('ir_project_sample_id');

// execute queries
for (var key in queries) {           
       results[key] = do_query_for_all_samples(sample_id_list, queries[key]);
}

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
              header_line+= key + ' time';
              header_line+= '\t';
              header_line+= key + ' results';
              header_line+= '\t';
       }
       print(header_line);   
}
