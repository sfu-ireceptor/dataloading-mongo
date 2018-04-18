// modified to print only total query time and total nb of results for each query type

var collection = '', sample_id_list = [], queries = [], results = [], output = '';

/****************************************************************************************
 config */

collection = 'sequences';

queries['total'] = {};
// queries['equals'] = {'junction_aa_length': 6};
// queries['substring'] = {'substring': 'CASSQVGTGVY'};
// queries['vregex'] = {'v_call': {'$regex': '^TRBV20-1\\*01'}};
// queries['jregex'] = {'j_call': {'$regex': '^TRBJ1-5\\*02'}};
// queries['dregex'] = {'d_call': {'$regex': '^TRBD2\\*01'}};

// queries['equals'] = {'junction_aa_length': 7};
// queries['substring'] = {'substring': 'CASSQVGTG'};
// queries['vregex'] = {'v_call': {'$regex': '^TRBV20-2\\*01'}};
// queries['jregex'] = {'j_call': {'$regex': '^TRBJ1-4\\*02'}};
// queries['dregex'] = {'d_call': {'$regex': '^TRBD1\\*01'}};

db[collection].distinct('junction_aa_length').forEach(function(length, i) {
       queries['equals' + length] = {'junction_aa_length': length};
});
/****************************************************************************************
 MAIN */

// get samples ids directly from sequences collection
sample_id_list = db[collection].distinct('ir_project_sample_id');

// execute queries
for (var key in queries) {           
       results[key] = do_query_for_all_samples(sample_id_list, queries[key]);
}

// print headers line
//print_headers(queries);

// print results
for (var key in queries) {
       output+= results[key]['count'];
       output+= '\t';
       output+= Math.ceil(results[key]['duration']);
       output+= '\t';
}
print(output);

/****************************************************************************************
 functions */

function do_query_for_all_samples(sample_id_list, filters) {
       var t=[], count = 0, duration = 0;
       
       sample_id_list.forEach(function(sample_id, i) {
              var result = [];
              
              filters['ir_project_sample_id'] = sample_id;
              result = do_query(filters);
              
              count += result['count'];
              duration += result['duration']
       });

       t['count'] = count;
       t['duration'] = duration;
       return t;
}

function do_query(filters) {
       var t0, t1, data = [];

       t0 = new Date();
       data['count'] = db[collection].count(filters);
       t1 = new Date();
       data['duration'] = (t1  - t0)/1000;

       return data;
}

function print_headers(queries) {
       var header_line = '';
       for (var key in queries) {
              header_line+= key + ' results';
              header_line+= '\t';
              header_line+= key + ' time';
              header_line+= '\t';
       }
       print(header_line);   
}