var collection = '', sample_id_list = [], queries = [], results = [];

/****************************************************************************************
 config */

collection = 'sequences';

queries['total'] = {};
queries['equals'] = {'junction_aa_length': 6};
queries['substring'] = {'substring': 'CASSQVGTGVY'};
queries['vregex'] = {'v_call': {'$regex': '^TRBV20-1\\*01'}};
queries['jregex'] = {'j_call': {'$regex': '^TRBJ1-5\\*02'}};
queries['dregex'] = {'d_call': {'$regex': '^TRBD2\\*01'}};

/****************************************************************************************
 MAIN */

// get samples ids directly from sequences collection
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