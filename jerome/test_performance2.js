// MongoDB collection
var collection = 'sequences';

// filter values
var filter_v_call = '^TRBV20-1\\*01';
var filter_j_call = '^TRBJ1-5\\*02';
var filter_d_call = '^TRBD2\\*01';
var filter_substring = 'CASSQVGTGVY';
var filter_junction_aa_length = 6;

// init result arrays
var equals = [];
var equals_res = [];
var substring = [];
var substring_res = [];
var vregex = [];
var vregex_res = [];
var dregex = [];
var dregex_res = [];
var jregex = [];
var jregex_res = [];
var total = [];
var total_res = [];
var range = [];
var range_res = [];

var data = [];
var filters = [];
var header_line = 'sample_id\t';

// get samples ids from *sequences* collection
var sample_id_list = db[collection].distinct('ir_project_sample_id');

/****************************************************************************************
 * functions
 ****************************************************************************************/


function do_query(filters) {
       var t0, t1, data = [];

       t0 = new Date();
       data['count'] = db[collection].count(filters);
       t1 = new Date();
       data['duration'] = (t1  - t0)/1000;

       return data;
}

function do_query_for_all_samples(sample_id_list, filters) {
       var data = [];
       
       sample_id_list.forEach(function(sample_id, i) {
              var t = [];
              
              filters['ir_project_sample_id'] = sample_id;
              data[sample_id] = do_query(filters);
       });

       return data;
}


/****************************************************************************************
 * MAIN
 ****************************************************************************************/

var queries = [], results = [];
queries['vregex'] = {"v_call": {"$regex": filter_v_call}};
queries['jregex'] = {"j_call": {"$regex": filter_j_call}};

// execute queries
for (var key in queries) {           
       results[key] = do_query_for_all_samples(sample_id_list, queries[key]);
}

// print results - headers
for (var key in results) {
       header_line+= key + ' time';
       header_line+= '\t';
       header_line+= key + ' results';
       header_line+= '\t';
}
print(header_line);


// print results - data
sample_id_list.forEach(function(sample_id, i) {
       var s = '' + sample_id + '\t';
       for (var key in results) {
              s+= results[key][sample_id]['duration'];
              s+= '\t';
              s+= results[key][sample_id]['count'];
              s+= '\t';
       }
       print(s);
});