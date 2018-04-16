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

// get samples ids from *sequences* collection
var sample_id_list = db[collection].distinct('ir_project_sample_id');

function do_query(filters) {
       var t0, t1, data = [];

       t0 = new Date();
       data['count'] = db[collection].count(filters);
       t1 = new Date();
       data['duration'] = (t1  - t0)/1000;

       return data;
}

function do_query_for_all_samples(sample_id_list, $filters) {
       var data = [];
       
       sample_id_list.forEach(function(sample_id, i) {
              var t = [];
              
              filters['ir_project_sample_id'] = sample_id;
              data[i] = do_query(filters);
       });

       return data;
}

print('before');

filters['v_call'] = {"$regex": filter_v_call};
data = do_query_for_all_samples(sample_id_list, filters);

print('ok');
print(data);
print('done');

// sample_id_list.forEach(function(sample_id, i) {
//        var t0, t1, duration, sequence_count;
       
//        t0 = new Date();
//        sequence_count = db[collection].count({"ir_project_sample_id": sample_id, "v_call": {"$regex": filter_v_call}});
//        t1 = new Date();
//        duration = (t1  - t0)/1000;
//        vregex[i] = duration;
//        vregex_res[i] = sequence_count;

// });

// print("sample id\tequal time\tequal results\tsubstring time\tsubstring results\tvregex time\tvregex results\tjregex time\tjregex results\tdregex time\tdregex results\ttotal time\ttotal results");
// print (sample_id + "\t" + equals[i] + "\t" + equals_res[i] + "\t" + substring[i] + "\t" + substring_res[i] + "\t" + vregex[i] + "\t" + vregex_res[i] + "\t" +jregex[i] + "\t" + jregex_res[i]+ "\t" +dregex[i] + "\t" + dregex_res[i] + "\t" + total[i] + "\t" + total_res[i]);
