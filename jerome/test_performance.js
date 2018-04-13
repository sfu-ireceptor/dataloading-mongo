var collection = 'sequences';

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

var sample_id_list = db[collection].distinct('ir_project_sample_id');
sample_id_list.forEach(function(sample_id, i) {
       print('Doing queries for sample_id=' + sample_id);

       var t0 = new Date();
       var sequence_count = db[collection].count({"ir_project_sample_id": sample_id, "v_call": {"$regex": '^TRBV20-1\\*01'}});
       var t1 = new Date();
       var duration = (t1  - t0)/1000;
       vregex[i] = duration;
       vregex_res[i] = sequence_count;

       var t0 = new Date();
       var sequence_count = db[collection].count({"ir_project_sample_id": sample_id, "j_call": {"$regex": '^TRBJ1-5\\*02'}});
       var t1 = new Date();
       var duration = (t1  - t0)/1000;
       jregex[i] = duration;
       jregex_res[i] = sequence_count;

       var t0 = new Date();
       var sequence_count = db[collection].count({"ir_project_sample_id": sample_id, "d_call": {"$regex": '^TRBD2\\*01'}});
       var t1 = new Date();
       var duration = (t1  - t0)/1000;
       dregex[i] = duration;
       dregex_res[i] = sequence_count;

       t0 = new Date();
       sequence_count = db[collection].count({"ir_project_sample_id": sample_id, "substring": "CASSQVGTGVY"});
       t1 = new Date();
       duration = (t1  - t0)/1000;
       substring[i] = duration;
       substring_res[i] = sequence_count;

       t0 = new Date();
       sequence_count = db[collection].count({"ir_project_sample_id": sample_id, "junction_aa_length": 6});
       t1 = new Date();
       duration = (t1  - t0)/1000;
       equals[i] = duration;
       equals_res[i] = sequence_count;

       t0 = new Date();
       sequence_count = db[collection].count({"ir_project_sample_id": sample_id});
       t1 = new Date();
       duration = (t1  - t0)/1000;
       total[i] = duration;
       total_res[i] = sequence_count;
});

print("sample id\tequal time\tequal results\tsubstring time\tsubstring results\tvregex time\tvregex results\tjregex time\tjregex results\tdregex time\tdregex results\ttotal time\ttotal results");
sample_id_list.forEach(function(sample_id, i) {
       print (i+ "\t" + equals[i] + "\t" + equals_res[i] + "\t" + substring[i] + "\t" + substring_res[i] + "\t" +vregex[i] + "\t" + vregex_res[i] + "\t" +jregex[i] + "\t" + jregex_res[i]+ "\t" +dregex[i] + "\t" + dregex_res[i] + "\t" + total[i] + "\t" + total_res[i]);
});