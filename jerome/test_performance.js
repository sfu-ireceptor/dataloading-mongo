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

// var sample_id_list = [2, 4, 5, 13, 14, 16, 17, 19, 21, 24, 27, 28, 29, 31, 34, 41, 42, 45, 47, 51, 52, 55, 59, 60, 63, 69, 72, 73, 74, 77, 78, 85, 86, 90, 97, 99, 100, 103, 104, 105, 110, 111, 112, 114, 115, 116, 118, 119, 121, 123, 124, 125, 128, 130, 133, 134, 138, 140, 144, 147, 148, 151, 153, 154, 158, 163, 164, 165, 166, 169, 175, 176, 179, 180, 182, 184, 185, 188, 190, 191, 194, 195, 198, 199, 204, 207, 210, 211, 218, 220, 221, 224, 227, 231, 232, 233, 236, 238, 242, 243, 244, 246, 250, 253, 255, 259, 261, 263, 264, 269, 270, 272, 276, 279, 282, 284, 286, 291, 292, 293, 294, 296, 299, 301, 302, 303, 306, 307, 308, 311, 312, 317, 320, 326, 327, 331, 332, 335, 337, 338, 343, 348, 349, 350, 353, 354, 357, 359, 360, 362, 365, 368, 370, 372, 373, 375, 376, 378, 380, 382, 383, 384, 385, 386, 391, 393, 394, 395, 397, 398, 400, 401, 403, 406, 409, 410, 411, 414, 415, 416, 418, 419, 429, 430, 432, 433, 434, 435, 437, 440, 442, 445, 453, 454, 455, 459, 461, 463, 466, 468, 469, 470, 471, 472, 475, 483, 484, 485, 486, 488, 489, 490, 491, 493, 495, 496, 500, 501, 503, 508, 511, 514, 515, 516, 517, 519, 521, 523, 526];
var sample_id_list = db.sequences.distinct('ir_project_sample_id');
sample_id_list.forEach(function(sample_id) {
       print('Doing queries for sample_id=' + sample_id);

       var t0 = new Date();
       var sequence_count = db.sequence.count({"ir_project_sample_id": sample_id, "v_call": {"$regex": '^TRBV20-1\\*01'}});
       var t1 = new Date();
       var duration = (t1  - t0)/1000;
       vregex[i] = duration;
       vregex_res[i] = sequence_count;

       var t0 = new Date();
       var sequence_count = db.sequence.count({"ir_project_sample_id": sample_id, "j_call": {"$regex": '^TRBJ1-5\\*02'}});
       var t1 = new Date();
       var duration = (t1  - t0)/1000;
       jregex[i] = duration;
       jregex_res[i] = sequence_count;

       var t0 = new Date();
       var sequence_count = db.sequence.count({"ir_project_sample_id": sample_id, "d_call": {"$regex": '^TRBD2\\*01'}});
       var t1 = new Date();
       var duration = (t1  - t0)/1000;
       dregex[i] = duration;
       dregex_res[i] = sequence_count;

       t0 = new Date();
       sequence_count = db.sequence.count({"ir_project_sample_id": sample_id, "substring": "CASSQVGTGVY"});
       t1 = new Date();
       duration = (t1  - t0)/1000;
       substring[i] = duration;
       substring_res[i] = sequence_count;

       t0 = new Date();
       sequence_count = db.sequence.count({"ir_project_sample_id": sample_id, "junction_aa_length": 6});
       t1 = new Date();
       duration = (t1  - t0)/1000;
       equals[i] = duration;
       equals_res[i] = sequence_count;

       t0 = new Date();
       sequence_count = db.sequence.count({"ir_project_sample_id": sample_id});
       t1 = new Date();
       duration = (t1  - t0)/1000;
       total[i] = duration;
       total_res[i] = sequence_count;
});

print("sample id\tequal time\tequal results\tsubstring time\tsubstring results\tvregex time\tvregex results\tjregex time\tjregex results\tdregex time\tdregex results\ttotal time\ttotal results");
sample_id_list.forEach(function(sample_id) {
       print (i+ "\t" + equals[i] + "\t" + equals_res[i] + "\t" + substring[i] + "\t" + substring_res[i] + "\t" +vregex[i] + "\t" + vregex_res[i] + "\t" +jregex[i] + "\t" + jregex_res[i]+ "\t" +dregex[i] + "\t" + dregex_res[i] + "\t" + total[i] + "\t" + total_res[i]);
});