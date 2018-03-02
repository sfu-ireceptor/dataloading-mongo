var n = db.sequence.count();
print("DB contains " + n + " sequences");

var t1 = new Date();
db.sequences.createIndex({ "substring" : 1, "ir_project_sample_id" : 1 });
var t2 = new Date();
var duration = Math.ceil((t2  - t1)/1000);
print(duration);
