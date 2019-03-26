// Dump the plan cache list
var cache_list = db.runCommand( { planCacheListFilters: "sequence" } )
print ("Plan cache list:" + JSON.stringify(cache_list, null, 4))

