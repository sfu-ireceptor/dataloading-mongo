var cache_list = db.runCommand( { planCacheListFilters: "sequences" } )
print ("Cache list:" + JSON.stringify(cache_list, null, 4))
