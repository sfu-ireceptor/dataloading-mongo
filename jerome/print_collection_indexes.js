var collection = '', index_list = [];

/****************************************************************************************
 config */

collection = 'sequences';


/****************************************************************************************
 MAIN */

index_list = db[collection].getIndexes();
index_list.forEach(function(index, i) {
		var ns = index['ns'],
			index_collection = ns.split('.')[1];

		if(index_collection == collection) {
	       print(tojson(index['key']));
		}
});