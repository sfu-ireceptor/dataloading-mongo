var collection = '', index_list = [];

/****************************************************************************************
 config */

collection = 'sequences';


/****************************************************************************************
 MAIN */

db[collection].getIndexes().forEach(function(index) {
		var ns = index['ns'],
			index_collection = ns.split('.')[1];

		if(index_collection == collection) {
			index_list.push(tojson(index['key']));
		}
});

// // print as one JS array
// print('[' + index_list + ']');

index_list.forEach(function(index) {
		print(index);
})
