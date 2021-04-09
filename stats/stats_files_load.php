<?php

// Keep track orf execution time.
$start_time = microtime(true);

// include Composer's autoloader
require '/vendor/autoload.php';


// Connect to MongoDB
$m = new MongoDB\Client("mongodb://ireceptor-database:27017");
echo "Connection to database successfully\n";

// Select a database and get the stats collection
$db = $m->ireceptor;
$stat_collection_name = "stat";
$stat_collection   = $db->selectCollection($stat_collection_name);

// We expect a list of files that have a single JSON object per line. 
// We iterate over the files.
for ($i=1; $i < $argc; $i++)
{
    echo "Reading stats from ".$argv[$i]."\n";
    $stats_file = fopen ( $argv[$i], "r");
    // We iterate over each line in the file.
    while(! feof($stats_file))
    {
	// For each line in the file, load the line as a JSON document
	// into mongo.
	$document_str = fgets($stats_file);
	$document = json_decode($document_str);
        //echo "-".$document_str."-\n";
	if (!is_null($document)) $stat_collection->insertOne($document);
    }
    // Close the file.
    fclose($stats_file);
}

// We are done...
$end_time = microtime(true) - $start_time;
echo "It took us " . $end_time. " seconds\n";

?>
