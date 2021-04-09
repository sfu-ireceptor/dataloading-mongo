<?php

// Include Composer's autoloader. Assumes this is running in a container with
// composer installed in the / so the vendor directory is /vendor.
require '/vendor/autoload.php';

// Check command line arguments.
if ($argc != 3) {
	echo "usage: ".$argv[0]." study_id output_dir\n";
	exit(1);
}

$study_id = $argv[1];
$outdir = $argv[2];

// Keep track orf execution time.
$start_time = microtime(true);

	//helper function that takes in an array with alelle/gene/family data and sorts out 
	//  whether to add to a _exists or _unique array. Does not decide on whether it's 
	//  productive or not
	function process_gene_array($gene_array, &$stat_exists_array, &$stat_unique_array)
	{
		// If we have more than one gene in the array, process each.
		if (sizeof($gene_array) > 1)
	    	{
			// Iterate over the genes.
	    		foreach ($gene_array as $single_entry)
	    		{
				// Check if the entry exists and increment if so, 
				// otherwise set the entry to 1.
	    			if (isset ($stat_exists_array[$single_entry]))
	    				$stat_exists_array[$single_entry]++;
	    			else
	    				$stat_exists_array[$single_entry]= 1;
	    		}
	    	}
	    	else
	    	{
				// Handle the case where the gene array is empty. If it is
				// use the empty string as the key, otherwise use the gene
				// as the key.
				if (sizeof($gene_array) == 0) $gene = "";
				else $gene = $gene_array[0];
				// We want to count this as both unique and exists. So check
				// both, and if a count exists for this gene, increment,
				// if no count yet, set count to 1
	    			if (isset ($stat_unique_array[$gene]))
	    				$stat_unique_array[$gene]++;
	    			else
	    				$stat_unique_array[$gene]= 1;
	    			if (isset ($stat_exists_array[$gene]))
	    				$stat_exists_array[$gene]++;
	    			else
	    				$stat_exists_array[$gene]= 1;
	    	}

	}

        function count_rearrangement_gene($gene_field, &$stats_exists, &$stats_unique)
        {
		    if (is_a($gene_field, "MongoDB\Model\BSONArray"))
		    {
		    	process_gene_array($gene_field, $stats_exists, $stats_unique); 	
		    }
		    else
		    {  	if (isset($stats_unique[$gene_field]))
		    	{
		    		$stats_unique[$gene_field]++;
				//echo ".";
		    	}
		    	else
		    	{
		    		$stats_unique[$gene_field]=1;
				//echo ".";
		    	}
		    	if (isset($stats_exists[$gene_field]))
		    	{
		    		$stats_exists[$gene_field]++;
				//echo ".";
		    	}
		    	else
		    	{
		    		$stats_exists[$gene_field]=1;
				//echo ".";
		    	}		    	
		    }
        }

	// Helper function to simply set the stats array for each rearrangement
	function process_stats($rearrangement, &$stats, $gene_fields, $count_fields)
	{
	    // Get the gene data for each gene field from the rearrangement record.
	    foreach ($gene_fields as $gene_stat=>$gene_field)
	    {
	        $gene_data[$gene_field] = isset($rearrangement[$gene_field])? $rearrangement[$gene_field] :"";
                //echo "data: ".$gene_field."\n";
	    }
	    // Get the data for each count field from the rearrangement record.
	    foreach ($count_fields as $count_stat=>$count_field)
	    {
	        $count_data[$count_field] = isset($rearrangement[$count_field])? $rearrangement[$count_field] :"";
                //echo "data: ".$count_field." = ".$count_data[$count_field]."\n";
	    }
	    // Store the productive value for alter use.
	    $productive_string = 'productive';
	    $productive = isset($rearrangement[$productive_string])?$rearrangement[$productive_string] :false;
            //echo "productive: ".$productive."\n";

	    // Calculate the productive stats.
	    if ($productive)
	    {
		// Process the gene stats for each gene field. This iterates over the gene fields we
		// have stats for, tracking both the base stat name and the field from which
		// to calculate it.
		foreach ($gene_fields as $gene_stat=>$gene_field)
	    	{	
	            // Gene stats have two types, exists and unique. The count function handles
		    // both, so we need to pass it the stats arrays for both. The function calculates
		    // the exists and unique values based on the gene data provided.
		    $exist_tag = $gene_stat."_exists_productive";
		    $unique_tag = $gene_stat."_unique_productive";
		    //echo "Count: ".$exist_tag.",".$unique_tag." from ".$gene_field."\n";
                    count_rearrangement_gene($gene_data[$gene_field], $stats[$exist_tag], $stats[$unique_tag]);
	    	}

		// Process the count stats. These are simple test and increment stats
		foreach ($count_fields as $count_stat=>$count_field)
	    	{	
		    // We are handling the productive stat here, so we need to use the
	            // correct tag.
		    $tag = $count_stat."_productive";
		    //echo "Count: ".$tag." from ".$count_field."\n";
		    // The tag is the stat we want. We want to check to see if the 
		    // value we are looking at is set or not and if not set it, else
		    // increment the field value.
		    if (isset ($stats[$tag][$count_data[$count_field]]))
		    {
		    	$stats[$tag][$count_data[$count_field]]++;
		        //echo ".";
		    }
		    else
		    {
		    	$stats[$tag][$count_data[$count_field]]=1;
		        //echo ".";
		    }
	    	}
	    }

	    // Process the gene stats for each gene field. This iterates over the gene fields we
	    // have stats for, tracking both the base stat name and the field from which
	    // to calculate it.
	    foreach ($gene_fields as $gene_stat=>$gene_field)
	    {	
	        // Gene stats have two types, exists and unique. The count function handles
		// both, so we need to pass it the stats arrays for both. The function calculates
		// the exists and unique values based on the gene data provided.
	        $exist_tag = $gene_stat."_exists";
	        $unique_tag = $gene_stat."_unique";
	        //echo "Count: ".$exist_tag.",".$unique_tag." from ".$gene_field."\n";
                count_rearrangement_gene($gene_data[$gene_field], $stats[$exist_tag], $stats[$unique_tag]);
	    }

	    // Process the count stats. These are simple test and increment stats
            foreach ($count_fields as $count_stat=>$count_field)
    	    {	
	        $tag = $count_stat;
	        //echo "Count: ".$tag." from ".$count_field."\n";
	        //echo "Count: ".$count_data[$count_field]."\n";
		// The tag is the stat we want. We want to check to see if the 
		// value we are looking at is set or not and if not set it, else
		// increment the field value.
	        if (isset ($stats[$tag][$count_data[$count_field]]))
	        {
	    	    $stats[$tag][$count_data[$count_field]]++;
		    //echo ".";
	        }
	        else
	        {
	    	    $stats[$tag][$count_data[$count_field]]=1;
		    //echo ".";
	        }
    	    }
	}


	function generate_stats_line($repertoire_id_field, $repertoire_id, $stat_field, $stat_key, $stat_count)
	{
            return '{"'.$repertoire_id_field.'":"'.$repertoire_id.'", "name":"'.$stat_field.'", "value":"'.$stat_key.'", "count":'.$stat_count."}\n";
        }

        function output_stats($repertoire_id_field, $repertoire_id, $stats_array, $stats_field, $out_file)
        {
		if (sizeof($stats_array)>0)
		{
			foreach ($stats_array as $key=>$count)
			{   
                                $line = '{"'.$repertoire_id_field.'":"'.$repertoire_id.'", "name":"'.$stats_field.'", "value":"'.$key.'", "count":'.$count."}\n";
				#$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_call_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		fflush($out_file);
        }


	// Connect to MongoDB
        $m = new MongoDB\Client("mongodb://ireceptor-database:27017");
        echo "Connection to database successfully\n";

        // Select a database
        $db = $m->ireceptor;

	// Set up the collections to use to find data.
        $repertoire_collection_name = "sample";
        $rearrangement_collection_name = "sequence";
	$stat_collection_name = "stat";
	$repertoire_collection = $db->selectCollection($repertoire_collection_name);
	$rearrangement_collection   = $db->selectCollection($rearrangement_collection_name);
	$stat_collection   = $db->selectCollection($stat_collection_name);
	echo "Database ireceptor selected, collections = ".$repertoire_collection_name.", ".$rearrangement_collection_name.", ".$stat_collection_name."\n";

        // Set the field names we use to link repertoires and rearrangements.
	$study_id_field = "study_id";
        $repertoire_id_field = "ir_annotation_set_metadata_id";
        $rearrangement_id_field = "ir_annotation_set_metadata_id_rearrangement";
	// We need to track the string we use the describes the field  that determines
	// productive rearrangements.
	$productive_string = 'productive';

	// Dictionaries that contain the stats we want to generate as counts. Key
	// in the dictionary is the stat name, the value is the field in the database.
	// The base fields for the stats name are listed in the iR+ stats spec:
	//     https://github.com/ireceptor-plus/specifications/blob/master/stats-api.yaml
	//
	// For each of the gene fields, there are four stats. The spec says that there is
	// a unique and exists stat - which handles multiple gene calls in two different
	// ways. For each of the exists and uniques stats there is also a productive version
	// that is a stat for the rearrangements filtered on productive rearrangements only. 
	$gene_stats = Array(
            'v_call' => 'v_call',
            'v_family' => 'ir_vgene_family',
            'v_gene' => 'ir_vgene_gene',

            'd_call' => 'd_call',
            'd_family' => 'ir_dgene_family',
            'd_gene' => 'ir_dgene_gene',

            'j_call' => 'j_call',
            'j_family' => 'ir_jgene_family',
            'j_gene' => 'ir_jgene_gene',

            'c_call' => 'c_call',
            'c_family' => 'ir_cgene_family',
            'c_gene' => 'ir_cgene_gene'
        );
	// For each of the count fields, there are two stats, a productive version
	// that is a stat for the rearrangements filtered on productive rearrangements only
	// and a normal version which counts all rearrangemetns. 
        $count_stats = Array( 
            'junction_length' => 'junction_length',
            'junction_aa_length' => 'junction_aa_length'
	);

        // Get all of the repertoires.
	$repertoire_results = $repertoire_collection->find([$study_id_field=>$study_id]);
        // Example with just a single repertoire. You need to find a repertoire_id for this to work
        // in the repository of choice...
	// $repertoire_results = $repertoire_collection->find([$repertoire_id_field=>'5faed5aec0fea5f2fe906fc9']);

        // Get the repertoire_ids
	$repertoire_ids = Array();
	foreach ($repertoire_results as $repertoire)
	{
		$repertoire_ids[] = $repertoire[$repertoire_id_field];
	}
	echo "Generating stats for ".$study_id_field."=".$study_id.", found ".count($repertoire_ids)." repertoires\n";
        // For each repertoire_id, process the repertoire.
	foreach ($repertoire_ids as $repertoire_id)
	{
		$stats = Array();
                $repertoire_start_time = microtime(true);
		# Intialize the count stat arrays.
		foreach ($count_stats as $stat=>$field)
		{
			// For each count stat, we have the stat and the stat filtered with productive.
			//echo "Array: ".$stat." from ".$field."\n";
			$stats[$stat] = Array();
			//echo "Array: ".$stat."_".$productive_string." from ".$field."\n";
			$stats[$stat."_".$productive_string] = Array();
		}
		# Intialize the gene stat arrays.
		foreach ($gene_stats as $stat=>$field)
		{
			// For each gene stat, we have the stat as an exists and a unique stat.
			// A unique count for a gene is incremented if the gene is
			// an exact match of data value in the rearrangement.
			// A exists count for a gene is incremented if the gene is a member
			// of the list of genes annotated for the rearragement.
			// We also have one of each for productive genes as well.
			$stat_name = $stat."_unique";
			//echo "Array: ".$stat_name." from ".$field."\n";
			$stats[$stat_name] = Array();

			$stat_name = $stat."_exists";
			//echo "Array: ".$stat_name." from ".$field."\n";
			$stats[$stat_name] = Array();

			$stat_name = $stat."_unique_productive";
			//echo "Array: ".$stat_name." from ".$field."\n";
			$stats[$stat_name] = Array();

			$stat_name = $stat."_exists_productive";
			//echo "Array: ".$stat_name." from ".$field."\n";
			$stats[$stat_name] = Array();

		}

		$rearrangement_count = $rearrangement_collection->count([$rearrangement_id_field=>$repertoire_id]);
                echo "Processing repertoire ".$repertoire_id.", rearrangement count = " . $rearrangement_count . "\n";
		$rearrangement_count_productive = $rearrangement_collection->count([$rearrangement_id_field=>$repertoire_id, 'productive'=>True]);
		$rearrangement_result = $rearrangement_collection->find([$rearrangement_id_field=>$repertoire_id]);

		// process the rearrangements into stats arrays
		foreach ($rearrangement_result as $rearrangement) {
			//echo "processing rearrangement\n";
			process_stats($rearrangement, $stats, $gene_stats, $count_stats);
		}
	        //echo "DUMP\n";
	        //var_dump($stats);

		//output each array into a file of the name repertoireId_statName.json
		if (!is_dir($outdir))
		{
			mkdir($outdir, 0777, true);
		}

		$file_name = $outdir.'/'.$repertoire_id."_stats.json";
		$out_file = fopen ( $file_name, "w");
                echo "Writing stats to ".$file_name."\n";

                $line = generate_stats_line($repertoire_id_field, $repertoire_id, "rearrangement_count", "rearrangement_count", $rearrangement_count);
		fwrite($out_file, $line);
                $line = generate_stats_line($repertoire_id_field, $repertoire_id, "rearrangement_count_productive", "rearrangement_count_productive", $rearrangement_count_productive);
		fwrite($out_file, $line);
                $line = generate_stats_line($repertoire_id_field, $repertoire_id, "duplicate_count", "duplicate_count", $rearrangement_count);
		fwrite($out_file, $line);
                $line = generate_stats_line($repertoire_id_field, $repertoire_id, "duplicate_count_productive", "duplicate_count_productive", $rearrangement_count_productive);
		fwrite($out_file, $line);
		fflush($out_file);

                // Output V-gene stats

		//echo "DUMP STAT\n";
		foreach ($stats as $stat=>$data)
		{
			//echo "Output stat ".$stat."\n";
                        output_stats($repertoire_id_field, $repertoire_id, $data, $stat, $out_file);
		}

		fclose($out_file);
                $repertoire_end_time = microtime(true) - $repertoire_start_time;
                echo "Reperotire stats took " . $repertoire_end_time . " seconds\n";
	}

$end_time = microtime(true) - $start_time;
echo "It took us " . $end_time. " seconds\n";

?>
