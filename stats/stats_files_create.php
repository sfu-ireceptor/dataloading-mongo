<?php

$start_time = microtime(true);

   // connect to mongodb

	require 'vendor/autoload.php'; // include Composer's autoloader
        #$m = new MongoDB\Client("mongodb://ireceptor-database:27017", array("username"=>"dataLoader", "password"=>"+#7saQkvrBbZN+-V"));
        $m = new MongoDB\Client("mongodb://ireceptor-database:27017");
        echo "Connection to database successfully\n";
        // select a database
        $db = $m->ireceptor;
/*
	$m = new MongoDB\Client("mongodb://localhost:27017");
	
	echo "Connection to database successfully";
	// select a database
	$db = $m->ireceptor;
*/
	//helper function that takes in an array with alelle/gene/family data and sorts out 
	//  whether to add to a _exists or _unique array. Does not decide on whether it's 
	//  productive or not
	function process_gene_array($gene_array, &$stat_exists_array, &$stat_unique_array)
	{
		if (sizeof($gene_array) > 1)
	    	{
	    		foreach ($gene_array as $single_entry)
	    		{
	    			if (isset ($stat_exists_array[$single_entry]))
	    			{
	    				$stat_exists_array[$single_entry]++;
	    			}
	    			else
	    			{
	    				$stat_exists_array[$single_entry]= 1;
	    			}
	    		}
	    	}
	    	else
	    	{
	    			if (isset ($stat_unique_array[$gene_array[0]]))
	    			{
	    				$stat_unique_array[$gene_array[0]]++;
	    			}
	    			else
	    			{
	    				$stat_unique_array[$gene_array[0]]= 1;
	    			}	    		
	    			if (isset ($stat_exists_array[$gene_array[0]]))
	    			{
	    				$stat_exists_array[$gene_array[0]]++;
	    			}
	    			else
	    			{
	    				$stat_exists_array[$gene_array[0]]= 1;
	    			}
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
		    	}
		    	else
		    	{
		    		$stats_unique[$gene_field]=1;
		    	}		    	
		    }
        }

	//helper function to simply set the stats array for each rearrangement
	function process_stats($rearrangement,
				&$stats_junction_aa, &$stats_junction, &$stats_junction_aa_productive, &$stats_junction_productive, 
				&$stats_vcall_exists, &$stats_vcall_unique, &$stats_vcall_exists_productive, &$stats_vcall_unique_productive, 
				&$stats_vgene_exists, &$stats_vgene_unique, &$stats_vgene_exists_productive, &$stats_vgene_unique_productive, 
				&$stats_vfamily_exists, &$stats_vfamily_unique, &$stats_vfamily_exists_productive, &$stats_vfamily_unique_productive, 
				&$stats_jcall_exists,  &$stats_jcall_unique, &$stats_jcall_exists_productive, &$stats_jcall_unique_productive, 
				&$stats_jgene_exists,  &$stats_jgene_unique, &$stats_jgene_exists_productive, &$stats_jgene_unique_productive, 
				&$stats_jfamily_exists,  &$stats_jfamily_unique, &$stats_jfamily_exists_productive, &$stats_jfamily_unique_productive, 
				&$stats_dcall_exists,  &$stats_dcall_unique, &$stats_dcall_exists_productive, &$stats_dcall_unique_productive, 
				&$stats_dgene_exists, &$stats_dgene_unique, &$stats_dgene_exists_productive, &$stats_dgene_unique_productive, 
				&$stats_dfamily_exists, &$stats_dfamily_unique, &$stats_dfamily_exists_productive, &$stats_dfamily_unique_productive, 
				&$stats_ccall_exists,  &$stats_ccall_unique, &$stats_ccall_exists_productive, &$stats_ccall_unique_productive, 
				&$stats_cfamily_exists,  &$stats_cfamily_unique, &$stats_cfamily_exists_productive, &$stats_cfamily_unique_productive, 
				&$stats_cgene_exists, &$stats_cgene_unique, &$stats_cgene_exists_productive, &$stats_cgene_unique_productive
				)
	{
	    $junction_length_string = 'junction_length';
	    $junction_aa_length_string = 'junction_aa_length';

	    $v_call_string = 'v_call';
	    $v_family_string = 'ir_vgene_family';
	    $v_gene_string = 'ir_vgene_gene';
    
	    $j_call_string = 'j_call';
	    $j_family_string = 'ir_jgene_family';
	    $j_gene_string = 'ir_jgene_gene';

	    $d_call_string = 'd_call';
	    $d_family_string = 'ir_dgene_family';
	    $d_gene_string = 'ir_dgene_gene';

	    $c_call_string = 'c_call';
	    $c_family_string = 'ir_cgene_family';
	    $c_gene_string = 'ir_cgene_gene';

	    $productive_string = 'productive';

	    $v_call = isset($rearrangement[$v_call_string])? $rearrangement[$v_call_string] :"";
	    $v_family = isset($rearrangement[$v_family_string])? $rearrangement[$v_family_string] :""; 
	    $v_gene = isset($rearrangement[$v_gene_string])? $rearrangement[$v_gene_string] :"";

	    $d_call = isset($rearrangement[$d_call_string])? $rearrangement[$d_call_string] :"";
	    $d_family = isset($rearrangement[$d_family_string])? $rearrangement[$d_family_string] :"";
	    $d_gene = isset($rearrangement[$d_gene_string])? $rearrangement[$d_gene_string] :"";
	    
	    $j_call = isset($rearrangement[$j_call_string])? $rearrangement[$j_call_string] :"";
	    $j_family = isset($rearrangement[$j_family_string])? $rearrangement[$j_family_string] :"";
	    $j_gene = isset($rearrangement[$j_gene_string])? $rearrangement[$j_gene_string] :"";

	    $c_call = isset($rearrangement[$c_call_string])? $rearrangement[$c_call_string] :"";
	    $c_family = isset($rearrangement[$c_family_string])? $rearrangement[$c_family_string] :"";
	    $c_gene = isset($rearrangement[$c_gene_string])? $rearrangement[$c_gene_string] :"";

	    $junction_length = isset($rearrangement[$junction_length_string])? $rearrangement[$junction_length_string]:0;
	    $junction_aa_length = isset($rearrangement[$junction_aa_length_string])? $rearrangement[$junction_aa_length_string]:0;

	    $productive = isset($rearrangement[$productive_string])?$rearrangement[$productive_string] :false;

	    // if V/D/J alele, gene or family are arrays, add any multiple values to the _exist stat
	    //   otherwise add to the appropriate _unique stat (or _productive in either case)
	    if ($productive)
	    {
	    	//process stats for V region
                count_rearrangement_gene($v_call, $stats_vcall_exists_productive, $stats_vcall_unique_productive);
                count_rearrangement_gene($v_gene, $stats_vgene_exists_productive, $stats_vgene_unique_productive);
		count_rearrangement_gene($v_family, $stats_vfamily_exists_productive, $stats_vfamily_unique_productive); 	

                count_rearrangement_gene($d_call, $stats_dcall_exists_productive, $stats_dcall_unique_productive);
                count_rearrangement_gene($d_gene, $stats_dgene_exists_productive, $stats_dgene_unique_productive);
		count_rearrangement_gene($d_family, $stats_dfamily_exists_productive, $stats_dfamily_unique_productive); 	

                count_rearrangement_gene($j_call, $stats_jcall_exists_productive, $stats_jcall_unique_productive);
                count_rearrangement_gene($j_gene, $stats_jgene_exists_productive, $stats_jgene_unique_productive);
		count_rearrangement_gene($j_family, $stats_jfamily_exists_productive, $stats_jfamily_unique_productive); 	

                count_rearrangement_gene($c_call, $stats_ccall_exists_productive, $stats_ccall_unique_productive);
                count_rearrangement_gene($c_gene, $stats_cgene_exists_productive, $stats_cgene_unique_productive);
		count_rearrangement_gene($c_family, $stats_cfamily_exists_productive, $stats_cfamily_unique_productive); 	

                // Process the junction length stats.
		if (isset ($stats_junction_productive[$junction_length]))
		{
		    	$stats_junction_productive[$junction_length]++;
		}
		else
		{
		    	$stats_junction_productive[$junction_length]=1;
		}

		if (isset ($stats_junction_aa_productive[$junction_aa_length]))
		{
		    	$stats_junction_aa_productive[$junction_aa_length]++;
		}
		else
		{
		    	$stats_junction_aa_productive[$junction_aa_length]=1;
		}
	    }

	    //process stats for V region
            count_rearrangement_gene($v_call, $stats_vcall_exists, $stats_vcall_unique);
            count_rearrangement_gene($v_gene, $stats_vgene_exists, $stats_vgene_unique);
	    count_rearrangement_gene($v_family, $stats_vfamily_exists, $stats_vfamily_unique); 	

            count_rearrangement_gene($d_call, $stats_dcall_exists, $stats_dcall_unique);
            count_rearrangement_gene($d_gene, $stats_dgene_exists, $stats_dgene_unique);
	    count_rearrangement_gene($d_family, $stats_dfamily_exists, $stats_dfamily_unique); 	

            count_rearrangement_gene($j_call, $stats_jcall_exists, $stats_jcall_unique);
            count_rearrangement_gene($j_gene, $stats_jgene_exists, $stats_jgene_unique);
	    count_rearrangement_gene($j_family, $stats_jfamily_exists, $stats_jfamily_unique); 	

            count_rearrangement_gene($c_call, $stats_ccall_exists, $stats_ccall_unique);
            count_rearrangement_gene($c_gene, $stats_cgene_exists, $stats_cgene_unique);
	    count_rearrangement_gene($c_family, $stats_cfamily_exists, $stats_cfamily_unique); 	

            // Do the junction length stats.
	    if (isset ($stats_junction[$junction_length]))
	    {
	    	$stats_junction[$junction_length]++;
	    }
	    else
	    {
	    	$stats_junction[$junction_length]=1; 
	    }

	    if (isset ($stats_junction_aa[$junction_aa_length]))
	    {
	    	$stats_junction_aa[$junction_aa_length]++;
	    }
	    else
	    {
	    	$stats_junction_aa[$junction_aa_length]=1;
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

        $repertoire_collection_name = "sample";
        $rearrangement_collection_name = "sequence";
	echo "Database ireceptor selected, collections = ".$repertoire_collection_name.", ".$rearrangement_collection_name."\n";
	$repertoire_collection = $db->selectCollection($repertoire_collection_name);
	$rearrangement_collection   = $db->selectCollection($rearrangement_collection_name);

        // Set the field name we use to link repertoires and rearrangements.
        #$repertoire_id_field = "_id";
        #$rearrangement_id_field = "ir_project_sample_id";
        $repertoire_id_field = "ir_annotation_set_metadata_id";
        $rearrangement_id_field = "ir_annotation_set_metadata_id_rearrangement";
        $rearrangement_id_field = "repertoire_id";

        // Get all of the repertoires.
	$repertoire_results = $repertoire_collection->find();
        // Example with just a single repertoire. You need to find a repertoire_id for this to work
        // in the repository of choice...
	// $repertoire_results = $repertoire_collection->find([$repertoire_id_field=>'5faed5aec0fea5f2fe906fc9']);
	$repertoire_ids = Array();

        // Get the repertoire_ids
	foreach ($repertoire_results as $repertoire)
	{
		$repertoire_ids[] = $repertoire[$repertoire_id_field];
	}
        // For each repertoire_id, process the repertoire.
	foreach ($repertoire_ids as $repertoire_id)
	{
                echo "Processing repertoire ".$repertoire_id."\n";
                $repertoire_start_time = microtime(true);
		$stats_junction = Array();
		$stats_junction_aa = Array();
		$stats_junction_productive = Array();
		$stats_junction_aa_productive = Array();

		$stats_vcall_unique = Array();
		$stats_vgene_unique =  Array();
		$stats_vfamily_unique =   Array();
		$stats_jcall_unique =   Array();
		$stats_jgene_unique =   Array();
		$stats_jfamily_unique =   Array();
		$stats_dcall_unique =   Array();
		$stats_dgene_unique =   Array();
		$stats_dfamily_unique =   Array();
		$stats_ccall_unique =   Array();
		$stats_cgene_unique =   Array();
		$stats_cfamily_unique =   Array();

		$stats_vcall_exists = Array();
		$stats_vgene_exists =  Array();
		$stats_vfamily_exists =   Array();
		$stats_jcall_exists =   Array();
		$stats_jgene_exists =   Array();
		$stats_jfamily_exists =   Array();
		$stats_dcall_exists =   Array();
		$stats_dgene_exists =   Array();
		$stats_dfamily_exists =   Array();
		$stats_ccall_exists =   Array();
		$stats_cgene_exists =   Array();
		$stats_cfamily_exists =   Array();

		$stats_vcall_unique_productive = Array();
		$stats_vgene_unique_productive =  Array();
		$stats_vfamily_unique_productive =   Array();
		$stats_jcall_unique_productive =   Array();
		$stats_jgene_unique_productive =   Array();
		$stats_jfamily_unique_productive =   Array();
		$stats_dcall_unique_productive =   Array();
		$stats_dgene_unique_productive =   Array();
		$stats_dfamily_unique_productive =   Array();
		$stats_ccall_unique_productive =   Array();
		$stats_cgene_unique_productive =   Array();
		$stats_cfamily_unique_productive =   Array();

		$stats_vcall_exists_productive = Array();
		$stats_vgene_exists_productive =  Array();
		$stats_vfamily_exists_productive =   Array();
		$stats_jcall_exists_productive =   Array();
		$stats_jgene_exists_productive =   Array();
		$stats_jfamily_exists_productive =   Array();
		$stats_dcall_exists_productive =   Array();
		$stats_dgene_exists_productive =   Array();
		$stats_dfamily_exists_productive =   Array();
		$stats_ccall_exists_productive =   Array();
		$stats_cgene_exists_productive =   Array();
		$stats_cfamily_exists_productive =   Array();

		$sample_id = $repertoire_id;

		$rearrangement_count = $rearrangement_collection->count([$rearrangement_id_field=>$sample_id]);
		$rearrangement_count_productive = $rearrangement_collection->count([$rearrangement_id_field=>$sample_id, 'productive'=>True]);
		$rearrangement_result = $rearrangement_collection->find([$rearrangement_id_field=>$sample_id]);

		//process the rearrangements into stats arrays
		foreach ($rearrangement_result as $rearrangement) {
			process_stats($rearrangement, $stats_junction_aa, $stats_junction, $stats_junction_aa_productive, $stats_junction_productive,
				$stats_vcall_exists, $stats_vcall_unique, $stats_vcall_exists_productive, $stats_vcall_unique_productive, 
				$stats_vgene_exists, $stats_vgene_unique, $stats_vgene_exists_productive, $stats_vgene_unique_productive, 
				$stats_vfamily_exists, $stats_vfamily_unique, $stats_vfamily_exists_productive, $stats_vfamily_unique_productive, 
				$stats_jcall_exists,  $stats_jcall_unique, $stats_jcall_exists_productive, $stats_jcall_unique_productive, 
				$stats_jgene_exists,  $stats_jgene_unique, $stats_jgene_exists_productive, $stats_jgene_unique_productive, 
				$stats_jfamily_exists,  $stats_jfamily_unique, $stats_jfamily_exists_productive, $stats_jfamily_unique_productive, 
				$stats_dcall_exists,  $stats_dcall_unique, $stats_dcall_exists_productive, $stats_dcall_unique_productive, 
				$stats_dgene_exists, $stats_dgene_unique, $stats_dgene_exists_productive, $stats_dgene_unique_productive, 
				$stats_dfamily_exists, $stats_dfamily_unique, $stats_dfamily_exists_productive, $stats_dfamily_unique_productive, 
				$stats_ccall_exists,  $stats_ccall_unique, $stats_ccall_exists_productive, $stats_ccall_unique_productive, 
				$stats_cfamily_exists,  $stats_cfamily_unique, $stats_cfamily_exists_productive, $stats_cfamily_unique_productive, 
				$stats_cgene_exists, $stats_cgene_unique, $stats_cgene_exists_productive, $stats_cgene_unique_productive
			);
		}

		//output each array into a file of the name repertoireId_statName.json
                $outdir = '/data/stats';
		if (!is_dir($outdir))
		{
			mkdir($outdir, 0777, true);
		}

		$file_name = $outdir.'/'.$sample_id."_stats.json";
		$out_file = fopen ( $file_name, "w");
                echo "Writing stats to ".$file_name."\n";

                $line = generate_stats_line($repertoire_id_field, $sample_id, "rearrangement_count", "rearrangement_count", $rearrangement_count);
		fwrite($out_file, $line);
                $line = generate_stats_line($repertoire_id_field, $sample_id, "rearrangement_count_productive", "rearrangement_count_productive", $rearrangement_count_productive);
		fwrite($out_file, $line);
                $line = generate_stats_line($repertoire_id_field, $sample_id, "duplicate_count", "duplicate_count", $rearrangement_count);
		fwrite($out_file, $line);
                $line = generate_stats_line($repertoire_id_field, $sample_id, "duplicate_count_productive", "duplicate_count_productive", $rearrangement_count_productive);
		fwrite($out_file, $line);
		fflush($out_file);

                // Output V-gene stats
                output_stats($repertoire_id_field, $sample_id, $stats_vcall_unique, "v_call_unique", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_vgene_unique, "v_gene_unique", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_vfamily_unique, "v_family_unique", $out_file);

                output_stats($repertoire_id_field, $sample_id, $stats_vcall_exists, "v_call_exists", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_vgene_exists, "v_gene_exists", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_vfamily_exists, "v_family_exists", $out_file);

                output_stats($repertoire_id_field, $sample_id, $stats_vcall_unique_productive, "v_call_unique_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_vgene_unique_productive, "v_gene_unique_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_vfamily_unique_productive, "v_family_unique_productive", $out_file);

                output_stats($repertoire_id_field, $sample_id, $stats_vcall_exists_productive, "v_call_exists_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_vgene_exists_productive, "v_gene_exists_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_vfamily_exists_productive, "v_family_exists_productive", $out_file);
		fflush($out_file);

                // Output D-gene stats
                output_stats($repertoire_id_field, $sample_id, $stats_dcall_unique, "d_call_unique", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_dgene_unique, "d_gene_unique", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_dfamily_unique, "d_family_unique", $out_file);

                output_stats($repertoire_id_field, $sample_id, $stats_dcall_exists, "d_call_exists", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_dgene_exists, "d_gene_exists", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_dfamily_exists, "d_family_exists", $out_file);

                output_stats($repertoire_id_field, $sample_id, $stats_dcall_unique_productive, "d_call_unique_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_dgene_unique_productive, "d_gene_unique_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_dfamily_unique_productive, "d_family_unique_productive", $out_file);

                output_stats($repertoire_id_field, $sample_id, $stats_dcall_exists_productive, "d_call_exists_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_dgene_exists_productive, "d_gene_exists_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_dfamily_exists_productive, "d_family_exists_productive", $out_file);
		fflush($out_file);

                // Output J-gene stats
                output_stats($repertoire_id_field, $sample_id, $stats_jcall_unique, "j_call_unique", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_jgene_unique, "j_gene_unique", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_jfamily_unique, "j_family_unique", $out_file);

                output_stats($repertoire_id_field, $sample_id, $stats_jcall_exists, "j_call_exists", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_jgene_exists, "j_gene_exists", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_jfamily_exists, "j_family_exists", $out_file);

                output_stats($repertoire_id_field, $sample_id, $stats_jcall_unique_productive, "j_call_unique_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_jgene_unique_productive, "j_gene_unique_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_jfamily_unique_productive, "j_family_unique_productive", $out_file);

                output_stats($repertoire_id_field, $sample_id, $stats_jcall_exists_productive, "j_call_exists_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_jgene_exists_productive, "j_gene_exists_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_jfamily_exists_productive, "j_family_exists_productive", $out_file);
		fflush($out_file);

                // Output C-gene stats
                output_stats($repertoire_id_field, $sample_id, $stats_ccall_unique, "c_call_unique", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_cgene_unique, "c_gene_unique", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_cfamily_unique, "c_family_unique", $out_file);

                output_stats($repertoire_id_field, $sample_id, $stats_ccall_exists, "c_call_exists", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_cgene_exists, "c_gene_exists", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_cfamily_exists, "c_family_exists", $out_file);

                output_stats($repertoire_id_field, $sample_id, $stats_ccall_unique_productive, "c_call_unique_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_cgene_unique_productive, "c_gene_unique_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_cfamily_unique_productive, "c_family_unique_productive", $out_file);

                output_stats($repertoire_id_field, $sample_id, $stats_ccall_exists_productive, "c_call_exists_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_cgene_exists_productive, "c_gene_exists_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_cfamily_exists_productive, "c_family_exists_productive", $out_file);
		fflush($out_file);

                // Output the junction stats.
                output_stats($repertoire_id_field, $sample_id, $stats_junction, "junction_length", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_junction_aa, "junction_aa_length", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_junction_productive, "junction_length_productive", $out_file);
                output_stats($repertoire_id_field, $sample_id, $stats_junction_aa_productive, "junction_aa_length_productive", $out_file);

		fclose($out_file);
                $repertoire_end_time = microtime(true) - $repertoire_start_time;
                echo "Reperotire stats took " . $repertoire_end_time . " seconds\n";
	}

$end_time = microtime(true) - $start_time;
echo "It took us " . $end_time. " seconds\n";

?>
