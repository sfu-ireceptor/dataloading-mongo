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
		    #if (is_a($v_call, "MongoDB\Model\BSONArray"))
		    #{
		    #	process_gene_array($v_call, $stats_vcall_exists_productive, $stats_vcall_unique_productive); 	
		    #}
		    #else
		    #{  	if (isset($stats_vcall_unique_productive[$v_call]))
		    #	{
		    #		$stats_vcall_unique_productive[$v_call]++;
		    #	}
		    #	else
		    #	{
		    #		$stats_vcall_unique_productive[$v_call]=1;
		    #	}		    	
		    #}

		    #if (is_a($v_gene, "MongoDB\Model\BSONArray"))
		    #{
		    #	process_gene_array($v_gene, $stats_vgene_exists_productive, $stats_vgene_unique_productive); 	
		    #}
		    #else
		    #{
		    #	if (isset($stats_vgene_unique_productive[$v_gene]))
		    #	{
		    #		$stats_vgene_unique_productive[$v_gene]++;
		    #	}
		    #	else
		    #	{
		    #		$stats_vgene_unique_productive[$v_gene]=1;
		    #	}
		    #}
		    #if (is_a($v_family, "MongoDB\Model\BSONArray"))
		    #{
		    #	process_gene_array($v_family, $stats_vfamily_exists_productive, $stats_vfamily_unique_productive); 	
		    #}
		    #else
		    #{
		    #	if (isset($stats_vfamily_unique_productive[$v_family]))
		    #	{
		    #		$stats_vfamily_unique_productive[$v_family]++;
		    #	}
		    #	else
		    #	{
		    #		$stats_vfamily_unique_productive[$v_family]=1;
		    #	}
		    #}

	    	//process stats for D region
		    #if (is_a($d_call, "MongoDB\Model\BSONArray"))
		    #{
		    #	process_gene_array($d_call, $stats_dcall_exists_productive, $stats_dcall_unique_productive); 	
		    #}
		    #else
		    #{
		    #	if (isset($stats_dcall_unique_productive[$d_call]))
		    #	{
		    #		$stats_dcall_unique_productive[$d_call]++;
		    #	}
		    #	else
		    #	{
		    #		$stats_dcall_unique_productive[$d_call]=1;
		    #	}
		    #}
#
#		    if (is_a($d_gene, "MongoDB\Model\BSONArray"))
#		    {
#		    	process_gene_array($d_gene, $stats_dgene_exists_productive, $stats_dgene_unique_productive); 	
#		    }
#		    else
#		    {
#		    	if (isset($stats_dgene_unique_productive[$d_gene]))
#		    	{
#		    		$stats_dgene_unique_productive[$d_gene]++;
#		    	}
#		    	else
#		    	{
#		    		$stats_dgene_unique_productive[$d_gene]=1;
#		    	}
#		    }
#		    if (is_a($d_family, "MongoDB\Model\BSONArray"))
#		    {
#		    	process_gene_array($d_family, $stats_dfamily_exists_productive, $stats_dfamily_unique_productive); 	
#		    }
#		    else
#		    {
#		    	if (isset($stats_dfamily_unique_productive[$d_family]))
#		    	{
#		    		$stats_dfamily_unique_productive[$d_family]++;
#		    	}
#		    	else
#		    	{
#		    		$stats_dfamily_unique_productive[$d_family]=1;
#		    	}
#		    }
#
#	    	//process stats for J region
#		    if (is_a($j_call, "MongoDB\Model\BSONArray"))
#		    {
#		    	process_gene_array($j_call, $stats_jcall_exists_productive, $stats_jcall_unique_productive); 	
#		    }
#		    else
#		    {
#		    	if (isset($stats_jcall_unique_productive[$j_call]))
#		    	{
#		    		$stats_jcall_unique_productive[$j_call]++;
#		    	}
#		    	else
#		    	{
#		    		$stats_jcall_unique_productive[$j_call]=1;
#		    	}
#		    }
#
#		    if (is_a($j_gene, "MongoDB\Model\BSONArray"))
#		    {
#		    	process_gene_array($j_gene, $stats_jgene_exists_productive, $stats_jgene_unique_productive); 	
#		    }
#		    else
#		    {
#		    	if (isset($stats_jgene_unique_productive[$j_gene]))
#		    	{
#		    		$stats_jgene_unique_productive[$j_gene]++;
#		    	}
#		    	else
#		    	{
#		    		$stats_jgene_unique_productive[$j_gene]=1;
#		    	}
#		    }
#		    if (is_a($j_family, "MongoDB\Model\BSONArray"))
#		    {
#		    	process_gene_array($j_family, $stats_jfamily_exists_productive, $stats_jfamily_unique_productive); 	
#		    }
#		    else
#		    {
#		    	if (isset($stats_jfamily_unique_productive[$j_family]))
#		    	{
#		    		$stats_jfamily_unique_productive[$j_family]++;
#		    	}
#		    	else
#		    	{
#		    		$stats_jfamily_unique_productive[$j_family]=1;
#		    	}
#		    }

	    	//process stats for C region
#		    if (is_a($c_call, "MongoDB\Model\BSONArray"))
#		    {
#		    	process_gene_array($c_call, $stats_ccall_exists_productive, $stats_ccall_unique_productive); 	
#		    }
#		    else
#		    {
#		    	if (isset($stats_ccall_unique_productive[$c_call]))
#		    	{
#		    		$stats_ccall_unique_productive[$c_call]++;
#		    	}
#		    	else
#		    	{
#		    		$stats_ccall_unique_productive[$c_call]=1;
#		    	}
#		    }
#
#		    if (is_a($c_gene, "MongoDB\Model\BSONArray"))
#		    {
#		    	process_gene_array($c_gene, $stats_cgene_exists_productive, $stats_cgene_unique_productive); 	
#		    }
#		    else
#		    {
#		    	if (isset($stats_cgene_unique_productive[$c_gene]))
#		    	{
#		    		$stats_cgene_unique_productive[$c_gene]++;
#		    	}
#		    	else
#		    	{
#		    		$stats_cgene_unique_productive[$c_gene]=1;
#		    	}
#		    }
#		    if (is_a($c_family, "MongoDB\Model\BSONArray"))
#		    {
#		    	process_gene_array($c_family, $stats_cfamily_exists_productive, $stats_cfamily_unique_productive); 	
#		    }
#		    else
#		    {
#		    	if (isset($stats_cfamily_unique_productive[$c_family]))
#		    	{
#		    		$stats_cfamily_unique_productive[$c_family]++;
#		    	}
#		    	else
#		    	{
#		    		$stats_cfamily_unique_productive[$c_family]=1;
#		    	}
#		    }

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
#	    if (is_a($v_call, "MongoDB\Model\BSONArray"))
#	    {
#	    	process_gene_array($v_call, $stats_vcall_exists, $stats_vcall_unique); 	
#	    }
#	    else
#	    {
#	    	if (isset($stats_vcall_unique[$v_call]))
#	    	{
#	    		$stats_vcall_unique[$v_call]++;
#	    	}
#	    	else
#	    	{
#	    		$stats_vcall_unique[$v_call]=1;
#	    	}
#	    }
#
#	    if (is_a($v_gene, "MongoDB\Model\BSONArray"))
#	    {
#	    	process_gene_array($v_gene, $stats_vgene_exists, $stats_vgene_unique); 	
#	    }
#	    else
#	    {
#	    	if (isset($stats_vgene_unique[$v_gene]))
#	    	{
#	    		$stats_vgene_unique[$v_gene]++;
#	    	}
#	    	else
#	    	{
#	    		$stats_vgene_unique[$v_gene]=1;
#	    	}
#	    }
#	    if (is_a($v_family, "MongoDB\Model\BSONArray"))
#	    {
#	    	process_gene_array($v_family, $stats_vfamily_exists, $stats_vfamily_unique); 	
#	    }
#	    else
#	    {
#	    	if (isset($stats_vfamily_unique[$v_family]))
#	    	{
#	    		$stats_vfamily_unique[$v_family]++;
#	    	}
#	    	else
#	    	{
#	    		$stats_vfamily_unique[$v_family]=1;
#	    	}
#	    }
#
#    	//process stats for D region
#	    if (is_a($d_call, "MongoDB\Model\BSONArray"))
#	    {
#	    	process_gene_array($d_call, $stats_dcall_exists, $stats_dcall_unique); 	
#	    }
#	    else
#	    {
#	    	if (isset($stats_dcall_unique[$d_call]))
#	    	{
#	    		$stats_dcall_unique[$d_call]++;
#	    	}
#	    	else
#	    	{
#	    		$stats_dcall_unique[$d_call]=1;
#	    	}
#	    }
#
#	    if (is_a($d_gene, "MongoDB\Model\BSONArray"))
#	    {
#	    	process_gene_array($d_gene, $stats_dgene_exists, $stats_dgene_unique); 	
#	    }
#	    else
#	    {
#	    	if (isset($stats_dgene_unique[$d_gene]))
#	    	{
#	    		$stats_dgene_unique[$d_gene]++;
#	    	}
#	    	else
#	    	{
#	    		$stats_dgene_unique[$d_gene]=1;
#	    	}
#	    }
#	    if (is_a($d_family, "MongoDB\Model\BSONArray"))
#	    {
#	    	process_gene_array($d_family, $stats_dfamily_exists, $stats_dfamily_unique); 	
#	    }
#	    else
#	    {
#	    	if (isset($stats_dfamily_unique[$d_family]))
#	    	{
#	    		$stats_dfamily_unique[$d_family]++;
#	    	}
#	    	else
#	    	{
#	    		$stats_dfamily_unique[$d_family]=1;
#	    	}
#	    }
#
#    	//process stats for J region
#	    if (is_a($j_call, "MongoDB\Model\BSONArray"))
#	    {
#	    	process_gene_array($j_call, $stats_jcall_exists, $stats_jcall_unique); 	
#	    }
#	    else
#	    {
#	    	if (isset($stats_jcall_unique[$j_call]))
#	    	{
#	    		$stats_jcall_unique[$j_call]++;
#	    	}
#	    	else
#	    	{
#	    		$stats_jcall_unique[$j_call]=1;
#	    	}
#	    }
#
#	    if (is_a($j_gene, "MongoDB\Model\BSONArray"))
#	    {
#	    	process_gene_array($j_gene, $stats_jgene_exists, $stats_jgene_unique); 	
#	    }
#	    else
#	    {
#	    	if (isset($stats_jgene_unique[$j_gene]))
#	    	{
#	    		$stats_jgene_unique[$j_gene]++;
#	    	}
#	    	else
#	    	{
#	    		$stats_jgene_unique[$j_gene]=1;
#	    	}
#	    }
#	    if (is_a($j_family, "MongoDB\Model\BSONArray"))
#	    {
#	    	process_gene_array($j_family, $stats_jfamily_exists, $stats_jfamily_unique); 	
#	    }
#	    else
#	    {
#	    	if (isset($stats_jfamily_unique[$j_family]))
#	    	{
#	    		$stats_jfamily_unique[$j_family]++;
#	    	}
#	    	else
#	    	{
#	    		$stats_jfamily_unique[$j_family]=1;
#	    	}
#	    }
#
#    	//process stats for C region
#	    if (is_a($c_call, "MongoDB\Model\BSONArray"))
#	    {
#	    	process_gene_array($c_call, $stats_ccall_exists, $stats_ccall_unique); 	
#	    }
#	    else
#	    {
#	    	if (isset($stats_ccall_unique[$c_call]))
#	    	{
#	    		$stats_ccall_unique[$c_call]++;
#	    	}
#	    	else
#	    	{
#	    		$stats_ccall_unique[$c_call]=1;
#	    	}
#	    }
#
#	    if (is_a($c_gene, "MongoDB\Model\BSONArray"))
#	    {
#	    	process_gene_array($c_gene, $stats_cgene_exists, $stats_cgene_unique); 	
#	    }
#	    else
#	    {
#	    	if (isset($cgene_unique[$c_gene]))
#	    	{
#	    		$stats_cgene_unique[$c_gene]++;
#	    	}
#	    	else
#	    	{
#	    		$stats_cgene_unique[$c_gene]=1;
#	    	}
#	    }
#	    if (is_a($c_family, "MongoDB\Model\BSONArray"))
#	    {
#	    	process_gene_array($c_family, $stats_cfamily_exists, $stats_cfamily_unique); 	
#	    }
#	    else
#	    {
#	    	if (isset($stats_cfamily_unique[$c_family]))
#	    	{
#	    		$stats_cfamily_unique[$c_family]++;
#	    	}
#	    	else
#	    	{
#	    		$stats_cfamily_unique[$c_family]=1;
#	    	}
#	    }

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
        }

	echo "Database ireceptor selected\n";

	$repertoire_collection = $db->selectCollection("sample");
	$rearrangement_collection   = $db->selectCollection("sequence");

        #$repertoire_id_field = "_id";
        #$rearrangement_id_field = "ir_project_sample_id";
        $repertoire_id_field = "ir_annotation_set_metadata_id";
        $rearrangement_id_field = "ir_annotation_set_metadata_id_rearrangement";
	$repertoire_results = $repertoire_collection->find([$repertoire_id_field=>'5faed5aec0fea5f2fe906fc9']);
	#$repertoire_results = $repertoire_collection->find();
	$repertoire_ids = Array();

	foreach ($repertoire_results as $repertoire)
	{
                echo "Hello\n";
		$repertoire_ids[] = $repertoire[$repertoire_id_field];
	}
	foreach ($repertoire_ids as $repertoire_id)
	{
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
		$outdir = '/mnt/stats/'.$sample_id."/";
		if (!is_dir($outdir))
		{
			mkdir($outdir, 0777, true);
		}

		$file_name = $outdir.$sample_id."stats.json";
		$out_file = fopen ( $file_name, "w");

                $line = generate_stats_line($repertoire_id_field, $sample_id, "rearrangement_count", "rearrangement_count", $rearrangement_count);
		# $line = '{"'.$repertoire_id_field.'":"'.$sample_id.'", "name":"rearrangement_count", "value":"rearrangement_count", "count":'.$rearrangement_count."}\n";
		fwrite($out_file, $line);
                $line = generate_stats_line($repertoire_id_field, $sample_id, "rearrangement_count_productive", "rearrangement_count_productive", $rearrangement_count_productive);
		#$line = '{"ir_project_sample_id":'.$sample_id.', "name":"rearrangement_count_productive", "value":"rearrangement_count_productive", "count":'.$rearrangement_count_productive."}\n";
		fwrite($out_file, $line);
                $line = generate_stats_line($repertoire_id_field, $sample_id, "duplicate_count", "duplicate_count", $rearrangement_count);
		#$line = '{"ir_project_sample_id":'.$sample_id.', "name":"duplicate_count", "value":"duplicate_count", "count":'.$rearrangement_count."}\n";
		fwrite($out_file, $line);
                $line = generate_stats_line($repertoire_id_field, $sample_id, "duplicate_count_productive", "duplicate_count_productive", $rearrangement_count_productive);
		#$line = '{"ir_project_sample_id":'.$sample_id.', "name":"duplicate_count_productive", "value":"duplicate_count_productive", "count":'.$rearrangement_count_productive."}\n";
		fwrite($out_file, $line);

                # Output V-gene stats
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

                # Output D-gene stats
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

                # Output J-gene stats
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

                # Output C-gene stats
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
		#if (sizeof($stats_vcall_exists)>0)
		#{
		#	foreach ($stats_vcall_exists as $key=>$count)
		#	{   
		#		$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_call_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
		#		fwrite($out_file, $line);
		#	}
		#}
#		if (sizeof($stats_vgene_exists)>0)
#		{
#			foreach ($stats_vgene_exists as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_gene_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_vfamily_exists)>0)
#		{
#			foreach ($stats_vfamily_exists as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_family_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#
#		if (sizeof($stats_jcall_exists)>0)
#		{
#			foreach ($stats_jcall_exists as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"j_call_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_jgene_exists)>0)
#		{
#			foreach ($stats_jgene_exists as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"j_gene_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_jfamily_exists)>0)
#		{
#			foreach ($stats_jfamily_exists as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"j_family_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#
#		if (sizeof($stats_dcall_exists)>0)
#		{
#			foreach ($stats_dcall_exists as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"d_call_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_dgene_exists)>0)
#		{
#			foreach ($stats_dgene_exists as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"d_gene_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_dfamily_exists)>0)
#		{
#			foreach ($stats_dfamily_exists as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"d_family_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#
#		if (sizeof($stats_ccall_exists)>0)
#		{
#			foreach ($stats_ccall_exists as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"c_call_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_cgene_exists)>0)
#		{
#			foreach ($stats_cgene_exists as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"c_gene_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_cfamily_exists)>0)
#		{
#			foreach ($stats_cfamily_exists as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"c_family_exists", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}

#		if (sizeof($stats_vcall_unique)>0)
#		{
#			foreach ($stats_vcall_unique as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_call_unique", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_vgene_unique)>0)
#		{
#			foreach ($stats_vgene_unique as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_gene_unique", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_vfamily_unique)>0)
#		{
#			foreach ($stats_vfamily_unique as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_family_unique", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#
#		if (sizeof($stats_jcall_unique)>0)
#		{
#			foreach ($stats_jcall_unique as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"j_call_unique", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_jgene_unique)>0)
#		{
#			foreach ($stats_jgene_unique as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"j_gene_unique", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_jfamily_unique)>0)
#		{
#			foreach ($stats_jfamily_unique as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"j_family_unique", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		
#		if (sizeof($stats_dcall_unique)>0)
#		{
#			foreach ($stats_dcall_unique as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"d_call_unique", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_dgene_unique)>0)
#		{
#			foreach ($stats_dgene_unique as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"d_gene_unique", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_dfamily_unique)>0)
#		{
#			foreach ($stats_dfamily_unique as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"d_family_unique", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#
#		if (sizeof($stats_ccall_unique)>0)
#		{
#			foreach ($stats_ccall_unique as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"c_call_unique", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_cgene_unique)>0)
#		{
#			foreach ($stats_cgene_unique as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"c_gene_unique", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
#		if (sizeof($stats_cfamily_unique)>0)
#		{
#			foreach ($stats_cfamily_unique as $key=>$count)
#			{   
#				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"c_family_unique", "value":'.json_encode($key).', "count":'.$count."}\n";
#				fwrite($out_file, $line);
#			}
#		}
/*
		if (sizeof($stats_vcall_exists_productive)>0)
		{
			foreach ($stats_vcall_exists_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_call_exists_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_vgene_exists_productive)>0)
		{
			foreach ($stats_vgene_exists_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_gene_exists_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_vfamily_exists_productive)>0)
		{
			foreach ($stats_vfamily_exists_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_family_exists_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}

		if (sizeof($stats_jcall_exists_productive)>0)
		{
			foreach ($stats_jcall_exists_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"j_call_exists_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_jgene_exists_productive)>0)
		{
			foreach ($stats_jgene_exists_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"j_gene_exists_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_jfamily_exists_productive)>0)
		{
			foreach ($stats_jfamily_exists_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"j_family_exists_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}

		if (sizeof($stats_dcall_exists_productive)>0)
		{
			foreach ($stats_dcall_exists_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"d_call_exists_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_dgene_exists_productive)>0)
		{
			foreach ($stats_dgene_exists_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"d_gene_exists_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_dfamily_exists_productive)>0)
		{
			foreach ($stats_dfamily_exists_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"d_family_exists_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}

		if (sizeof($stats_ccall_exists_productive)>0)
		{
			foreach ($stats_ccall_exists_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"c_call_exists_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_cgene_exists_productive)>0)
		{
			foreach ($stats_cgene_exists_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"c_gene_exists_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_cfamily_exists_productive)>0)
		{
			foreach ($stats_cfamily_exists_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"c_family_exists_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}

		if (sizeof($stats_vcall_unique_productive)>0)
		{
			foreach ($stats_vcall_unique_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_call_unique_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_vgene_unique_productive)>0)
		{
			foreach ($stats_vgene_unique_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_gene_unique_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_vfamily_unique_productive)>0)
		{
			foreach ($stats_vfamily_unique_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"v_family_unique_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}

		if (sizeof($stats_jcall_unique_productive)>0)
		{
			foreach ($stats_jcall_unique_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"j_call_unique_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_jgene_unique_productive)>0)
		{
			foreach ($stats_jgene_unique_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"j_gene_unique_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_jfamily_unique_productive)>0)
		{
			foreach ($stats_jfamily_unique_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"j_family_unique_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}

		if (sizeof($stats_dcall_unique_productive)>0)
		{
			foreach ($stats_dcall_unique_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"d_call_unique_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_dgene_unique_productive)>0)
		{
			foreach ($stats_dgene_unique_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"d_gene_unique_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_dfamily_unique_productive)>0)
		{
			foreach ($stats_dfamily_unique_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"d_family_unique_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}

		
		if (sizeof($stats_ccall_unique_productive)>0)
		{
			foreach ($stats_ccall_unique_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"c_call_unique_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_cgene_unique_productive)>0)
		{
			foreach ($stats_cgene_unique_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"c_gene_unique_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_cfamily_unique_productive)>0)
		{
			foreach ($stats_cfamily_unique_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"c_family_unique_productive", "value":'.json_encode($key).', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}

		if (sizeof($stats_junction)>0)
		{
			foreach ($stats_junction as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"junction_length", "value":'.$key.', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_junction_aa)>0)
		{
			foreach ($stats_junction_aa as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"junction_aa_length", "value":'.$key.', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_junction_productive)>0)
		{
			foreach ($stats_junction_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"junction_length_productive", "value":'.$key.', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
		if (sizeof($stats_junction_aa_productive)>0)
		{
			foreach ($stats_junction_aa_productive as $key=>$count)
			{   
				$line = '{"ir_project_sample_id":'.$sample_id.', "name":"junction_aa_length_productive", "value":'.$key.', "count":'.$count."}\n";
				fwrite($out_file, $line);
			}
		}
*/
		fclose($out_file);
	}

$end_time = microtime(true) - $start_time;
echo "It took us " . $end_time. " seconds\n";

?>
