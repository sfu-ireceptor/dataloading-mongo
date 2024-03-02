#! /opt/ireceptor/data/bin/python
"""
 update_adaptive_genes.py is a script to update family, gene, and allele
 fields in the Rearrangement collection that have illegal gene names.

 The primary use for this script is to fix bad gene names that originate
 from Adaptive Biosciences incorrectly named gene calls from their 
 ImmunoSeq platform. The iReceptor Dataloader converts these badly named
 genes when Adaptive data is loaded, but this tool can be used to fix any
 badly named genes that were missed (in earlier generations of the Adaptive
 data loader for example).
"""
import os
import argparse
import time
import sys
import pandas as pd

# AIRR Mapping class.
from airr_map import AIRRMap
# Repository class - hides the DB implementation
from repository import Repository
# Rearrangement loader classes
from rearrangement import Rearrangement

# Get the command line arguments...
def getArguments():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Note: for proper data processing, project --samples metadata should\n" +
        "generally be read first into the database before loading other data types."
    )

    parser.add_argument("--version", action="version", version="%(prog)s 2.0")
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Run the program in verbose mode. This option will generate a lot of output, but is recommended from a data provenance perspective as it will inform you of how it mapped input data columns into repository columns.")
    parser.add_argument(
        "--skipload",
        action="store_true",
        help="Run the program without actually lodaing data into the repository. This option will allow testing of the entire load process without changing the repository.")
    parser.add_argument(
        "--update",
        action="store_true",
        help="Run the program in update mode rather than insert mode. This only works for repertoires.")

    # Add configuration options 
    config_group = parser.add_argument_group("Configuration file options", "")
    config_group.add_argument(
        "--mapfile",
        dest="mapfile",
        default="ireceptor.cfg",
        help="the iReceptor configuration file. Defaults to 'ireceptor.cfg' in the local directory where the command is run. This file contains the mappings between the AIRR Community field definitions, the annotation tool field definitions, and the fields and their names that are stored in the repository."
    )

    # Database options
    db_group = parser.add_argument_group("database options")
    db_group.add_argument(
        "--host",
        dest="host",
        default="localhost",
        help="MongoDb server hostname. Defaults to 'localhost'."
    )
    db_group.add_argument(
        "--port",
        dest="port",
        default=27017,
        type=int,
        help="MongoDb server port number. Defaults to 27017."
    )
    db_group.add_argument(
        "-u",
        "--user",
        dest="user",
        default=os.environ.get("MONGODB_SERVICE_USER", ""),
        help="MongoDb user name. Defaults to the MONGODB_SERVICE_USER environment variable if set. Defaults to empty string (no user name) otherwise."
    )
    db_group.add_argument(
        "-p",
        "--password",
        dest="password",
        default=os.environ.get("MONGODB_SERVICE_SECRET", ""),
        help="MongoDb service user account password. Defaults to the MONGODB_SERVICE_SECRET environment variable if set. Defaults to empty string (no password) otherwise."
    )
    db_group.add_argument(
        "-d",
        "--database",
        dest="database",
        default=os.environ.get("MONGODB_DB", "ireceptor"),
        help="Target MongoDb database. Defaults to the MONGODB_DB environment variable if set. Defaults to 'ireceptor' otherwise."
    )
    db_group.add_argument(
        "--database_map",
        dest="database_map",
        default="ir_repository",
        help="Mapping to use to map data terms into repository terms. Defaults to ir_repository, which is the mapping for the iReceptor Turnkey repository. This mapping keyword MUST be in the term mapping file as specified by --mapfile"
    )
    db_group.add_argument(
        "--database_chunk",
        dest="database_chunk",
        default=100000,
        type=int,
        help="Number of records to process in a single step when loading rearrangment data into the repository. This is used to reduce the memory footpring of the loading process when very large files are being loaded. Defaults to 100,000"
    )
    db_group.add_argument(
        "--repertoire_collection",
        dest="repertoire_collection",
        default="sample",
        help="The collection to use for storing and searching repertoires (sample metadata). This is the collection that sample metadata is inserted into when the --sample option is specified. Defaults to 'sample', which is the collection in the iReceptor Turnkey repository."
    )
    db_group.add_argument(
        "--rearrangement_collection",
        dest="rearrangement_collection",
        default="sequence",
        help="The collection to use for storing and searching rearrangements (sequence annotations). This is the collection that data is inserted into when the --mixcr, --imgt, and --airr options are used to load files. Defaults to 'sequence', which is the collection in the iReceptor Turnkey repository."
    )
    db_group.add_argument(
        "--clone_collection",
        dest="clone_collection",
        default="clone",
        help="The collection to use for storing and searching clones. This is the collection that data is inserted into when the --mixcr-clone option is used to load files. Defaults to 'clone', which is the collection in the iReceptor Turnkey repository."
    )
    db_group.add_argument(
        "--cell_collection",
        dest="cell_collection",
        default="cell",
        help="The collection to use for storing and searching cells. This is the collection that data is inserted into when the --airr-cell option is used to load files. Defaults to 'cell', which is the collection in the iReceptor Turnkey repository."
    )
    db_group.add_argument(
        "--expression_collection",
        dest="expression_collection",
        default="expression",
        help="The collection to use for storing and searching gene expression. This is the collection that data is inserted into when the --airr-expression option is used to load files. Defaults to 'expression', which is the collection in the iReceptor Turnkey repository."
    )

    # Application specific command line arguments
    path_group = parser.add_argument_group("gene options")
    parser.add_argument(
        "gene_field",
        help="Field in which the gene name is searched for, and if found, replaced."
    )
    parser.add_argument(
        "allele_field",
        help="Field in which the allele name is searched for, and if found, replaced."
    )
    parser.add_argument(
        "gene_map",
        help="File that contains two columns with headers, first column is a the Adaptive name of a gene while the second column is the correctly named gene name."
    )

    options = parser.parse_args()

    if options.verbose:
        print('HOST               :', options.host)
        print('PORT               :', options.port)
        print('USER               :', options.user[0] + (len(options.user) - 2) * "*" + options.user[-1] if options.user else "")
        print('PASSWORD           :', options.password[0] + (len(options.password) - 2) * "*" + options.password[-1] if options.password else "")
        print('DATABASE           :', options.database)
        print('DATABASE_MAP       :', options.database_map)
        print('MAPFILE            :', options.mapfile)
        print('FILE               :', options.file_map)

    return options

def processRearrangements(gene_field, allele_field, gene_map_df, repository, airr_map,
                          rearrangementParser, options):
    print('Info:')
    print('Info: Processing - gene_field = %s'%(gene_field))
    # Start timing the processing
    t_start = time.perf_counter()

    # Set the tag for the repository that we are using.
    repository_tag = rearrangementParser.getRepositoryTag()

    # Get the tag to use for iReceptor specific mappings
    ireceptor_tag = rearrangementParser.getiReceptorTag()

    # Get the sequence_id field that is used to identify unique
    # sequences in the repository.
    airr_sequence_id_field = airr_map.getMapping("rearrangement_id",
                                       ireceptor_tag, repository_tag,
                                       airr_map.getRearrangementClass())

    # Get the field in the repository that is used to store data update time
    updated_at_field = airr_map.getMapping("ir_updated_at_rearrangement",
                                           ireceptor_tag, repository_tag)

    # Get the repository field name for the gene field we are processing. 
    repo_gene_field = airr_map.getMapping(gene_field, ireceptor_tag, repository_tag,
                                          airr_map.getIRRearrangementClass())

    # Get the repository field name for the allele field we are processing. 
    repo_allele_field = airr_map.getMapping(allele_field, ireceptor_tag, repository_tag,
                                          airr_map.getIRRearrangementClass())
    print("Info: gene field = %s"%(gene_field))
    print("Info: repository gene field = %s"%(repo_gene_field))
    print("Info: allele field = %s"%(allele_field))
    print("Info: repository allele field = %s"%(repo_allele_field))

    # For each gene name we need to convert. This is iterating over the list of
    # bad gene names in the Adaptive produced data.
    for index, gene_map in gene_map_df.iterrows():
        print("Info: mapping %s to %s"%(gene_map["Adaptive"],gene_map["Fixed"]))
        # Get the counts for each bad gene and output some info.
        rearrangement_count = repository.countRearrangements(repo_gene_field,
                                                             gene_map["Adaptive"])
        print("Info: Found %d rearrangements with %s = %s"%
              (rearrangement_count, repo_gene_field, gene_map["Adaptive"]))
        # Query for the gene we want to fix.
        query = {repo_gene_field: {'$eq': gene_map["Adaptive"]}}
        rearrangement_cursor = repository.rearrangement.find(query)
        # For each rearrangement found with the bad gene, replace it
        # with the fixed gene.
        for rearrangement in rearrangement_cursor:
            this_sequence_id = rearrangement[airr_sequence_id_field]
            print("Info:     sequence_id = %s"%(this_sequence_id))

            # Rearrangements have lists of gene calls
            gene_list = rearrangement[repo_gene_field]
            print("Info:     %s"%(gene_list))
            new_gene_list = []
            update_needed = False
            # Iterate over the gene list and make the change.
            for gene in gene_list:
                print("Info:         %s"%(gene))
                if gene == gene_map["Adaptive"]:
                    update_needed = True
                    new_gene_list.append(gene_map["Fixed"])
                else:
                    new_gene_list.append(gene)
            print("Info:     %s"%(new_gene_list))

            
            # Do the same for the allele call.
            # Rearrangements have lists of allele calls
            allele_list = rearrangement[repo_allele_field]
            print("Info:     %s"%(allele_list))
            new_allele_list = []
            # Iterate over the allel list and make the change.
            for allele in allele_list:
                print("Info:         %s"%(allele))
                # If the allele is identical to the fix, then we need to make the fix. 
                # This means there is no allele denoted in the allele call.
                # If the allele is identical to the fix with a * appended (e.g. TRBD2-1*)
                # then we need to do the fix. We don't want to apply the fix to TRBD2-10
                # so the above catches this case.
                if allele == gene_map["Adaptive"] or gene_map["Adaptive"]+"*" in allele:
                    update_needed = True
                    new_allele_list.append(allele.replace(gene_map["Adaptive"],gene_map["Fixed"]))
                else:
                    new_allele_list.append(allele)
            print("Info:     %s"%(new_allele_list))
            # Set the rearrangement field to contain the new values. 
            #if update_needed:
            repository.updateRearrangementField(airr_sequence_id_field, this_sequence_id,
                                                repo_gene_field, new_gene_list,
                                                updated_at_field)
            repository.updateRearrangementField(airr_sequence_id_field, this_sequence_id,
                                                repo_allele_field, new_allele_list,
                                                updated_at_field)

                    
    return

    # Execute the query to find all rearrangemetns in the Rearrangement collection that are
    # associated with the rearrangement link ID (associated with the file). Note this DOES NOT
    # look at the file, it looks in the database to find all Rearrangements that are
    # associated with the file.
    query = {repo_gene_field: {'$eq': rearrangement_link_id}}
    rearrangement_cursor = repository.rearrangement.find(query)
    # Keep track of the number of updates as we iterate over the cursor.
    update_count = 0
    for rearrangement in rearrangement_cursor:
        #print("Info:     %s,%s,%s"%(
        #        rearrangement[airr_sequence_id_field],
        #        rearrangement[tool_cell_field],
        #        rearrangement[airr_cell_id_field]))
        # Sequence ID - needed to update this sequence in the repository
        this_sequence_id = rearrangement[airr_sequence_id_field]
        # Get the AIRR cell ID field, this is the field we overwrite.
        this_cell_id = rearrangement[airr_cell_id_field]
        # The cell dictionary is keyed on the tool cell ID, which is not unique in the DB.
        # If the rearrangement has a tool cell ID in the DB unique cell ID field, we are not
        # unique. If so we need to update the cell ID field in the rearrangement collection
        # with the unique cell id field which is in the dictionary. 
        if this_cell_id in cell_id_dict:
            # Get the Cell collection unique ID from the dictionary.
            repository_cell_id = cell_id_dict[this_cell_id]
            # Set the rearrangement cell_id to be the unqique cell_id from the Cell object.
            repository.updateRearrangementField(airr_sequence_id_field, this_sequence_id,
                                                airr_cell_id_field, repository_cell_id)
            # Add the sequence ID to the cell list of rearrangements.
            cell_seq_dict[repository_cell_id].append(this_sequence_id)
            # Update our count.
            update_count = update_count + 1
        else:
            # In this case we can't find the rearrangement cell ID in the dictionary. Why?
            if this_cell_id in cell_id_dict.values():
                # Check whether the dictionary contains this_cell_id in its values. If it does,
                # then it is likely that the rearrangement cell_id has already been set to be
                # the repository unique cell_id.
                print("Warning: Cell id for sequence %s already set (cell_id = %s)"%(this_sequence_id,this_cell_id))
            else:
                # If nothing then we could not find a cell for a sequence, print a warning.
                print("Warning: Could not find a Cell for sequence %s"%(this_sequence_id))
    # If we want to store rearrangement object in the Cell collection, we can do so by looping
    # over the sequence dictionary, but we need to check what is there, append, and make unique
    # so we don't have any duplicates. Not necessary so leaving out for now.
    #for repository_cell_id, sequence_list in cell_seq_dict.items():
    #    print("Info: %s %s"%(repository_cell_id,sequence_list))



    # time end
    print("Info: Update of %d rearrangements"%(update_count))
    t_end = time.perf_counter()
    print("Info: Finished processing in %f seconds (%f updates/s)"%(
           (t_end - t_start),(update_count/(t_end-t_start))),flush=True)
    return True

if __name__ == "__main__":
    # Get the command line arguments.
    options = getArguments()

    # Create the repository object, which establishes the repository connection.
    repository = Repository(options.user, options.password,
                            options.host, options.port,
                            options.database,
                            options.repertoire_collection,
                            options.rearrangement_collection,
                            options.clone_collection,
                            options.cell_collection,
                            options.expression_collection,
                            options.skipload, options.update,
                            options.verbose)
    # Check on the successful creation of the repository
    if repository is None or not repository:
        sys.exit(1)

    # Create the AIRR mapping object, which has the mapping of fields between
    # the various components. This is essentially a mapping between the AIRR
    # standard fields, the fields in the input file being parsed, and the fields
    # that are stored in the repository.
    airr_map = AIRRMap(options.verbose)
    airr_map.readMapFile(options.mapfile)
    if airr_map.getRearrangementMapColumn(options.database_map) is None:
        print("ERROR: Could not find repository mapping %s in AIRR Mappings"%
              (options.database_map))
        sys.exit(1)

    # Create parser objects. We don't actually parse these data, but we use the
    # objects to ensure we use the iReceptor fields in these objects correctly
    rearrangementParser = Rearrangement(options.verbose, options.database_map,
                                        options.database_chunk, airr_map, repository)
    # Get a time point so we can record timing for the update.
    t_total_start = time.perf_counter()
    # Open the gene map file - it has two columns, the gene name to replace and the
    # gene to use as a replacement. 
    gene_map_df = pd.read_csv(options.gene_map, sep='\t')
    # Process the rearrangements as per the gene_map file.
    processRearrangements(options.gene_field, options.allele_field, gene_map_df, repository,
                          airr_map, rearrangementParser, options)

    # Output timing
    t_total_end = time.perf_counter()
    print("Info: Finished total processing in {:.2f} mins".format((t_total_end - t_total_start) / 60))
    # Check final results and exit. If all are True we are good, otherwise give a ERROR message.
    if True:
        sys.exit(0)
    else:
        print('ERROR: one or more conversions failed.')
        sys.exit(1)
