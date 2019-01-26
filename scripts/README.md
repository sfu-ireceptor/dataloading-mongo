# Overview

This directory contains the core python code for loading data into the iReceptor Platform. It consists of a single data loading script that can load either repertoire metadata files or rearrangement annotation files. It supports a simple UTF-8 encoded Comma Separated Values (CSV) file for repertoire metadata loading as well as a supporting the loading of rearrangement files as produced by a number of widely used annotation tools (IMGT HighV-QUEST, MiXCR, and igblast).

# Usage

Usage of dataloader.py is quite straight forward. You run it as a python script, providing command line arguments to minimally describe the type of file that you are loading and the file that you want to load. A minimal command line would be:

- python dataloader.py --sample -f PRJNA248411_Palanichamy_2018-12-18.csv
- python dataloader.py --imgt -f SRR1298731.txz

The first command above would load a repertiore metadata file, in this case the repertoire metadata from a study "Immunoglobulin class-switched B cells provide an active immune axis between CNS and periphery in multiple sclerosis" by Palanichamy et. al. (https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4176763/).

The second command would load a single rearrangement file, in the IMGT HighV-QUEST format, for that same study. It is important to note that the data loader utilizes the rearrangement file name (SRR1298731.txz) to link the rearrangements in the file to the appropriate repertoire in the study. This means that:

1) The repertoire metadata file must be loaded before the rearrangement file is loaded, and
2) The rearrangement file name MUST appear in the field ir_rearrangement_file_name in one, and only one, of the rows in the repertoire metadata file.

If this is not the case, the dataloader will produce an error message and will refuse to load the rearrangement file.

