#!/usr/bin/python3

import os
from os.path import exists

import pandas as pd
import urllib.parse
import pymongo
import json
import optparse

from sample import Sample
from imgt   import IMGT
#from mixcr  import MiXCR


_type2ext = {
		"sample" : "csv",
		"imgt"   : "zip", # assume a zip archive
		"mixcr"  : "zip", # assume a zip archive
}

def inputParameters():

	parser = optparse.OptionParser(
						usage="%prog [options]\n"+
							  "Note: for proper data processing, project --samples metadata should\n"+
							   "generally be read first into the database before loading other data types.",
	                    version='1.0',
    				)

	mode_opts = optparse.OptionGroup(
						    parser, 'Data Type Options',
						    'Options to specify the type of data to load.',
						    )
	
	mode_opts.add_option('--sample', 
					action="store_const", 
					const='sample', 
					dest='type', 
					default='sample'
	)
	
	mode_opts.add_option('--imgt',    
					action='store_const', 
					const='imgt',    
					dest='type'
	)
	
	#mode_opts.add_option('--mixcr', 
	#				action='store_const', 
	#				const='mixcr', 
	#				dest='type'
	#)
		
	parser.add_option_group(mode_opts) 
	
	db_opts = optparse.OptionGroup(
						    parser, 'Database Connection Options',
						    'These options control access to the database.',
						    )
	
	default_host =  os.environ.get('MONGODB_HOST', 'localhost')
	
	db_opts.add_option('--host', 
	                  dest="host", 
	                  default=default_host,
	                  help="MongoDb server hostname. If the MONGODB_HOST environment variable is set, it is used. Defaults to 'localhost' otherwise."
	                  )
	
	db_opts.add_option('--port', 
	                  dest="port", 
	                  default=27017,
	                  type="int",
	                  help="MongoDb server port number. Defaults to 27017." 
	                  )
	
	default_user =  os.environ.get('MONGODB_USER', 'admin')
	
	db_opts.add_option('-u', '--user',
	                  dest="user", 
	                  default=default_user,
	                  help="MongoDb service user name. Defaults to the MONGODB_USER environment variable if set. Defaults to 'admin' otherwise."
	                  )
	     
	default_password =  os.environ.get('MONGODB_PASSWORD', '')
	    
	db_opts.add_option('-p', '--password', 
	                  dest="password", 
	                  default=default_password,
	                  help="MongoDb service user account secret ('password'). Defaults to the MONGODB_PASSWORD environment variable if set. Defaults to empty string otherwise."
	                  )
	
	default_database = os.environ.get('MONGODB_DB', 'ireceptor')
	
	db_opts.add_option('-d', '--database', 
	                  dest="database", 
	                  default=default_database,
	                  help="Target MongoDb database. Defaults to the MONGODB_DB environment variable if set. Defaults to 'ireceptor' otherwise." 
	                  )
	
	parser.add_option_group(db_opts) 
	
	data_opts = optparse.OptionGroup(
						    parser, 'Data Source Options',
						    'These options specify the identity and location of data files to be loaded.',
						    )
	
	data_opts.add_option('-l', '--library', 
	                  dest="library", 
	                  default=".",
	                  help="Path to 'library' directory of data files. Defaults to the current working directory."
	                  )
	                  
	data_opts.add_option('-f', '--filename', 
	                  dest="filename", 
	                  default="", 
	                  help="Name of file to load. Defaults to a 'csv' file with the --type name as the root name."
	                  )
	                  
	db_opts = optparse.OptionGroup(
						    parser, 'General Options',
						    'Options for general information.',
						    )
	
	parser.add_option_group(data_opts) 

	parser.add_option('-v', '--verbose',
	                  dest="verbose",
	                  default=False,
	                  action="store_true",
	                  )
	                  
	parser.add_option('--version',
	                  dest="version",
	                  default=1.0,
	                  type="float",
	                  )
	                  
	options, remainder = parser.parse_args()

	if not options.filename:
		options.filename = options.type + "." + _type2ext[options.type]

	if options.verbose:
		print('INPUT TYPE:', options.type)
		print('HOST      :', options.host)
		print('USER      :', options.user[0]+"***"+options.user[-1])
		print('PORT      :', options.port)
		print('PASSWORD  :', options.password[0]+"***"+options.password[-1])
		print('DATABASE  :', options.database)
		print('LIBRARY   :', options.library)
		print('FILENAME  :', options.filename)
		print('VERSION   :', options.version)
	
	return options

class Context:
	
	def __init__(self, library, path, samples, sequences  ):
		self.library = library
		self.path = path
		self.samples = samples
		self.sequences = sequences
		
def getContext(options):

	if not options.filename: 
	    return False
	   
	if options.library:
		path = options.library + "/" + options.filename
	else:
		path = filename
	
	if not exists(path): 
	    return None
	   
	else:
		
		# Connect with Mongo db
	    username = urllib.parse.quote_plus(options.user)
	    password = urllib.parse.quote_plus(options.password)
	    uri = 'mongodb://%s:%s@%s:%s' % ( username, password, options.host, options.port )
	    
	    mng_client = pymongo.MongoClient(uri)
		
	    # Set Mongo db name
	    mng_db = mng_client[options.database]
	    
	    return  Context( 
					options.library, 
					path , 
					mng_db['sample'], 
					mng_db['sequence'] 
				)

if __name__ == "__main__":
	
	options = inputParameters()
	
	context = getContext(options)

	if context:
		
		if options.type == "sample":
			
			# process samples
			print("processing Sample metadata file: ",options.filename)
			
			sample = Sample(context)
			
			if sample.process():
				print("Sample metadata file loaded")
			else:
				print("ERROR: Sample input file not found?")
			
		elif options.type == "imgt":

			# process imgt
			
			print("processing IMGT data file: ",options.filename)
			
			imgt = IMGT(context)

			if imgt.process():
				print("IMGT data file loaded")
			else:
				print("ERROR: IMGT data file not found?")
			
		elif options.type == "mixcr":
			
			# process mixcr
			
			print("Processing MiXCR data file: ",options.filename)
			
			mixcr = MiXCR(context)
			
			if mixcr.process():
				print("MiXCR data file loaded")
			else:
				print("ERROR: MiXCR data file not found?")
		else:
			print( "ERROR: unknown input data type:", options.type )

