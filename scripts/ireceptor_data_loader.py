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

	parser = optparse.OptionParser()
	
	parser.add_option('--sample', 
					action="store_const", 
					const='sample', 
					dest='type', 
					default='sample'
	)
	
	parser.add_option('--imgt',    
					action='store_const', 
					const='imgt',    
					dest='type'
	)
	
	#parser.add_option('--mixcr', 
	#				action='store_const', 
	#				const='mixcr', 
	#				dest='type'
	#)
	
	default_host =  os.environ.get('MONGODB_HOST', 'localhost')
	
	parser.add_option('--host', 
	                  dest="host", 
	                  default=default_host,
	                  help="MongoDb server hostname. If the MONGODB_HOST environment variable is set, it is used. Defaults to 'localhost' otherwise."
	                  )
	
	parser.add_option('--port', 
	                  dest="port", 
	                  default=27017,
	                  type="int",
	                  help="MongoDb server port number. Defaults to 27017." 
	                  )
	
	default_user =  os.environ.get('MONGODB_SERVICE_USER', 'admin')
	
	parser.add_option('-u', '--user',
	                  dest="user", 
	                  default=default_user,
	                  help="MongoDb service user name. Defaults to the MONGODB_SERVICE_USER environment variable if set. Defaults to 'admin' otherwise."
	                  )
	     
	default_password =  os.environ.get('MONGODB_PASSWORD', '')
	    
	parser.add_option('-p', '--password', 
	                  dest="password", 
	                  default=default_password,
	                  help="MongoDb service user account secret ('password'). Defaults to the MONGODB_PASSWORD environment variable if set. Defaults to empty string otherwise."
	                  )
	
	default_database = os.environ.get('MONGODB_DB', 'ireceptor')
	
	parser.add_option('-d', '--database', 
	                  dest="database", 
	                  default=default_database,
	                  help="Target MongoDb database. Defaults to the MONGODB_DB environment variable if set. Defaults to 'ireceptor' otherwise." 
	                  )
	                  
	parser.add_option('-l', '--library', 
	                  dest="library", 
	                  default=".",
	                  help="Path to 'library' directory of data files. Defaults to the current working directory."
	                  )
	                  
	parser.add_option('-f', '--filename', 
	                  dest="filename", 
	                  default="", 
	                  help="Name of file to load. Defaults to a 'csv' file with the --type name as the root name."
	                  )
	                  
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
		print('USER      :', options.user)
		print('PORT      :', options.port)
		print('PASSWORD  :', options.password)
		print('DATABASE  :', options.database)
		print('LIBRARY   :', options.library)
		print('FILENAME  :', options.filename)
		print('VERSION   :', options.version)
		print('VERBOSE   :', options.verbose)
		#print('REMAINING :', remainder)
	
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
	    
	    # Set Mongo db collection name to 
	    # data source type: samples, imgt, igblast?
	    collection = mng_db[
			_type2collection[options.type]
		]
	    
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

