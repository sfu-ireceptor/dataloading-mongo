import os
import urllib.parse
import pymongo

class Repository:
    def __init__(self, user, password, host, port, database, repertoire_collection, rearrangement_collection):
        """Create an interface to the Mongo repository

        Keyword arguments:
          - user: username for the repository - Empty string is OK
          - password: password for the user on the repository - Empty string is OK
          - host: host on which to connect for the repository
          - port: port on which to connect for the repository
          - database: name of the database to use in the repository
          - repertoire_collection: name of the repertoire collection in the database
          - rearrangement_collection: name of the rearrangement collection in the database

        """

        self.username = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.repertoire_collection = repertoire_collection
        self.rearrangement_collection = rearrangement_collection

        self.repertoire = None
        self.rearrangement = None

        # Connect with Mongo db
        self.username = urllib.parse.quote_plus(self.username)
        self.password = urllib.parse.quote_plus(self.password)
        if len(self.username) == 0 and len(self.password) == 0:
            uri = 'mongodb://%s:%s' % (self.host, self.port)
            print("Info: Connecting to Mongo with no username/password on '%s:%s'" %
                (self.host, self.port))
        else:
            uri = 'mongodb://%s:%s@%s:%s' % (self.username, self.password, self.host, self.port)
            print("Info: Connecting to Mongo as user '%s' on '%s:%s'" %
                (self.username, self.host, self.port))

        # Connect to the Mongo server and return if not able to connect.
        try:
            self.mongo_client = pymongo.MongoClient(uri)
        except pymongo.errors.ConfigurationError as err:
            print("ERROR: Unable to connect to %s:%s - %s"
                    % (self.host, self.port, err))
            return None

        # Constructor doesn't block - need to check to see if the connection works.
        try:
            # We need to check that we can perform a real operation on the collection
            # at this time. We want to check for connection errors, authentication
            # errors. We want to let through the case that there is an empty repository
            # and the cursor comes back empty.
            self.mongo_db = self.mongo_client[self.database]
            self.repertoire = self.mongo_db[self.repertoire_collection]

            cursor = self.repertoire.find( {}, { "_id": 1 } ).sort("_id", -1).limit(1)
            record = cursor.next()
        except pymongo.errors.ConnectionFailure:
            print("ERROR: Unable to connect to %s:%s, Mongo server not available"
                    % (self.host, self.port))
            return None
        except pymongo.errors.OperationFailure as err:
            print("ERROR: Operation failed on %s:%s, %s"
                    % (self.host, self.port, str(err)))
            return None
        except StopIteration:
            # This exception is not an error. The cursor.next() raises this exception when it has no more
            # data in the cursor. In this case, this would mean that the database is empty,
            # but the database was opened and the query worked. So this is not an error case as it
            # OK to have an empty database.
            pass


        # Set Mongo db name and keep track of the mongo entry points to make queries.
        self.mongo_db = self.mongo_client[self.database]
        self.repertoire = self.mongo_db[self.repertoire_collection]
        self.rearrangement = self.mongo_db[self.rearrangement_collection]

    # Look for the file_name given in the samples collection in the file_field field in
    # the repository. Return an array of integers which are the sample IDs where the
    # file_name was found in the field field_name.
    def getRepertoireIDs(self, file_field, file_name):
        repertoire_cursor = self.repertoire.find( {file_field: { '$regex': file_name }}, {'_id': 1})
        idarray = [reperotire['_id'] for reperotire in repertoire_cursor]
        return idarray

    # Write the set of JSON records provided to the "rearrangements" collection.
    # This is hiding the Mongo implementation. Probably should refactor the
    # repository implementation completely.
    def insertRearrangements(self, json_records):
        self.rearrangement.insert(json_records)

    # Count the number of rearrangements that belong to a specific repertoire. Note: In our
    # early implementations, we had an internal field name called ir_project_sample_id. We
    # want to hide this and just talk about reperotire IDs, so this is hidden in the
    # Rearrangement class...
    def countRearrangements(self, repertoire_field, repertoire_id):
        rearrangement_count = self.rearrangement.find(
                {repertoire_field:{'$eq':repertoire_id}}
            ).count()
        return rearrangement_count



    # Update the count for the given reperotire and count field. 
    def updateCount(self, repertoire_id, count_field, count):
        self.repertoire.update(
            {"_id":repertoire_id}, {"$set": {count_field:count}}
        )

    # Insert a repertoire document into the repertoire collection
    def insertRepertoire( self, doc ):
        # We want to get a single record, sorted, so we can get the latest ID
        cursor = self.repertoire.find( {}, { "_id": 1 } ).sort("_id", -1).limit(1)

        # Check to see if the collection is empty (we get a StopIteration exception)
        empty = False
        try:
            record = cursor.next()
        except StopIteration:
            print("Info: No previous record, this is the first insertion")
            empty = True

        if empty:
            # If the cursor is empty, then this is the first record in the repertoire
            # collection. So we set the repertoire ID to 1.
            seq = 1
        else:
            # If it isn't empty, check the type... If it is an integer, generate a 
            # new unique identifier and use that for the record.
            seq = record["_id"]
            if not type(seq) is int:
                print("ERROR: Invalid ID for samples found, expecting an integer, got " + str(seq))
                print("ERROR: DB may be corrupt")
                return False
            else:
                seq = seq + 1

        # Add the ID to the record we are writing.
        doc["_id"] = seq

        # Write the record and return
        try:
            results = self.repertoire.insert(doc)
        except:
            print("ERROR: Repository insertion failed")
            return False
        return True

