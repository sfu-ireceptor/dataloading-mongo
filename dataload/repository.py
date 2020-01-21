import os
import urllib.parse
import pymongo

class Repository:
    def __init__(self, user, password, host, port, database, repertoire_collection, rearrangement_collection, skipload):
        """Create an interface to the Mongo repository

        Keyword arguments:
          - user: username for the repository - Empty string is OK
          - password: password for the user on the repository - Empty string is OK
          - host: host on which to connect for the repository
          - port: port on which to connect for the repository
          - database: name of the database to use in the repository
          - repertoire_collection: name of the repertoire collection in the database
          - rearrangement_collection: name of the rearrangement collection in the database
          - skipload: flag to determine if we skip the data load operation.

        """

        self.username = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.repertoire_collection = repertoire_collection
        self.rearrangement_collection = rearrangement_collection
        self.skipload = skipload

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
            uri = 'mongodb://%s:%s@%s:%s' % (self.username, self.password,
                                             self.host, self.port)
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
            # This exception is not an error. The cursor.next() raises this exception
            # when it has no more data in the cursor. In this case, this would mean
            # that the database is empty, but the database was opened and the query
            # worked. So this is not an error case as it OK to have an empty database.
            pass


        # Set Mongo db name and keep track of the mongo entry points to make queries.
        self.mongo_db = self.mongo_client[self.database]
        self.repertoire = self.mongo_db[self.repertoire_collection]
        self.rearrangement = self.mongo_db[self.rearrangement_collection]

    # Look for the search_name given in the repertoire collection in the search_field in
    # the repository. NOTE: This is a regurlar expression search (string contains).
    # Return an array of IDs which are the IDs from repertoire_field
    # where the search_name was found in the field search_field.
    # Return None on error, return empty array if not found.
    def getRepertoireIDs(self, repertoire_field, search_field, search_name):
        query =  {search_field: {'$regex': search_name}}
        idarray = []
        try:
            repertoire_cursor = self.repertoire.find(query, {repertoire_field: 1})
            for repertoire in repertoire_cursor:
                idarray.append(repertoire[repertoire_field])
        except Exception as err:
            print("ERROR: Search for repertoire field failed, field = %s"%(str(err)))
            return None
            
        return idarray

    # Look for the search_name given in the repertoire collection in the search_field in
    # the repository. Return an array of the repertoire which are repertoitre_field
    # where the search_name was found in the field search_field.
    # Return None on error, return empty array if not found.
    def getRepertoires(self, search_field, search_name):
        query =  {search_field: {'$eq': search_name}}
        rep_array = []
        try:
            repertoire_cursor = self.repertoire.find(query)
            for repertoire in repertoire_cursor:
                rep_array.append(repertoire)
        except Exception as err:
            print("ERROR: Search for repertoire field failed, field = %s"%(str(err)))
            return None
            
        return rep_array

    # Write the set of JSON records provided to the "rearrangements" collection.
    # This is hiding the Mongo implementation. Probably should refactor the
    # repository implementation completely.
    # Return True on success False on failure.
    def insertRearrangements(self, json_records):
        if not self.skipload:
            try:
                record_ids = self.rearrangement.insert(json_records)
            except Exception as err:
                print("ERROR: Unable to write records to repository, %s"%(err))
                return None
        return record_ids

    # Update the update_field to update_value wherever search_field is equal to
    # search value.
    def updateRearrangementField(self, search_field, search_value,
                                 update_field, update_value):
        if not self.skipload:
            update = {"$set": {update_field:update_value}}
            self.rearrangement.update( {search_field:search_value}, update)

    # Count the number of rearrangements that belong to a specific repertoire. 
    # Return -1 on error. Note: In our early implementations, we had an
    # internal field name called ir_project_sample_id. We want to hide
    # this and just talk about reperotire IDs, so this is hidden in the
    # Rearrangement class...
    def countRearrangements(self, repertoire_field, repertoire_id):
        if repertoire_field is None or repertoire_id is None:
            print("ERROR: Invalid repertoire field (%s) or repertoire_id (%s)"%
                  (repertoire_field, repertoire_id))
            return -1
        query = {repertoire_field:{'$eq':repertoire_id}}
        try:
            rearrangement_count = self.rearrangement.find(query).count()
        except Exception as err:
            print("ERROR: Query failed for repertoire field (%s) or repertoire_id (%s)"%
                  (repertoire_field, repertoire_id))
            return -1

        return rearrangement_count

    # Update the update_field to update_value wherever search_field is equal to
    # search value.
    def updateField(self, search_field, search_value, update_field, update_value):
        if not self.skipload:
            update = {"$set": {update_field:update_value}}
            self.repertoire.update( {search_field:search_value}, update)

    # Insert a repertoire document into the repertoire collection. Generates a 
    # unique identifier for the record and stores that ID in the field provided
    # in the link_field field for refernce without having to access the internal
    # _id.  Returns the string value of the record ID on success, return 
    # None on failure.
    def insertRepertoire( self, doc, link_field ):
        try:
            if not self.skipload:
                results = self.repertoire.insert(doc)
        except Exception as err:
            print("ERROR: Repository insertion failed, %s"%(err))
            return None

        # Store the internal ID as a string in the link_field.
        self.updateField("_id", results, link_field, str(results))
        # Return a string repersentation of the internal ID
        return str(results)

    # Insert a repertoire document into the repertoire collection. Generates a 
    # unique integer ID for the record, based on the largest interger stored 
    # thus far and increments it. Uses record 1 if there are no records in the
    # repository yet. Returns the record ID on success, return -1 on failure.
    def insertRepertoireOld( self, doc, link_field ):
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
            rep_id = 1
        else:
            # If it isn't empty, check the type... If it is an integer, generate a 
            # new unique identifier and use that for the record.
            rep_id = record["_id"]
            if not type(rep_id) is int:
                print("ERROR: Invalid ID for samples found, expecting an integer, got " + str(rep_id))
                print("ERROR: DB may be corrupt")
                return None
            else:
                rep_id = rep_id + 1

        # Add the ID to the record we are writing.
        doc["_id"] = rep_id
        # Add the ID to the link_field if one was provided. This allows us to track the
        # ID field with another field in the repository without exposing the internal
        # Mongo field.
        if link_field is None:
            print("ERROR: Must provide a link field for rearrangements")
            return None
        else:
            doc[link_field] = rep_id

        # Write the record and return
        try:
            if not self.skipload:
                results = self.repertoire.insert(doc)
                print(results)
        except Exception as err:
            print("ERROR: Repository insertion failed, %s"%(err))
            return None
        return rep_id

