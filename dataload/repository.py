import os
import urllib.parse
import pymongo
from parser import Parser

class Repository:
    def __init__(self, user, password, host, port, database,
                 repertoire_collection, rearrangement_collection,
                 clone_collection, cell_collection, expression_collection,
                 skipload, update, verbose=False):
        """Create an interface to the Mongo repository

        Keyword arguments:
          - user: username for the repository - Empty string is OK
          - password: password for the user on the repository - Empty string is OK
          - host: host on which to connect for the repository
          - port: port on which to connect for the repository
          - database: name of the database to use in the repository
          - repertoire_collection: name of the repertoire collection in the database
          - rearrangement_collection: name of the rearrangement collection in the database
          - clone_collection: name of the clone collection in the database
          - cell_collection: name of the cell collection in the database
          - expression_collection: name of the expression collection in the database
          - skipload: flag to determine if we skip the data load operation.
          - update: flag to determine if we are updating rather than inserting
            (repertoire only).
        """

        self.username = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.repertoire_collection = repertoire_collection
        self.rearrangement_collection = rearrangement_collection
        self.clone_collection = clone_collection
        self.cell_collection = cell_collection
        self.expression_collection = expression_collection
        self.skipload = skipload
        self.update = update
        self.verbose = verbose

        self.repertoire = None
        self.rearrangement = None
        self.clone = None
        self.cell = None
        self.expression = None

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
        self.clone = self.mongo_db[self.clone_collection]
        self.cell = self.mongo_db[self.cell_collection]
        self.expression = self.mongo_db[self.expression_collection]


    # Return the update flag so clients can determine if we are in update mode or not.
    def updateOnly(self):
        return self.update

    # Look for the search_name given in the repertoire collection in the search_field in
    # the repository. NOTE: This is an exact match on an array element. The field
    # being searched must be an array.
    # Return an array of IDs which are the IDs from repertoire_field
    # where the search_name was found in the field search_field.
    # Return None on error, return empty array if not found.
    def getRepertoireIDs(self, repertoire_field, search_field, search_name):
        # Build the query. This is primarily used for searching for repertoires that
        # link to a specific file. Originally the field was a comma separated set of strings
        # so we used query = {search_field: {'$regex': search_name}}. Now that we are
        # using an array of stings we want to use an exact match. We can take advantage of
        # the fact that Mongo will query each array element for an exact match with this
        # query.
        query = {search_field: {'$eq': search_name}} 
        idarray = []
        try:
            # Perform the query and build an array of the resulting values of the fields
            # requested.
            repertoire_cursor = self.repertoire.find(query, {repertoire_field: 1})
            for repertoire in repertoire_cursor:
                idarray.append(repertoire[repertoire_field])
        except Exception as err:
            print("ERROR: Search for repertoire field failed, field = %s"%(search_field))
            print("ERROR: Error message = %s"%(str(err)))
            return None
            
        return idarray

    # Look for the search_name given in the repertoire collection in the search_field in
    # the repository. Return an array of the repertoire which are repertoitre_field
    # where the search_name was found in the field search_field.
    # Return None on error, return empty array if not found.
    def getRepertoires(self, search_field, search_name):
        # Build the query
        query =  {search_field: {'$eq': search_name}}
        rep_array = []
        try:
            # Execute the query and then extract the array of the values returned.
            repertoire_cursor = self.repertoire.find(query)
            for repertoire in repertoire_cursor:
                rep_array.append(repertoire)
        except Exception as err:
            print("ERROR: Search for repertoire field failed, field = %s"%(str(err)))
            return None
            
        return rep_array

    # Write the set of JSON records provided to the "rearrangements" collection.
    # This is hiding the repository implementation.
    # Return a list of the ids on success None on failure.
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
                                 update_field, update_value, update_time_field):
        if not self.skipload:
            # Get the current time, and add it to the update information.
            now_str = Parser.getDateTimeNowUTC()
            update = {"$set": {update_field:update_value, update_time_field:now_str}}
            self.rearrangement.update( {search_field:search_value}, update)

    # Count the number of rearrangements that belong to a specific repertoire. 
    # Return -1 on error. Note: In our early implementations, we had an
    # internal field name called ir_project_sample_id. We want to hide
    # this and just talk about reperotire IDs, so this is hidden in the
    # Rearrangement class...
    def countRearrangements(self, repertoire_field, repertoire_id):
        # Check for valid fields
        if repertoire_field is None or repertoire_id is None:
            print("ERROR: Invalid repertoire field (%s) or repertoire_id (%s)"%
                  (repertoire_field, repertoire_id))
            return -1
        # Build the query and try to perform it. 
        query = {repertoire_field:{'$eq':repertoire_id}}
        try:
            rearrangement_count = self.rearrangement.find(query).count()
        except Exception as err:
            print("ERROR: Query failed for repertoire field (%s) or repertoire_id (%s)"%
                  (repertoire_field, repertoire_id))
            return -1

        # If sucessful return the count.
        return rearrangement_count

    # Write the set of JSON records provided to the "clones" collection.
    # This is hiding the repository implementation.
    # Return a list of the ids on success None on failure.
    def insertClones(self, json_records):
        if not self.skipload:
            try:
                record_ids = self.clone.insert(json_records)
            except Exception as err:
                print("ERROR: Unable to write records to repository, %s"%(err))
                return None
        return record_ids

    # Update the update_field to update_value wherever search_field is equal to
    # search value.
    def updateCloneField(self, search_field, search_value,
                         update_field, update_value, update_time_field):
        if not self.skipload:
            # Get the current time, and add it to the update information.
            now_str = Parser.getDateTimeNowUTC()
            update = {"$set": {update_field:update_value, update_time_field:now_str}}
            self.clone.update( {search_field:search_value}, update)

    # Count the number of clones that belong to a specific repertoire. 
    # Return -1 on error. 
    def countClones(self, repertoire_field, repertoire_id):
        # Check for valid fields
        if repertoire_field is None or repertoire_id is None:
            print("ERROR: Invalid repertoire field (%s) or repertoire_id (%s)"%
                  (repertoire_field, repertoire_id))
            return -1
        # Build the query and try to perform it. 
        query = {repertoire_field:{'$eq':repertoire_id}}
        try:
            clone_count = self.clone.find(query).count()
        except Exception as err:
            print("ERROR: Query failed for repertoire field (%s) or repertoire_id (%s)"%
                  (repertoire_field, repertoire_id))
            return -1

        # If sucessful return the count.
        return clone_count

    # Write the set of JSON records provided to the "cell" collection.
    # This is hiding the repository implementation.
    # Return a list of the ids on success None on failure.
    def insertCells(self, json_records):
        if not self.skipload:
            try:
                record_ids = self.cell.insert(json_records)
            except Exception as err:
                print("ERROR: Unable to write records to repository, %s"%(err))
                return None
        return record_ids

    # Update the update_field to update_value wherever search_field is equal to
    # search value.
    def updateCellField(self, search_field, search_value,
                        update_field, update_value, update_time_field):
        if not self.skipload:
            # Get the current time, and add it to the update information.
            now_str = Parser.getDateTimeNowUTC()
            update = {"$set": {update_field:update_value, update_time_field:now_str}}
            self.cell.update( {search_field:search_value}, update)

    # Count the number of cells that belong to a specific repertoire. 
    # Return -1 on error. 
    def countCells(self, repertoire_field, repertoire_id):
        # Check for valid fields
        if repertoire_field is None or repertoire_id is None:
            print("ERROR: Invalid repertoire field (%s) or repertoire_id (%s)"%
                  (repertoire_field, repertoire_id))
            return -1
        # Build the query and try to perform it. 
        query = {repertoire_field:{'$eq':repertoire_id}}
        try:
            cell_count = self.cell.find(query).count()
        except Exception as err:
            print("ERROR: Query failed for repertoire field (%s) or repertoire_id (%s)"%
                  (repertoire_field, repertoire_id))
            return -1

        # If sucessful return the count.
        return cell_count


    # Write the set of JSON records provided to the "expression" collection.
    # This is hiding the repository implementation.
    # Return a list of the ids on success None on failure.
    def insertExpression(self, json_records):
        if not self.skipload:
            try:
                record_ids = self.expression.insert(json_records)
            except Exception as err:
                print("ERROR: Unable to write records to repository, %s"%(err))
                return None
        return record_ids

    # Update the update_field to update_value wherever search_field is equal to
    # search value.
    def updateExpressionField(self, search_field, search_value,
                              update_field, update_value, update_time_field):
        if not self.skipload:
            # Get the current time, and add it to the update information.
            now_str = Parser.getDateTimeNowUTC()
            update = {"$set": {update_field:update_value, update_time_field:now_str}}
            self.expression.update( {search_field:search_value}, update)

    # Count the number of gene expression values that belong to a specific repertoire. 
    # Return -1 on error. 
    def countExpression(self, repertoire_field, repertoire_id):
        # Check for valid fields
        if repertoire_field is None or repertoire_id is None:
            print("ERROR: Invalid repertoire field (%s) or repertoire_id (%s)"%
                  (repertoire_field, repertoire_id))
            return -1
        # Build the query and try to perform it. 
        query = {repertoire_field:{'$eq':repertoire_id}}
        try:
            expression_count = self.expression.find(query).count()
        except Exception as err:
            print("ERROR: Query failed for repertoire field (%s) or repertoire_id (%s)"%
                  (repertoire_field, repertoire_id))
            return -1

        # If sucessful return the count.
        return expression_count

    # Update the update_field to update_value wherever search_field is equal to
    # search value.
    def updateField(self, search_field, search_value,
                    update_field, update_value, update_time_field):
        if not self.skipload:
            # Get the current time, and add it to the update information.
            now_str = Parser.getDateTimeNowUTC()
            update = {"$set": {update_field:update_value, update_time_field:now_str}}
            return self.repertoire.update( {search_field:search_value}, update)

    # Update a repertoire document in the repertoire collection. Takes a single 
    # field and a value for that field, searches for it, and if it finds one
    # record it updates that record with the document provided. This is a non
    # destructive update, as it will add and overwrite fields, but it does not 
    # delete any fields that are existing that are not overwritten.
    def updateRepertoire( self, search_field, search_value, doc, update_time_field):

        try:
            # Get the number of records. We expect there to be 1, error if not.
            num_records = self.repertoire.count_documents({search_field:search_value})
            if num_records != 1:
                print("ERROR: Could not find a single record (found %d), update failed"%
                      (num_records))
                return None
            # Get the old record
            cursor = self.repertoire.find({search_field:search_value})
            try:
                old_doc = cursor.next()
            except Exception as err:
                print("ERROR: Repository repertoire update failed, %s"%(err))
                return None

            # For each field in the new document, replace the old value with the
            # the new value. If it is the same, don't do anything.
            for (k, v) in doc.items():
                # Get the old value if it exists.
                old_value = None
                if k in old_doc:
                    old_value = old_doc[k]
                # If the old and new are different, do an update.
                if old_value != v:
                    if self.verbose:
                        print("Info: Updating %s: %s => %s"%(k,old_value,v))
                    # Don't do anything if we are skipping loading.
                    if not self.skipload:
                        self.updateField(search_field, search_value, k, v, update_time_field)
            #results = self.repertoire.update({search_field:search_value},{"$set":doc})
        except Exception as err:
            print("ERROR: Repository repertoire update failed, %s"%(err))
            return None
        # Return the value of the field that determined which record to update.
        return search_value

    # Insert a repertoire document into the repertoire collection. Generates a 
    # unique identifier for the record and stores that ID in the field provided
    # in the link_field field for reference without having to access the internal
    # _id.  Returns the string value of the record ID on success, return 
    # None on failure.
    def insertRepertoire( self, doc, link_field, update_time_field ):
        try:
            if not self.skipload:
                results = self.repertoire.insert(doc)
        except Exception as err:
            print("ERROR: Repository insertion failed, %s"%(err))
            return None

        # Store the internal ID as a string in the link_field.
        self.updateField("_id", results, link_field, str(results), update_time_field)
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

