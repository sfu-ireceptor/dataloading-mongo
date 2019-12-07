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


        # Set Mongo db name
        self.mongo_db = self.mongo_client[self.database]
        self.repertoire = self.mongo_db[self.repertoire_collection]
        self.rearrangement = self.mongo_db[self.rearrangement_collection]
