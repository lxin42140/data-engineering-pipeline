from pymongo import MongoClient
from pymongo.errors import InvalidName
import json
import re
import pandas as pd
from bson.objectid import ObjectId
import pytz
from datetime import datetime, date
from pathlib import Path

"""
Primary intermediate storage will be Atlas, and local will be strictly for backup.

At any point of time, data between local and Atlas will be consistent i.e. data present in local must be present in Atlas

This implies that if a write operation fails in Atlas, we will not perform the same operation in local as doing so 
will lead to Atlas and local be inconsistent. This will then require us to check and perform the sychronisation in the immediate next 
component when Atlas is available again, so that the changes can be synchronised. 

This could be an expensive operation, as we have to check the collections in local and compare with Atlas, then push the changes to Atlas, in multiple 
components
"""

DEBUG_ENV = "DBHELPER"
DEBUG_CLOUD = "[ATLAS]"
DEBUG_LOCAL = "[LOCAL]"
IGNORED_DATABASES = ['admin', 'local', 'config']
OUT_FILE_PATH = "/home/airflow/3107-pipeline/upload"
class DBHelper:

    def __init__(self):
        self.cloud_conn_str = "mongodb+srv://admin:admin@cluster0.rljpy.mongodb.net" # connecting to cluster only, not database
        self.local_conn_str= "mongodb://localhost:27017"
        self.conn_timeout = 5000 # set a 5-second connection timeout
        self.validate = False
        self.__connect__();                 

    # Connect to both local MongoDB and MongoDB Atlas
    # Do not need to manually close these connections
    def __connect__(self):
        try:
            self.cloud_client = MongoClient(self.cloud_conn_str, self.conn_timeout)
            print(f"{DEBUG_ENV}-{DEBUG_CLOUD}: Connection Success...\n")
        except Exception as e:
            raise Exception(f"{DEBUG_ENV}-{DEBUG_CLOUD}: Connection Failed: {e}\n")

        try:
            self.local_client = MongoClient(self.local_conn_str, self.conn_timeout)     
            print(f"{DEBUG_ENV}-{DEBUG_LOCAL}: Connection Success...\n")
        except Exception as e:
            print(f"{DEBUG_ENV}-{DEBUG_LOCAL}: Connection Failed: {e}\n")
            
    def __validate__(self, database_name, collection_name=None):
        
        database_names = self.get_database_names()
        if database_name not in database_names:
            raise Exception(f"{DEBUG_ENV}: {database_name} Does Not Exist!\nExpected One Of {database_names}")

        if collection_name is not None:
            collection_names = self.get_collection_names_for_database(database_name)
            if collection_name not in collection_names:
                raise Exception(f"{DEBUG_ENV}: {collection_name} Does Not Exist In {database_name}!\nExpected One Of {collection_names}")

    def set_validation(self, validate):
        if not validate:
            self.validate = False
        else:
            self.validate = True
    
    # return list of database names in cluster
    def get_database_names(self, run_cloud = True):
        try:
            names = []
            if run_cloud:
                names = self.cloud_client.list_database_names()
            else:
                names = self.local_client.list_database_names()
            
            result = []
            for name in names:
                if name in IGNORED_DATABASES:
                    pass
                else: 
                    result.append(name)
            
            return result
        except Exception as e:
            if run_cloud: # Cloud failed, tried local
                return self.get_database_names(self, run_cloud = False)
            print(f"{DEBUG_ENV}: Get Database Names Failed: {e}\n")
            
    # return list of collection names in database
    def get_collection_names_for_database(self, database_name, run_cloud = True):
        if self.validate:
            self.__validate__(database_name)
        
        try:
            if run_cloud:
                return self.cloud_client.get_database(name=database_name).list_collection_names()
            else:
                return self.local_client.get_database(name=database_name).list_collection_names()
        except Exception as e:
            if run_cloud: # Cloud failed, tried local
                return self.get_collection_names_for_database(self, database_name, run_cloud = False)
            print(f"{DEBUG_ENV}: Get Collection Names Failed For {database_name}: {e}\n")
    
    # return a list of documents in collection
    def get_documents_for_collection(self, database_name, collection_name, run_cloud = True, filter={}, columns={}):
        if self.validate:
            self.__validate__(database_name, collection_name)

        try:
            if run_cloud:
                return list(self.cloud_client.get_database(name=database_name).get_collection(name=collection_name).find(filter, columns))
            else:
                return list(self.local_client.get_database(name=database_name).get_collection(name=collection_name).find(filter, columns))
        except Exception as e:
            if run_cloud: # Cloud failed, tried local
                return self.get_documents_for_collection(self, database_name, collection_name, run_cloud = False)
            print(f"{DEBUG_ENV}: Get Documents For {collection_name} Failed: {e}\n")
    
    # If the insert failed on Atlas, it will not insert in local
    def insert_many_for_collection(self, database_name, collection_name, records):
        if records is None or len(records) == 0:
            raise Exception("{DEBUG_ENV}: Invalid Records")
        
        if self.validate:
            self.__validate__(database_name, collection_name)
        
        for record in records:
            record['created_at'] = datetime.now(tz=pytz.UTC).isoformat()

        try: 
            def cloud_callback(session):
                self.cloud_client[database_name][collection_name].insert_many(documents=records, session=session)

            with self.cloud_client.start_session() as session:
                session.with_transaction(cloud_callback)
                print(f"{DEBUG_ENV}-{DEBUG_CLOUD}: Insert Many For Collection Success!\n")

        except Exception as e:
            raise Exception(f"{DEBUG_ENV}-{DEBUG_CLOUD}: Insert Many For Collection Failed: {e}\n")

        try:             
            def local_callback(session):
                self.local_client[database_name][collection_name].insert_many(documents=records, session=session)
                
            with self.local_client.start_session() as session:
                session.with_transaction(local_callback)
                print(f"{DEBUG_ENV}-{DEBUG_LOCAL}: Insert Many For Collection Success!\n")

        except Exception as e:
            print(f"{DEBUG_ENV}-{DEBUG_LOCAL}: Insert Many For Collection Failed: {e}\n")
    
    # if delete failed on cloud, will not delete local
    def delete_documents_for_collection(self, database_name, collection_name):
        if self.validate:
            self.__validate__(database_name, collection_name)
        
        try:
            self.cloud_client[database_name][collection_name].delete_many({})
        except Exception as e:
            raise Exception(f"{DEBUG_ENV}-{DEBUG_CLOUD}: Delete Collection {collection_name} Failed For {database_name}: {e}\n")

        deleted_local = False
        try:
            self.local_client[database_name][collection_name].delete_many({})
            deleted_local = True
        except Exception as e:
            print(f"{DEBUG_ENV}-{DEBUG_LOCAL}: Delete Collection {collection_name} Failed For {database_name}: {e}\n")
            
        return deleted_local
        
    # Drop all documents in all collections for all databases
    # Will drop both local and cloud
    def clean_all_databases(self):        
        database_names = self.get_database_names()
        
        all_databases_deleted = True
        for database in database_names:
            if not self.clean_database(database):
                all_databases_deleted = False
        
        if not all_databases_deleted:
            print(f"{DEBUG_ENV}-{DEBUG_LOCAL}: Clean All Databases Failed, Some Databases Are Not Cleared\n")
        else:
            print(f"{DEBUG_ENV}: Clean All Databases Success\n")
            
    # Drop all documents in all collections for a database
    # Will drop both local and cloud
    def clean_database(self, database_name):
        if self.validate:
            self.__validate__(database_name)
            
        collection_names = self.get_collection_names_for_database(database_name)
    
        all_collections_deleted = True
        for collection in collection_names:
            deleted = self.delete_documents_for_collection(database_name, collection)
            if not deleted:
                all_collections_deleted = False
        
        if not all_collections_deleted:
            print(f"{DEBUG_ENV}-{DEBUG_LOCAL}: Clean {database_name} Failed, Some Collections Are Not Cleared\n")
        else:
            print(f"{DEBUG_ENV}: Clean {database_name} Success\n")
        
        return all_collections_deleted
        
    # Export a collection to json
    def export_collection_to_json(self, database_name, collection_name):
        if self.validate:
            self.__validate__(database_name, collection_name)
            
        documents = self.get_documents_for_collection(database_name=database_name, collection_name=collection_name)
        
        if len(documents) <= 0:
            print(f"{DEBUG_ENV}: Collection {collection_name} For {database_name} Is Empty\n")
            return
        
        try: 
            Path(f"{OUT_FILE_PATH}/{database_name}").mkdir(parents=True, exist_ok=True)
            with open(f"{OUT_FILE_PATH}/{database_name}/{collection_name.capitalize()}.json", "w+") as f:
                for document in documents:
                    del document["_id"]
                    json.dump(document, f)
                    f.write('\n')
            
            print(f"{DEBUG_ENV}: Exported Collection {collection_name} For {database_name}\n")
        except Exception as e:
            raise Exception(f"{DEBUG_ENV}: Failed To Export Collection {collection_name} Failed For {database_name}: {e}\n")
    
    # export all collections within a database to json
    def export_database_collections_to_json(self, database_name):
        if self.validate:
            self.__validate__(database_name)
            
        collections  = self.get_collection_names_for_database(database_name)
        for collection in collections:
            self.export_collection_to_json(database_name, collection)
    
    # Export all collections for all databases to json
    def export_all_database_collections_to_json(self):
        databases = self.get_database_names()
        
        for database in databases:
            collections  = self.get_collection_names_for_database(database)
            for collection in collections:
                self.export_collection_to_json(database, collection)