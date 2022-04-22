from DBHelper import DBHelper
from bson.objectid import ObjectId
# from adapted_stocker import tomorrow

db = DBHelper()

# print(db.get_database_names())
# print(db.get_database_names(run_cloud=False))

# print(db.get_collection_names_for_database('DBHelperTester-database'))
# print(db.get_collection_names_for_database('DBHelperTester-database', run_cloud=False))

# db.set_validation(True)
# print(db.get_documents_for_collection('qualitative-analysis', 'raw-telegram'))
# print(db.get_documents_for_collection('qualitative-analysis', 'raw-telegram',run_cloud=False))
# print(db.get_documents_for_collection('DBHelperTester-database', 'DBHelperTester-collection', filter={'_id': ObjectId('62491d16db44fd997b758c91')}))
# print(db.get_documents_for_collection('DBHelperTester-database', 'DBHelperTester-collection', run_cloud=False))
# print(db.get_documents_for_collection('DBHelperTester-database', 'DBHelperTester-collection', filter={'_id': ObjectId('62491d16db44fd997b758c91')}, run_cloud=False))

# print(db.insert_many_for_collection('Test', 'Test', None))
# print(db.insert_many_for_collection('Test', 'Test', []))
# print(db.insert_many_for_collection('Test', 'Test', [{'name': 'this is a test1'}, {'name': 'this is a test2'}]))
# print(db.insert_many_for_collection('DBHelperTester-database', 'DBHelperTester-collection', [{'name': "this is a test1"}, {'name': "this is a test2"}]))
# print(db.get_documents_for_collection('DBHelperTester-database', 'DBHelperTester-collection'))

db.clean_database('industry-analysis')

# db.export_collection_to_json('qualitative-analysis', 'company-trends')
# db.export_collection_to_json('DBHelperTester-database', 'DBHelperTester-collection')
# db.export_all_database_collections_to_json()
# db.export_database_collections_to_json('qualitative-analysis')

# print(tomorrow('AAPL', features=['Interest', 'Wiki_views', 'RSI', '%K', '%R']))
