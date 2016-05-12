from pymongo import MongoClient


def query_mongo(db_name, collection, query=None, host=None, port=None):
    client = MongoClient(host=host)
    db = client[db_name]
    cursor = db[collection].find(query)
    for document in cursor:
        yield document



