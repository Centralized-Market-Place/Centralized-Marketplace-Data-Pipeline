from pymongo import MongoClient
from dotenv import dotenv_values

config = dotenv_values('secrets.env')

def store_raw_data(raw_data):
    mongo_db_client = MongoClient(config['DB_API'])
    db = mongo_db_client[config['DB_NAME']]
    collection = db[config['RAW_DATA_COLLECTION']]
    collection.insert_one(raw_data)

def store_decoded(data):
    mongo_db_client = MongoClient(config['DB_API'])
    db = mongo_db_client[config['DB_NAME']]
    collection = db[config['DECODED_DATA_COLLECTION']]
    collection.insert_one(data)