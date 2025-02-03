from pymongo import MongoClient
from tqdm import tqdm
# from dotenv import dotenv_values
from datetime import datetime

MONGO_URI="mongodb+srv://semahegnsahib:sahib@cluster0.vmyk3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME="centeral_marketplace"
# config = dotenv_values('secrets.env')

def store_raw_data(raw_data, collection_name='raw_data'):
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DATABASE_NAME]
        collection = db[collection_name]
        for datum in raw_data:
            collection.insert_one(datum)
        return True
    except Exception as e:
        print(str(e))
    return False

def fetch_stored_messages(collection_name='raw_data'):
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DATABASE_NAME]
        collection = db[collection_name]
        messages = list(collection.find())
        return messages
    except Exception as e:
        print(str(e))
    return []

def store_decoded(data):
    pass



def store_products(products):
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DATABASE_NAME]
        collection = db['products']
        for product in tqdm(products, desc="Storing products"):
            product['created_at'] = str(datetime.utcnow())
            product['updated_at'] = str(datetime.utcnow())
            # insert or update if message_id matches
            collection.update_one({'message_id': product['message_id']}, {'$set': product}, upsert=True)
        return True
    except Exception as e:
        print(str(e))
    return False

def store_product(product):
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DATABASE_NAME]
        collection = db['products']
        product['created_at'] = str(datetime.utcnow())
        product['updated_at'] = str(datetime.utcnow())
        # insert or update if message_id matches
        collection.update_one({'message_id': product['message_id']}, {'$set': product}, upsert=True)
        return True
    except Exception as e:
        print(str(e))
    return False

def store_latest_and_oldest_ids(latest_ids, oldest_ids):
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DATABASE_NAME]
        collection = db['latest_channel_posts']
        
        for channel_id, message_id in tqdm(latest_ids.items(), desc="Storing latest IDs"):
            collection.update_one({'channel_id': channel_id}, {'$set': {'latest_id': message_id}}, upsert=True)
        
        collection = db['oldest_channel_posts']
        for channel_id, message_id in tqdm(oldest_ids.items(), desc="Storing oldest IDs"):
            collection.update_one({'channel_id': channel_id}, {'$set': {'oldest_id': message_id}}, upsert=True)
        return True
        
    except Exception as e:
        print(str(e))
        return False

    