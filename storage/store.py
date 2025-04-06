from pymongo import MongoClient
from tqdm import tqdm
# from dotenv import dotenv_values
from datetime import datetime, timezone

MONGO_URI="mongodb+srv://semahegnsahib:sahib@cluster0.vmyk3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME="centeral_marketplace"
# config = dotenv_values('secrets.env')

def store_raw_data(raw_data, collection_name='raw_data'):
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DATABASE_NAME]
        collection = db[collection_name]
        for datum in tqdm(raw_data, desc="Storing row data: "):
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
        for product in tqdm(products, desc="Storing products: "):
            # product['created_at'] = str(datetime.now())
            product['updated_at'] = str(datetime.now())
            # insert or update if message_id matches
            collection.update_one({'message_id': product['id']}, {'$set': product}, upsert=True)
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

def store_channels(channels):
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client[DATABASE_NAME]
        collection = db['channels']

        for channel in tqdm(channels, desc="Storing channels"):
            channel['created_at'] = str(datetime.now())
            channel['updated_at'] = str(datetime.now())
            # Insert or update the channel info based on channel_id
            collection.update_one({'id': channel['id']}, {'$set': channel}, upsert=True)
        
        return True
    except Exception as e:
        print(str(e))
        return False

# ======================= products correction code ===================================
def extract_message_data(message_obj):
    try:
        message_id = message_obj.get('message_id')
        
        peer_id = message_obj.get('peer_id', {})
        channel_id = peer_id.get('channel_id') if peer_id.get('_') == 'PeerChannel' else None

        date = message_obj.get('date')
        message = message_obj.get('message', '')
        if not message:
            return 0, {}

        forwards = message_obj.get('forwards', 0)
        views = message_obj.get('views', 0)
        images = message_obj.get('images', [])


        reactions_data = []
        reactions = message_obj.get('reactions', [])
        if isinstance(reactions, list):
            for reaction in reactions:
                if isinstance(reaction, list) and len(reaction) == 2:
                    emoji, count = reaction
                    reactions_data.append((emoji, count))
        else:
            results = reactions.get('results', [])
            for reaction in results:
                emoji = reaction.get('reaction', {}).get('emoticon', '')
                count = reaction.get('count', 0)
                if emoji:
                    reactions_data.append((emoji, count))

        return {
            'message_id': message_id,
            'channel_id': channel_id,
            'date': date,
            'description': message,
            'forwards': forwards,
            'views': views,
            'reactions': reactions_data,
            'images': images,
            'updated_at': message_obj.get('updated_at')
        }
        
    except Exception as e:
        print(f"Error processing message: {e}")
        return None


# ======================= products correction code ===================================
# def update_products():
#     mongo_client = MongoClient(MONGO_URI)
#     db = mongo_client[DATABASE_NAME]
#     collection = db['products']

#     products = collection.find({})
#     # insert products into a text file
#     # with open('products.txt', 'w', encoding='utf-8') as f:
#     #     for product in products:
#     #         f.write(str(product) + '\n')


#     total_products = 0
#     update_count = 0
#     correct = 0
#     unknown = 0
#     incorrect = 0
#     corrected = []
#     for product in tqdm(products, desc="Formatting products"):
#         flag, formatted_data = extract_message_data(product)
#         total_products += 1
#         if flag == 2:
#             update_count += 1
#             # formatted_data.pop('_id', None)
#             corrected.append(formatted_data)
#             # collection.update_one(
#             #     {"_id": product["_id"]}, 
#             #     {"$set": formatted_data, "$unset": {key: "" for key in product if key not in formatted_data}}
#             # )
#         elif flag == 1:
#             correct += 1
#         elif flag == 3:
#             unknown += 1
#             unknown.append(product)
#         else:
#             incorrect += 1
#             collection.delete_one({"_id": product["_id"]})
    
#     # delete by corrected id and insert corrected products
#     collection.delete_many({"_id": {"$in": [product["raw_id"] for product in corrected]}})
#     collection.insert_many(corrected)

#     print(f"Corrected: {update_count} products.")
#     print(f"Correct: {correct} products already correct.")
#     print(f"Incorrect: {incorrect} products deleted.")
#     print(f"Unknown: {unknown} products.")
#     print("Product formatting complete.")

