from pymongo import MongoClient
from tqdm import tqdm
# from dotenv import dotenv_values
from datetime import datetime, timezone

MONGO_URI="mongodb+srv://semahegnsahib:sahib@cluster0.vmyk3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME="centeral_marketplace"
# config = dotenv_values('secrets.env')
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DATABASE_NAME]

def store_raw_data(raw_data, collection_name='raw_data'):
    try:
        collection = db[collection_name]
        for datum in tqdm(raw_data, desc="Storing row data: "):
            collection.insert_one(datum)
        return True
    except Exception as e:
        print(str(e))
    return False

def fetch_all_channels(collection_name='channels_pool'):
    try:
        collection = db[collection_name]
        channels = list(collection.find())
        return channels
    except Exception as e:
        print(str(e))
    
    return []

def fetch_stored_messages(collection_name='raw_data'):
    try:
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
        collection = db['structured_products']
        for product in tqdm(products, desc="Storing products: "):
            # product['created_at'] = str(datetime.now())
            product['updated_at'] = datetime.now(timezone.utc)
            # insert or update if message_id matches
            collection.update_one({'message_id': product['message_id']}, {'$set': product}, upsert=True)
        return True
    except Exception as e:
        print(str(e))
    return False

def store_product(product):
    try:
        collection = db['products']
        product['created_at'] = datetime.now(timezone.utc)
        product['updated_at'] = datetime.now(timezone.utc)
        # insert or update if message_id matches
        collection.update_one({'message_id': product['message_id']}, {'$set': product}, upsert=True)
        return True
    except Exception as e:
        print(str(e))
    return False

def store_latest_and_oldest_ids(latest_ids, oldest_ids):
    try:
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

def insert_document(collection_name, document):
    """Insert a document into a collection."""
    try:
        collection = db[collection_name]
        collection.insert_one(document)
        return True
    except Exception as e:
        print(f"Insert failed: {str(e)}")
        return False

def update_document(collection_name, query, update_data, upsert=False):
    """Update a document in a collection."""
    try:
        collection = db[collection_name]
        collection.update_one(query, {"$set": update_data}, upsert=upsert)
        return True
    except Exception as e:
        print(f"Update failed: {str(e)}")
        return False

def delete_document(collection_name, query):
    """Delete a document from a collection."""
    try:
        collection = db[collection_name]
        collection.delete_one(query)
        return True
    except Exception as e:
        print(f"Delete failed: {str(e)}")
        return False

def find_documents(collection_name, query=None, sort_field=None, sort_order=1):
    """Find documents in a collection."""
    try:
        collection = db[collection_name]
        if query is None:
            query = {}
        if sort_field:
            return list(collection.find(query).sort(sort_field, sort_order))
        return list(collection.find(query))
    except Exception as e:
        print(f"Find failed: {str(e)}")
        return []

def solve_channels_slash_pool_inconsistency():
    channels_collection = db["channels"]
    channels_pool_collection = db["channels_pool"]
    
    # Fetch all pool entries
    pool_channels = list(channels_pool_collection.find({}))
    
    updated_count = 0

    for pool_channel in pool_channels:
        channel_with_at = pool_channel.get('channel', '')
        username = channel_with_at.lstrip('@')  # remove '@' symbol

        if not username:
            continue
        
        result = channels_collection.update_one(
            {"username": username},
            {"$set": {"pool_entry_id": str(pool_channel["_id"])}}
        )
        
        if result.matched_count:
            updated_count += 1
        else:
            print(f"⚠️ No matching channel found for username: {username}")
    
    print(f"✅ Successfully updated {updated_count} channels with pool_entry_id.")
def solve_product_channel_id_reference_inconsistency():
    """
    1. Rename 'channel_id' -> 'telegram_channel_id'
    2. Attach proper 'channel_id' from MongoDB channels collection
    3. Remove 'channel_mongo_id' if exists
    """
    products_collection = db["products"]
    channels_collection = db["channels"]

    # Build a lookup map: telegram_channel_id -> mongo _id
    channel_id_map = {}
    for channel in channels_collection.find({}):
        telegram_id = channel["id"]
        mongo_id = channel["_id"]
        channel_id_map[telegram_id] = mongo_id

    print(f"ℹ️ Found {len(channel_id_map)} channels to map.")

    # Process all products
    products = list(products_collection.find({}))
    updated_count = 0

    for product in tqdm(products, desc="Updating fields: "):
        update_fields = {}
        unset_fields = {}

        # 1. Rename channel_id -> telegram_channel_id 
        if "telegram_channel_id" not in product and "channel_id" in product and isinstance(product["channel_id"], int):
            update_fields["telegram_channel_id"] = product["channel_id"]

        # 2. Set correct Mongo channel_id
        telegram_id = product.get("telegram_channel_id") or product.get("channel_id")
        if isinstance(telegram_id, int):
            mongo_channel_id = channel_id_map.get(telegram_id)
            if mongo_channel_id:
                update_fields["channel_id"] = mongo_channel_id
            else:
                print(f"⚠️ No matching channel Mongo ID for Telegram ID {telegram_id} (Product ID {product['_id']})")
                continue  # Skip update if channel not found
        
        # 3. Remove channel_mongo_id field if it exists
        if "channel_mongo_id" in product:
            unset_fields["channel_mongo_id"] = ""

        # Perform the update if needed
        if update_fields or unset_fields:
            update_doc = {}
            if update_fields:
                update_doc["$set"] = update_fields
            if unset_fields:
                update_doc["$unset"] = unset_fields

            products_collection.update_one(
                {"_id": product["_id"]},
                update_doc
            )
            updated_count += 1

    print(f"✅ Migration complete. Updated {updated_count} products.")

def update_pool_with_channel_info():
    """
    Updates each pool entry with its corresponding channel_infos _id
    by matching the username.
    """

    channels_collection = db["channels"]
    pool_collection = db["channels_pool"]

    # Build a username -> channel_info _id map
    channel_username_map = {}
    for channel in channels_collection.find({}):
        if channel.get("username"):
            username = channel["username"].lower()
            channel_username_map[username] = channel["_id"]

    print(f"ℹ️ Found {len(channel_username_map)} channel usernames to map.")

    updated_count = 0
    for pool_entry in pool_collection.find({}):
        pool_username = pool_entry.get("channel")
        if not pool_username:
            continue
        
        # Remove '@' and lower the username
        clean_username = pool_username.lstrip("@").lower()
        channel_info_id = channel_username_map.get(clean_username)
        if channel_info_id:
            pool_collection.update_one(
                {"_id": pool_entry["_id"]},
                {"$set": {"channel_info_id": channel_info_id}}
            )
            updated_count += 1
        else:
            print(f"⚠️ No matching channel info for pool username {pool_username} (Pool ID {pool_entry['_id']})")

    print(f"✅ Pool update complete. Updated {updated_count} pool entries.")

def rename_channel_id_field():
    old_field = 'id'
    new_field = 'telegram_id'
    collection = db["channels"]

    result = collection.update_many(
        {old_field: {"$exists": True}},
        {"$rename": {old_field: new_field}}
    )

    print(f"Renamed field '{old_field}' to '{new_field}' in {result.modified_count} documents.")
    return result.modified_count

