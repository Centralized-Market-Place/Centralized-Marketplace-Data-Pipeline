
from datetime import datetime, timezone

from tqdm import tqdm
from pymongo import MongoClient

MONGO_URI="mongodb+srv://semahegnsahib:sahib@cluster0.vmyk3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME="centeral_marketplace"
# COLLECTION_NAME2="products"
client = MongoClient(MONGO_URI)
# collection2 = client2[DB_NAME2][COLLECTION_NAME2]

db = client[DB_NAME]
# raw_data = db["raw_data"]
# products = db["products"]
# structured = db["structured_products"]

def insert_document(collection_name, document):
    """Insert a document into a MongoDB collection."""
    # print(f"[DB Insertion]")
    # return True
    try:
        result = db[collection_name].insert_one(document)
        print(f"Inserted document with ID: {result.inserted_id}")
        return result.inserted_id
    except Exception as e:
        print(f"❌ Failed to insert document: {str(e)}")
        return None
    
def find_documents(collection_name, query=None, sort_field=None, sort_order=1):
    """Find and return documents with optional sorting."""
    query = query or {}
    cursor = db[collection_name].find(query)
    if sort_field:
        cursor = cursor.sort(sort_field, sort_order)
    return list(cursor)

def delete_document(collection_name, filter_query):
    """Delete a single document matching the filter."""
    # print(f"[DB Delete] {filter_query}")
    # return True
    try:
        result = db[collection_name].delete_one(filter_query)
        print(f"Deleted {result.deleted_count} document(s)")
    except Exception as e:
        print(f"❌ Delete failed: {str(e)}")

def update_document(collection_name, filter_query, update_query):
    """Update a single document matching the filter."""
    # print(f"[DB Update] {filter_query}")
    # return True
    try:
        result = db[collection_name].update_one(filter_query, {"$set": update_query}, upsert=True)
        if result.matched_count > 0 or result.upserted_id is not None:
            if result.upserted_id is not None:
                print(f"Inserted new document with ID: {result.upserted_id}")
            else:
                print(f"Updated {result.modified_count} document(s)")
        else:
            print("No documents matched the filter and no new document inserted.")
    except Exception as e:
        print(f"❌ Update failed: {str(e)}")

# update only if updated_by_seller is false else do nothing
def update_document_if_not_updated_by_seller(collection_name, filter_query, update_query):
    """Update a single document only if updated_by_seller is false."""
    # print(f"[DB Conditional Update] {filter_query} with {update_query}")
    # return True
    try:
        # Check if the document exists and updated_by_seller is False
        existing_doc = db[collection_name].find_one(filter_query)
        if existing_doc and (not existing_doc.get("is_updated", False)) and (not existing_doc.get("is_deleted", False)):
            result = db[collection_name].update_one(filter_query, {"$set": update_query})
            if result.matched_count > 0:
                print(f"Updated {result.modified_count} document(s)")
            else:
                print("No documents matched the filter.")
        else:
            print("Document already updated by seller or does not exist.")
    except Exception as e:
        print(f"❌ Update failed: {str(e)}")