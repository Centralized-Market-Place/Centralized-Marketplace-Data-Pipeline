from datetime import datetime, timezone
from tqdm import tqdm
from pymongo import MongoClient
import logging

MONGO_URI="mongodb+srv://semahegnsahib:sahib@cluster0.vmyk3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME="centeral_marketplace"
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

logger = logging.getLogger("CMP-DB-G")
logger.setLevel(logging.INFO)

# File handler
fh = logging.FileHandler("service.log")
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))

# Console handler
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))

logger.addHandler(fh)
logger.addHandler(ch)


def insert_document(collection_name, document):
    """Insert a document into a MongoDB collection."""
    try:
        result = db[collection_name].insert_one(document)
        logger.info(f"Inserted document with ID: {result.inserted_id}")
        return result.inserted_id
    except Exception as e:
        logger.error(f"❌ Failed to insert document: {str(e)}")
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
    try:
        result = db[collection_name].delete_one(filter_query)
        logger.info(f"Deleted {result.deleted_count} document(s)")
    except Exception as e:
        logger.error(f"❌ Delete failed: {str(e)}")

def update_document(collection_name, filter_query, update_query):
    """Update a single document matching the filter."""
    try:
        result = db[collection_name].update_one(filter_query, {"$set": update_query}, upsert=True)
        if result.matched_count > 0 or result.upserted_id is not None:
            if result.upserted_id is not None:
                logger.info(f"Inserted new document with ID: {result.upserted_id}")
            else:
                logger.info(f"Updated {result.modified_count} document(s)")
        else:
            logger.info("No documents matched the filter and no new document inserted.")
    except Exception as e:
        logger.error(f"❌ Update failed: {str(e)}")

# update only if updated_by_seller is false else do nothing
def update_document_if_not_updated_by_seller(collection_name, filter_query, update_query):
    """Update a single document only if updated_by_seller is false."""
    try:
        # Check if the document exists and updated_by_seller is False
        existing_doc = db[collection_name].find_one(filter_query)
        if existing_doc and (not existing_doc.get("is_updated", False)) and (not existing_doc.get("is_deleted", False)):
            result = db[collection_name].update_one(filter_query, {"$set": update_query})
            if result.matched_count > 0:
                logger.info(f"Updated {result.modified_count} document(s)")
            else:
                logger.info("No documents matched the filter.")
        else:
            logger.info("Document already updated by seller or does not exist.")
    except Exception as e:
        logger.error(f"❌ Update failed: {str(e)}")
