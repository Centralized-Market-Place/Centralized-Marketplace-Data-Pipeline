# # Fix 1
# # reupload images
# import os
# import sys
# import asyncio
# from collections import defaultdict
# import json
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# from telethon.sync import TelegramClient
# from datetime import datetime, timezone
# from telethon.tl.custom.message import Message
# from telethon.tl.types import PeerChannel

# from tqdm import tqdm
# from pymongo import MongoClient

# import cloudinary
# import cloudinary.uploader
# import cloudinary.api



# TRACK_FILE = "updated_messages.json"

# # Load existing tracked updates
# if os.path.exists(TRACK_FILE):
#     with open(TRACK_FILE, "r") as f:
#         updated_map = json.load(f)
# else:
#     updated_map = {}

# print("Packages Loaded!")


# def mark_as_updated(channel_id, msg_id):
#     key = f"{channel_id}:{msg_id}"
#     updated_map[key] = True
#     with open(TRACK_FILE, "w") as f:
#         json.dump(updated_map, f)

# def is_already_updated(channel_id, msg_id):
#     return updated_map.get(f"{channel_id}:{msg_id}", False)


# #============== check

# def insert_document(collection_name, document):
#     """Insert a document into a MongoDB collection."""
#     try:
#         result = db[collection_name].insert_one(document)
#         print(f"Inserted document with ID: {result.inserted_id}")
#         return result.inserted_id
#     except Exception as e:
#         print(f"❌ Failed to insert document: {str(e)}")
#         return None
    
# def find_documents(collection_name, query=None, sort_field=None, sort_order=1):
#     """Find and return documents with optional sorting."""
#     query = query or {}
#     cursor = db[collection_name].find(query)
#     if sort_field:
#         cursor = cursor.sort(sort_field, sort_order)
#     return list(cursor)

# def delete_document(collection_name, filter_query):
#     """Delete a single document matching the filter."""
#     try:
#         result = db[collection_name].delete_one(filter_query)
#         print(f"Deleted {result.deleted_count} document(s)")
#     except Exception as e:
#         print(f"❌ Delete failed: {str(e)}")

# async def check_and_evict(message_date, required_space=0):
#     """Check Cloudinary usage and evict LRU assets if needed."""
#     try:
#         usage = await asyncio.to_thread(cloudinary.api.usage)
#         current_storage = usage["storage"]["usage"]

#         needed = current_storage + required_space - CLOUDINARY_STORAGE_LIMIT
#         if needed <= 0:
#             return True

#         print(f"[Eviction] Need to free {needed} bytes...")

#         # Fetch LRU assets based on Telegram message date (ascending)
#         lru_assets = find_documents(
#             "cloudinary_assets_v2",
#             sort_field="last_accessed",
#             sort_order=1  # ascending
#         )

#         total_freed = 0
#         for asset in lru_assets:
#             # Only evict if the new asset (to be uploaded) is newer than the asset in DB
#             if message_date is not None and asset.get("last_accessed") is not None:
#                 if message_date <= asset["last_accessed"]:
#                     continue  # Skip eviction if new asset is not newer

#             if total_freed >= needed:
#                 break

#             try:
#                 await asyncio.to_thread(
#                     cloudinary.uploader.destroy,
#                     asset["public_id"],
#                     invalidate=True
#                 )
#                 delete_document("cloudinary_assets_v2", {"_id": asset["_id"]})
#                 total_freed += asset["size"]
#                 print(f"✅ Evicted: {asset['public_id']} ({asset['size']} bytes)")
#             except Exception as e:
#                 print(f"❌ Failed to evict {asset['public_id']}: {str(e)}")

#         print(f"[Eviction] Total freed: {total_freed} bytes")
#         return total_freed >= needed

#     except Exception as e:
#         print(f"❌ Eviction check failed: {str(e)}")
#         return False


# async def upload_with_eviction(file_path, asset_type="post_photo", channel_id=None, message_id=None, message_date=None):
#     """
#     Upload a file to Cloudinary, evicting LRU assets if storage limit is exceeded.
#     Uses Telegram message date as 'last_accessed' timestamp.
#     """

#     try:
#         file_size = os.path.getsize(file_path)

#         # Evict if necessary
#         success = await check_and_evict(message_date, file_size)
#         if not success:
#             print("Insufficient storage even after eviction.")
#             return None

#         # Upload to Cloudinary
#         upload_result = await asyncio.to_thread(
#             cloudinary.uploader.upload,
#             file_path,
#             folder=asset_type
#         )

#         # Use message date if provided, otherwise default to now
#         now = datetime.now(timezone.utc)
#         accessed_time = message_date if message_date is not None else now

#         # Build asset record
#         asset_data = {
#             "public_id": upload_result["public_id"],
#             "url": upload_result["secure_url"],
#             "uploaded_at": now,
#             "last_accessed": accessed_time,
#             "size": upload_result["bytes"],
#             "type": asset_type
#         }

#         # Optional: store channel/message linkage
#         if channel_id is not None:
#             asset_data["channel_id"] = channel_id
#         if message_id is not None:
#             asset_data["message_id"] = message_id

#         # Save to DB
#         insert_document("cloudinary_assets_v2", asset_data)

#         return asset_data  # Return for caller (e.g., for structured update)

#     except Exception as e:
#         print(f"Upload failed: {str(e)}")
#         return None





# async def redownload_all_images(tg_client):
#     print("Starting redownload_all_images...")
#     structured_messages = list(structured.find({}))

#     print(f"Total structured messages: {len(structured_messages)} loaded.")
#     raw_data_cache = {}
#     raw_data_group_cache = defaultdict(set)

#     raw_docs = list(raw_data.find({}))
#     print(f"Total raw data documents: {len(raw_docs)} loaded.")

#     for doc in raw_docs:
#         channel_id = doc['peer_id']['channel_id'] if 'peer_id' in doc and 'channel_id' in doc['peer_id'] else None
#         grouped_id = doc.get('grouped_id')
#         if not channel_id:
#             continue
#         key = (channel_id, doc['id'])
#         raw_data_cache[key] = doc
#         if grouped_id:
#             raw_data_group_cache[(channel_id, grouped_id)].add(doc['id'])

#     print(f"Total raw data documents in groups: {len(raw_data_group_cache)} loaded.")
#     def get_raw_message(channel_id, msg_id):
#         return raw_data_cache.get((channel_id, msg_id))
    
#     for struct_msg in tqdm(structured_messages, desc="Processing messages"):
#         try:
#             channel_id = struct_msg["telegram_channel_id"]
#             msg_id = struct_msg["message_id"]

#             if is_already_updated(channel_id, msg_id):
#                 continue
            
#             raw_msg = get_raw_message(channel_id, msg_id)
#             if not raw_msg:
#                 print(f"Missing raw_data: {channel_id}, {msg_id}")
#                 continue
#             images = []     

#             # check if it is part of a group
#             grouped_id = raw_msg.get('grouped_id')
#             if grouped_id:
#                 group_doc_ids = raw_data_group_cache.get((channel_id, grouped_id))
#                 if group_doc_ids:
#                     # Check if any of the group members have images
#                     try:
#                         for group_doc_id in group_doc_ids:
#                             # fetch message from telegram
#                             await asyncio.sleep(2)
#                             tg_client_msg = await tg_client.get_messages(
#                                 PeerChannel(channel_id),
#                                 ids=group_doc_id
#                             )
#                             if isinstance(tg_client_msg, Message):
#                                 # Check if the message has media
#                                 if not tg_client_msg.media or not hasattr(tg_client_msg.media, "photo"):
#                                     print(f"Missing media in group message: {channel_id}, {group_doc_id}")
#                                     continue
                                
#                                 # Rate limit
#                                 await asyncio.sleep(2)
#                                 file = await tg_client.download_media(tg_client_msg.media)

#                                 if file:
#                                     url_data = await upload_with_eviction(
#                                         file,
#                                         asset_type="post_photo",
#                                         channel_id=channel_id,
#                                         message_id=msg_id,
#                                         message_date=getattr(tg_client_msg, "date", None)
#                                     )
#                                     url = url_data["url"] if url_data else None
#                                     if url:
#                                         images.append(url)
#                                     os.remove(file)
#                     except Exception as e:
#                         print(f"Error processing group message: {e}")
#                         continue    

                    
#                 else:
#                     print(f"Missing raw_data group: {channel_id}, {grouped_id}")
#                     continue    
                
#             else:
#                 # rate limit
#                 await asyncio.sleep(2)
#                 # Fetch message from Telegram
#                 tg_client_msg = await tg_client.get_messages(
#                     PeerChannel(channel_id),
#                     ids=msg_id
#                 )
#                 if not isinstance(tg_client_msg, Message):
#                     continue
                
#                 # Check if the message has media
#                 if not tg_client_msg.media or not hasattr(tg_client_msg.media, "photo"):
#                     print(f"Missing media in message: {channel_id}, {msg_id}")
#                     continue

#                 try:
#                     # Rate limit
#                     await asyncio.sleep(2)
#                     file = await tg_client.download_media(tg_client_msg.media)
#                     if file:
#                         url_data = await upload_with_eviction(
#                             file,
#                             asset_type="post_photo",
#                             channel_id=channel_id,
#                             message_id=msg_id,
#                             message_date=getattr(tg_client_msg, "date", None)
#                         )
#                         url = url_data["url"] if url_data else None
#                         if url:
#                             images.append(url)
#                         os.remove(file)
#                     else:
#                         print(f"Failed to download media: {channel_id}, {msg_id}")
#                         continue

#                 except Exception as e:
#                     print(f"Error processing message: {e}")
#                     continue
            
#             if images:
#                 # Update images array in structured
#                 structured.update_one(
#                     {"_id": struct_msg["_id"]},
#                     {"$set": {"images": images}}
#                 )
#                 # Mark as processed
#                 mark_as_updated(channel_id, msg_id)
#         except Exception as e:
#             print(f"Error processing structured message: {e}")
#             continue


# if __name__ == "__main__":
#     loop = asyncio.get_event_loop()
#     client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
#     with client:
#         loop.run_until_complete(redownload_all_images(client))