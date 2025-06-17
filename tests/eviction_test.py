# import os
# import asyncio
# from datetime import datetime
# import cloudinary
# import cloudinary.api
# from pymongo import MongoClient

# # # Configure MongoDB
# mongo_client = MongoClient(os.getenv("MONGODB_URI"))
# db = mongo_client[os.getenv("DB_NAME", "telegram_data")]
# cloudinary_assets = db.cloudinary_assets

# # Configure Cloudinary


# # Storage limit in bytes (Free tier = 1GB)
# CLOUDINARY_STORAGE_LIMIT = 1073741824

# async def check_and_evict(required_space=0):
#     """Check storage and evict LRU assets if needed"""

#     usage = await asyncio.to_thread(cloudinary.api.usage)
#     current_storage = usage["storage"]["usage"]
    
#     # Calculate needed space
#     needed = current_storage + required_space - CLOUDINARY_STORAGE_LIMIT
#     if needed <= 0:
#             return True

#     print(f"Storage overage detected. Need to free {needed} bytes")

#     # Find LRU assets sorted by last access
#     lru_assets = list(cloudinary_assets.find().sort("last_accessed", 1))
#     total_freed = 0

#     for asset in lru_assets:
#         if total_freed >= needed:
#             break

#         try:
#             # Delete from Cloudinary
#             await asyncio.to_thread(
#                 cloudinary.uploader.destroy, 
#                 asset["public_id"],
#                 invalidate=True
#             )
#             # Delete from MongoDB
#             cloudinary_assets.delete_one({"_id": asset["_id"]})
#             total_freed += asset["size"]
#             print(f"Evicted: {asset['public_id']} ({asset['size']} bytes)")
#         except Exception as e:
#             print(f"Failed to evict {asset['public_id']}: {str(e)}")

#     print(f"Total freed: {total_freed} bytes")
#     return total_freed >= needed

# async def upload_with_eviction(file_path, asset_type="post_photo"):
#     """Upload file with LRU eviction when needed"""
#     try:
#         file_size = os.path.getsize(file_path)
        
#         # Check if we need to evict before uploading
#         success = await check_and_evict(file_size)
#         if not success:
#             print("Insufficient storage after eviction")
#             return None

#         # Upload to Cloudinary
#         upload_result = await asyncio.to_thread(
#             cloudinary.uploader.upload,
#             file_path,
#             folder=asset_type
#         )

#         # Store metadata in MongoDB
#         asset_data = {
#             "public_id": upload_result["public_id"],
#             "url": upload_result["secure_url"],
#             "uploaded_at": datetime.now(),
#             "last_accessed": datetime.now(),
#             "size": upload_result["bytes"],
#             "type": asset_type
#         }
#         cloudinary_assets.insert_one(asset_data)

#         return upload_result["secure_url"]
    
#     except Exception as e:
#         print(f"Upload failed: {str(e)}")
#         return None

# async def update_last_accessed(public_id):
#     """Update access time when serving assets"""
#     cloudinary_assets.update_one(
#         {"public_id": public_id},
#         {"$set": {"last_accessed": datetime.now()}}
#     )

# asyncio.run(check_and_evict())