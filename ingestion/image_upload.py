import os
import sys
import asyncio
from datetime import datetime, timezone
from collections import defaultdict
import json
import cloudinary
import cloudinary.uploader
import cloudinary.api
from storage.generic_store import insert_document, find_documents, delete_document, update_document

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# cloudinary
CLOUD_NAME = "di46hiehs"
API_KEY = "238679894441775"
API_SECRET = "M6-xzfdi-BO96d_9chO4WwPWGcY"

cloudinary.config(
    cloud_name=CLOUD_NAME,
    api_key=API_KEY,
    api_secret=API_SECRET,
    secure=True
)
# Storage limit in bytes (Free tier = 25GB)
CLOUDINARY_STORAGE_LIMIT = 23_000_000_000  # 23GB


async def check_and_evict(message_date, required_space=0):
    """Check Cloudinary usage and evict LRU assets if needed."""
    try:
        usage = await asyncio.to_thread(cloudinary.api.usage)
        current_storage = usage["storage"]["usage"]

        needed = current_storage + required_space - CLOUDINARY_STORAGE_LIMIT
        if needed <= 0:
            return True

        print(f"[Eviction] Need to free {needed} bytes...")

        # Fetch LRU assets based on Telegram message date (ascending)
        lru_assets = find_documents(
            "cloudinary_assets_v2",
            sort_field="last_accessed",
            sort_order=1  # ascending
        )

        total_freed = 0
        for asset in lru_assets:
            # Only evict if the new asset (to be uploaded) is newer than the asset in DB
            if message_date is not None and asset.get("last_accessed") is not None:
                if message_date <= asset["last_accessed"]:
                    continue  # Skip eviction if new asset is not newer

            if total_freed >= needed:
                break

            try:
                await asyncio.to_thread(
                    cloudinary.uploader.destroy,
                    asset["public_id"],
                    invalidate=True
                )
                delete_document("cloudinary_assets_v2", {"_id": asset["_id"]})
                total_freed += asset["size"]
                print(f"✅ Evicted: {asset['public_id']} ({asset['size']} bytes)")
            except Exception as e:
                print(f"❌ Failed to evict {asset['public_id']}: {str(e)}")

        print(f"[Eviction] Total freed: {total_freed} bytes")
        return total_freed >= needed

    except Exception as e:
        print(f"❌ Eviction check failed: {str(e)}")
        return False


async def upload_with_eviction(file_path, asset_type="post_photo", channel_id=None, message_id=None, message_date=None):
    """
    Upload a file to Cloudinary, evicting LRU assets if storage limit is exceeded.
    Uses Telegram message date as 'last_accessed' timestamp.
    """

    try:
        file_size = os.path.getsize(file_path)

        # Evict if necessary
        success = await check_and_evict(message_date, file_size)
        if not success:
            print("Insufficient storage even after eviction.")
            return None

        # Upload to Cloudinary
        upload_result = await asyncio.to_thread(
            cloudinary.uploader.upload,
            file_path,
            folder=asset_type
        )

        # Use message date if provided, otherwise default to now
        now = datetime.now(timezone.utc)
        accessed_time = message_date if message_date is not None else now

        # Build asset record
        asset_data = {
            "public_id": upload_result["public_id"],
            "url": upload_result["secure_url"],
            "uploaded_at": now,
            "last_accessed": accessed_time,
            "size": upload_result["bytes"],
            "type": asset_type
        }

        # Optional: store channel/message linkage
        if channel_id is not None:
            asset_data["channel_id"] = channel_id
        if message_id is not None:
            asset_data["message_id"] = message_id

        # Save to DB
        insert_document("cloudinary_assets_v2", asset_data)

        return asset_data  # Return for caller (e.g., for structured update)

    except Exception as e:
        print(f"Upload failed: {str(e)}")
        return None

async def upload_channel_thumbnail(photo_bytes):
    """
    Upload a channel thumbnail to Cloudinary.
    """
    try:
        response = cloudinary.uploader.upload(
            photo_bytes,
            folder="channel_thumbnails",
            transformation=[
                {"width": 480, "height": 480, "crop": "fill"},
                {"quality": "auto:best"}
            ]
        )
        return response['secure_url']

    except Exception as e:
        print(f"Thumbnail upload failed: {str(e)}")
        return None