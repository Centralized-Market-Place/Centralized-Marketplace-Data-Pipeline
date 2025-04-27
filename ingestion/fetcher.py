import os
import json
import tqdm
import asyncio
import cloudinary
import cloudinary.api
from tqdm import tqdm
import cloudinary.uploader
from datetime import datetime
from collections import defaultdict
from telethon import TelegramClient
from telethon.tl.types import PeerChannel
from cloudinary.utils import cloudinary_url
from telethon.tl.custom.message import Message
from ingestion.constants import CHANNEL_IDS, ALL_CHANNEL_IDS, NEW_CHANNEL_IDS
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from storage.store import store_raw_data, fetch_stored_messages, store_products, store_channels, store_latest_and_oldest_ids, fetch_all_channels
from storage.store import insert_document, update_document, delete_document, find_documents

# telegram
API_ID = os.getenv("API_ID", "")
API_HASH = os.getenv("API_HASH", "")
SESSION_NAME = os.getenv("SESSION_NAME", "telegram_client")
client = TelegramClient(SESSION_NAME, API_ID, API_HASH)


# cloudinary
CLOUD_NAME = os.getenv("CLOUD_NAME", "")
API_KEY = os.getenv("API_KEY", "")
API_SECRET = os.getenv("API_SECRET", "")
cloudinary.config(
    cloud_name=CLOUD_NAME,
    api_key=API_KEY,
    api_secret=API_SECRET,
    secure=True
)
# Storage limit in bytes (Free tier = 1GB)
CLOUDINARY_STORAGE_LIMIT = 751106637 # 1073741824 without existing images

#=========================== messages ============================

async def check_and_evict(required_space=0):
    """Check storage and evict LRU assets if needed"""
    usage = await asyncio.to_thread(cloudinary.api.usage)
    current_storage = usage["storage"]["usage"]
    
    needed = current_storage + required_space - CLOUDINARY_STORAGE_LIMIT
    if needed <= 0:
        return True

    print(f"Storage overage detected. Need to free {needed} bytes")

    lru_assets = find_documents("cloudinary_assets", sort_field="last_accessed", sort_order=1)
    total_freed = 0

    for asset in lru_assets:
        if total_freed >= needed:
            break

        try:
            await asyncio.to_thread(
                cloudinary.uploader.destroy, 
                asset["public_id"],
                invalidate=True
            )
            delete_document("cloudinary_assets", {"_id": asset["_id"]})
            total_freed += asset["size"]
            print(f"Evicted: {asset['public_id']} ({asset['size']} bytes)")
        except Exception as e:
            print(f"Failed to evict {asset['public_id']}: {str(e)}")

    print(f"Total freed: {total_freed} bytes")
    return total_freed >= needed

async def upload_with_eviction(file_path, asset_type="post_photo"):
    """Upload file with LRU eviction when needed"""
    try:
        file_size = os.path.getsize(file_path)
        
        success = await check_and_evict(file_size)
        if not success:
            print("Insufficient storage after eviction")
            return None

        upload_result = await asyncio.to_thread(
            cloudinary.uploader.upload,
            file_path,
            folder=asset_type
        )

        asset_data = {
            "public_id": upload_result["public_id"],
            "url": upload_result["secure_url"],  # Using secure_url for public display
            "uploaded_at": datetime.now(),
            "last_accessed": datetime.now(),
            "size": upload_result["bytes"],
            "type": asset_type
        }
        insert_document("cloudinary_assets", asset_data)

        return upload_result["secure_url"]  # Returning secure_url for public display
    
    except Exception as e:
        print(f"Upload failed: {str(e)}")
        return None

async def update_last_accessed(public_id):
    """Update access time when serving assets"""
    update_document(
        "cloudinary_assets",
        {"public_id": public_id},
        {"last_accessed": datetime.now()}
    )

async def download_images(messages, tg_client):
    grouped_messages = defaultdict(list)
    images = defaultdict(list)
    
    # to be removed
    # return images

    for message in tqdm(messages, desc="Downloading Single Photos: "):
        await asyncio.sleep(2)  # Rate limit
        
        if message.grouped_id:  # Grouped media (album)
            grouped_messages[message.grouped_id].append(message)

        elif message.media and hasattr(message.media, 'photo'):  # Single photo
            try:
                file = await tg_client.download_media(message.media)
                if file:
                    url = await upload_with_eviction(file)
                    if url:
                        channel_id = message.peer_id.channel_id if isinstance(message.peer_id, PeerChannel) else None
                        if channel_id:
                            images[(channel_id, message.id)].append(url)
                    else:
                        print(f"Failed to upload image for message ID {message.id}")
                    os.remove(file)
            finally:
                if file and os.path.exists(file):
                    os.remove(file)

    # Now process grouped messages (albums)
    for grouped_id, msgs in tqdm(grouped_messages.items(), desc="Downloading Albums: "):
        for grouped_msg in msgs:
            if grouped_msg.media and hasattr(grouped_msg.media, 'photo'):
                await asyncio.sleep(2)  # Rate limit
                try:
                    file = await tg_client.download_media(grouped_msg.media)
                    if file:
                        url = await upload_with_eviction(file)
                        if url:
                            # Use channel_id and grouped_msg.id as the key for grouping
                            channel_id = grouped_msg.peer_id.channel_id if isinstance(grouped_msg.peer_id, PeerChannel) else None
                            if channel_id:
                                images[(channel_id, grouped_id)].append(url)
                        else:
                            print(f"Failed to upload image for grouped message ID {grouped_msg.id}")
                        os.remove(file)
                finally:
                    if file and os.path.exists(file):
                        os.remove(file)
        # print(f"Grouped messages with grouped_id {grouped_id} processed.")
    
    return images



# look into the limit
async def fetch_unread_messages(channel_username, channel_mongo_id, last_fetched_id, tg_client, limit=5, delay=2, round=1): # 4*30*30 = 3600
    """Fetch unread messages from the specified Telegram channel"""

    messages = []
    channel_id = None
    new_last_fetched_id = last_fetched_id if last_fetched_id else float('inf')

    for _ in tqdm(range(round), desc="Fetching Messages: "):
        if new_last_fetched_id != float('inf'):
            async for message in tg_client.iter_messages(channel_username, limit=limit, offset_id=new_last_fetched_id):
                messages.append(message)
                new_last_fetched_id = min(new_last_fetched_id, message.id)
        else:
            async for message in tg_client.iter_messages(channel_username, limit=limit):
                messages.append(message)
                new_last_fetched_id = min(new_last_fetched_id, message.id)
        await asyncio.sleep(delay)
    
    messages_json = [json.loads(message.to_json()) for message in tqdm(messages, desc="Decoding Messages to JSON: ")]
    stored = store_raw_data(messages_json, collection_name="raw_data")
    if not stored:
        print('Storage failed !!!!!')
    
    # store last fetched info
    last_fetched_info = {
        "channel_id": channel_username,
        "last_fetched_id": new_last_fetched_id
        }
    store_raw_data([last_fetched_info], collection_name="last_fetched_info")
    

    images = await download_images(messages, tg_client)
    if images:
        for message_json in tqdm(messages_json, desc="Attaching Images: "):
            channel_id = message_json['peer_id']['channel_id'] if 'peer_id' in message_json and 'channel_id' in message_json['peer_id'] else None
            grouped_id = message_json.get('grouped_id')
            message_images = []
            if grouped_id:
                message_images = images.get((channel_id, grouped_id), [])
            elif channel_id:
                message_id = message_json['id']
                message_images = images.get((channel_id, message_id), [])
            message_json['images'] = message_images
        
        # Store products in MongoDB
        # stored_products = store_raw_data(messages_json)
        # if not stored_products:
        #     print('Failed to store products!')

    # Extract and store simplified message data
    simplified_messages = []
    for msg in messages_json:
        extracted = extract_message_data(msg, channel_mongo_id)
        if extracted:
            simplified_messages.append(extracted)

    stored_simplified = store_products(simplified_messages)
    if not stored_simplified:
        print('Failed to store products data!')
        return 
    print(f'Messages fetched and stored from {channel_username}')
    return

def extract_message_data(message_obj, channel_mongo_id):
    try:
        message_id = message_obj.get('id')
        
        peer_id = message_obj.get('peer_id', {})
        channel_id = peer_id.get('channel_id') if peer_id.get('_') == 'PeerChannel' else None
        channel_mongo_id = channel_mongo_id
        date = message_obj.get('date')
        message = message_obj.get('message', '')
        if not message:
            return
        # Check if the message contains at least 50% Latin characters/numbers
        latin_count = sum(1 for char in message if char.isalnum() and char.isascii())
        if latin_count / len(message) < 0.5:
            return
        
        forwards = message_obj.get('forwards', 0)
        views = message_obj.get('views', 0)
        images = message_obj.get('images', [])
        
        reactions_data = []
        try:
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

        except Exception as e:
            print(f"Error extracting reactions: {e}")
            
        return {
            'message_id': message_id,
            'telegram_channel_id': channel_id,
            'channel_id': channel_mongo_id,
            'date': date,
            'description': message,
            'forwards': forwards,
            'views': views,
            'reactions': reactions_data,
            'images': images,
            'updated_at': message_obj.get('updated_at'),
            "upvotes": 0,
            "downvotes": 0,
            "shares": 0,
            "comments": 0
        }
        
    except Exception as e:
        print(f"Error processing message: {e}")
        return None
    
async def fetch_runner():
    await client.start()
    # todo 
    ALL_CHANNEL_USERNAMES_AND_MONGO_IDS = [(channel["channel"], channel["channel_info_id"]) for channel in fetch_all_channels()]
    # see if we have their info in db 
    for username, channel_id in tqdm(ALL_CHANNEL_USERNAMES_AND_MONGO_IDS, desc="Channels fetched: "):
        # last_fetched_info = fetch_last_fetched_info(channel_id)
        # last_fetched_id = last_fetched_info.get('last_fetched_id')
        await fetch_unread_messages(username, channel_id, 0, client)
    await client.disconnect()


#======================== channel details =========================
async def join_channel(channel, tg_client):
    await tg_client(JoinChannelRequest(channel))

async def get_channel_participants_count(channel, tg_client):
    await asyncio.sleep(2)
    full_channel  = await tg_client(GetFullChannelRequest(channel))
    return full_channel.full_chat.participants_count

async def download_channel_thumbnail(channel, tg_client):
    """Download and upload channel thumbnail to Cloudinary"""
    try:
        # Download thumbnail bytes
        photo_bytes = await tg_client.download_profile_photo(channel, file=bytes)
        if not photo_bytes:
            return None
            
        # Upload to Cloudinary
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
        print(f"Thumbnail upload failed for {channel.title}: {str(e)}")
        return None

async def fetch_channel_info(channel_username, channel_pool_id, tg_client):
    """Fetch channel info with thumbnail URL"""
    try:
        await asyncio.sleep(2)  # Rate limiting
        channel = await tg_client.get_entity(channel_username)
        
        # Get base channel info
        channel_info = {
            "id": channel.id,
            "title": channel.title,
            "username": channel.username,
            "pool_entry_id": channel_pool_id,
            "description": getattr(channel, "about", ""),
            "participants": await get_channel_participants_count(channel, tg_client),
            "date_created": channel.date.isoformat(),
            "verified": channel.verified,
            "thumbnail_url": None,  # Initialize as None
            "restricted": channel.restricted,  # Is the channel restricted?
            "scam": channel.scam,  # Is it marked as a scam?
            "has_link": channel.has_link,  # Does it have an invite link?
            "has_geo": channel.has_geo,  # Does it have a location?
            "photo_id": channel.photo.photo_id if channel.photo else None,  # Profile photo ID
        }
        
        # Add thumbnail URL if available
        channel_info["thumbnail_url"] = await download_channel_thumbnail(channel, tg_client)
        
        return channel_info
    except Exception as e:
        print(f"Error fetching channel info: {str(e)}")
        return None

# async def fetch_channel_info(channel_username, tg_client):
#     try:
#         await asyncio.sleep(2)
#         channel = await tg_client.get_entity(channel_username)
#         participants_count = await get_channel_participants_count(channel, tg_client)
#         channel_info = {
#                 "id": channel.id,
#                 "title": channel.title,
#                 "username": channel.username,
#                 "description": getattr(channel, "about", ""),  # Channel description
#                 "participants": participants_count,  # Number of members
#                 "date_created": channel.date.isoformat(),  # Channel creation date
#                 "broadcast": channel.broadcast,  # Is it a broadcast channel?
#                 "verified": channel.verified,  # Verified status
#                 "restricted": channel.restricted,  # Is the channel restricted?
#                 "scam": channel.scam,  # Is it marked as a scam?
#                 "has_link": channel.has_link,  # Does it have an invite link?
#                 "has_geo": channel.has_geo,  # Does it have a location?
#                 "photo_id": channel.photo.photo_id if channel.photo else None,  # Profile photo ID
#             }
#         return channel_info
#     except Exception as e:
#         print(f"Error getting channel info {channel_username}")
        

async def fetch_bulk_channel_info(usernames, tg_client):
    """usernames: a list of channel usernames"""
    channel_infos = []
    for channel_username in tqdm(usernames, desc="Fetching infos of channels from Telegram"):
        assert False # channel_pool_id parameter
        channel_info = await fetch_channel_info(channel_username, tg_client)
        if channel_info:
            channel_infos.append(channel_info)
    
    return channel_infos

async def fetch_all_channels_runner():
    await client.start()
    channel_infos = await fetch_bulk_channel_info(ALL_CHANNEL_IDS, client)
    stored = store_channels(channel_infos)
    if not stored:
        print('Failed to store channels!')
    await client.disconnect()

async def fetch_channel_runner(channel_username: str):
    await client.start()
    channel_info = await fetch_channel_info(channel_username, client)
    stored = store_channels([channel_info])
    if not stored:
        print('Failed to store channel!')
    await client.disconnect()
