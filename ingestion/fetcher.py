import os
import json
import tqdm
import asyncio
import cloudinary
from tqdm import tqdm
import cloudinary.uploader
from collections import defaultdict
from telethon import TelegramClient
from telethon.tl.types import PeerChannel
from cloudinary.utils import cloudinary_url
from telethon.tl.custom.message import Message
from ingestion.constants import CHANNEL_IDS, ALL_CHANNEL_IDS, NEW_CHANNEL_IDS
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from storage.store import store_raw_data, fetch_stored_messages, store_products, store_channels, store_latest_and_oldest_ids


# we should replace these with env variables
API_ID = "21879721"
API_HASH = "cadd93c819128f73ba3439a0f430e677"
SESSION_NAME = 'telegram_client'

# we should replace these values with env variables
cloudinary.config( 
    cloud_name = "dkgm7qlfb", 
    api_key = "429625611193934", 
    api_secret = "eqxt9DA9_gbPr-j1Iiz3wYQo7EI",
    secure=True
)

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

#=========================== messages ============================


# cloudinary
async def evict_asset():
    pass

# cloudinary
async def evict_and_insert_asset():
    pass


async def download_images(messages, tg_client):
    grouped_messages = defaultdict(list)
    images = defaultdict(list)
    
    # to be removed
    return images

    for message in tqdm(messages, desc="Downloading Single Photos: "):
        await asyncio.sleep(2)  # Rate limit
        
        if message.grouped_id:  # Grouped media (album)
            grouped_messages[message.grouped_id].append(message)

        elif message.media and hasattr(message.media, 'photo'):  # Single photo
            file = await tg_client.download_media(message.media)
            if file:
                response = cloudinary.uploader.upload(file)
                if response:
                    channel_id = message.peer_id.channel_id if isinstance(message.peer_id, PeerChannel) else None
                    if channel_id:
                        images[(channel_id, message.id)].append(response['url'])
                else:
                    print(f"Failed to upload image for message ID {message.id}")
                os.remove(file)
    

    
    # Now process grouped messages (albums)
    for grouped_id, msgs in tqdm(grouped_messages.items(), desc="Downloading Albums: "):
        for grouped_msg in msgs:
            if grouped_msg.media and hasattr(grouped_msg.media, 'photo'):
                await asyncio.sleep(2)  # Rate limit
                file = await tg_client.download_media(grouped_msg.media)
                if file:
                    response = cloudinary.uploader.upload(file)
                    if response:
                        # Use channel_id and grouped_msg.id as the key for grouping
                        channel_id = grouped_msg.peer_id.channel_id if isinstance(grouped_msg.peer_id, PeerChannel) else None
                        if channel_id:
                            images[(channel_id, grouped_id)].append(response['url'])
                    else:
                        print(f"Failed to upload image for grouped message ID {grouped_msg.id}")
                    os.remove(file)

        # print(f"Grouped messages with grouped_id {grouped_id} processed.")
    
    return images



# look into the limit
async def fetch_unread_messages(channel_username, last_fetched_id, tg_client, limit=30, delay=2, round=4): # 4*30*30 = 3600
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
    simplified_messages = [extract_message_data(msg) for msg in messages_json if extract_message_data(msg)]
    stored_simplified = store_products(simplified_messages)
    if not stored_simplified:
        print('Failed to store products data!')
        return 
    print(f'Messages fetched and stored from {channel_username}')
    return

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
    
async def fetch_runner():
    await client.start()
    for channel_id in tqdm(ALL_CHANNEL_IDS, desc="Channels fetched: "):
        # last_fetched_info = fetch_last_fetched_info(channel_id)
        # last_fetched_id = last_fetched_info.get('last_fetched_id')
        await fetch_unread_messages(channel_id, 0, client)
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

async def fetch_channel_info(channel_username, tg_client):
    """Fetch channel info with thumbnail URL"""
    try:
        await asyncio.sleep(2)  # Rate limiting
        channel = await tg_client.get_entity(channel_username)
        
        # Get base channel info
        channel_info = {
            "id": channel.id,
            "title": channel.title,
            "username": channel.username,
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