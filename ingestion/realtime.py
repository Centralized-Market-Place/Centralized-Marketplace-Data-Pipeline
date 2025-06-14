import os
import asyncio
from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from collections import defaultdict
import hashlib

from pymongo import MongoClient
from tqdm import tqdm
# from dotenv import dotenv_values
from datetime import datetime, timezone
from bson import ObjectId

# custom
from processing.extractor import extract
from storage.generic_store import insert_document, update_document, update_document_if_not_updated_by_seller, delete_document, find_documents
from ingestion.image_upload import upload_with_eviction, upload_channel_thumbnail

# MONGO_URI = "mongodb+srv://milliontolessa:mRZA8ra4IQmudFuQ@products.6xxlile.mongodb.net/?retryWrites=true&w=majority&appName=products"
# DATABASE_NAME = "fyp-search"

# MONGO_URI="mongodb+srv://semahegnsahib:sahib@cluster0.vmyk3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# DATABASE_NAME="centeral_marketplace"
# config = dotenv_values('secrets.env')
# mongo_client = MongoClient(MONGO_URI)
# db = mongo_client[DATABASE_NAME]

API_ID = "21879721"
API_HASH = "cadd93c819128f73ba3439a0f430e677"
SESSION_NAME = 'telegram_client'

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
message_queue = asyncio.Queue()

# all_channels = []  # Will be updated once per day
# all_channels = [
#     { "username":"@just_for_a_test", "_id": "1234567890" },
#     # { "username":"@just_for_a_test2", "_id": "1234567891" },
# ]

channel_id_to_full_info_map = {}

album_buffer = defaultdict(list)
album_timers = {}

ALBUM_TIMEOUT = 2  # seconds to wait for all album parts
TG_RATE_LIMITING_SECONDS = 2  # max requests per second
last_request_time = 0  # to track last request time

# request telegram api with rate limiting
rate_limit_lock = asyncio.Lock()

# async def request_with_rate_limit(func, *args, **kwargs):
#     global last_request_time
#     current_time = asyncio.get_event_loop().time()
#     if current_time - last_request_time < TG_RATE_LIMITING_SECONDS:
#         sleep_time = TG_RATE_LIMITING_SECONDS - (current_time - last_request_time)
#         await asyncio.sleep(sleep_time)

#     last_request_time = asyncio.get_event_loop().time()
#     return await func(*args, **kwargs)

def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

async def request_with_rate_limit(func, *args, **kwargs):
    global last_request_time
    async with rate_limit_lock:
        current_time = asyncio.get_event_loop().time()
        if current_time - last_request_time < TG_RATE_LIMITING_SECONDS:
            sleep_time = TG_RATE_LIMITING_SECONDS - (current_time - last_request_time)
            await asyncio.sleep(sleep_time)
        last_request_time = asyncio.get_event_loop().time()
        return await func(*args, **kwargs)


def fetch_all_channels(collection_name='channels-realtime-test'):
    global channel_id_to_full_info_map
    # return all_channels
    try:
        # fetch from db -> channel_info colletion
        all_channels_from_db = find_documents(collection_name, query=None, sort_field=None, sort_order=1)
        # take telegram_id from each channel
        # todo 
        # if channel is deleted or is suspended, skip it 

        all_channels_ids = []
        new_channels_info_map = {}
        for channel in all_channels_from_db:
            if channel and 'telegram_id' in channel:
                channel_id = channel['telegram_id']
                all_channels_ids.append(channel_id)
                new_channels_info_map[channel_id] = channel
            
        channel_id_to_full_info_map = new_channels_info_map
        return all_channels_ids
                            
    except Exception as e:
        print(str(e))

    return []


all_channels = []

async def is_participant(channel, user) -> bool:
    try:
        # await asyncio.sleep(2)  # Avoid hitting rate limits
        # await client.get_permissions(channel, user)
        await request_with_rate_limit(client.get_permissions, channel, user)
        return True
    except errors.UserNotParticipantError:
        return False

'''
1472934983
2500257537
# Printed Messages Note
## Just text
Message(id=82, peer_id=PeerChannel(channel_id=2672743939), date=datetime.datetime(2025, 5, 23, 12, 16, 25, tzinfo=datetime.timezone.utc), message='her', out=False, mentioned=False, media_unread=False, silent=False, post=True, from_scheduled=False, legacy=False, edit_hide=False, pinned=False, noforwards=False, invert_media=False, offline=False, video_processing_pending=False, from_id=None, from_boosts_applied=None, saved_peer_id=None, fwd_from=None, via_bot_id=None, via_business_bot_id=None, reply_to=None, media=None, reply_markup=None, entities=[], views=1, forwards=0, replies=None, edit_date=None, post_author=None, grouped_id=None, reactions=None, restriction_reason=[], ttl_period=None, quick_reply_shortcut_id=None, effect=None, factcheck=None)

## Photo
Message(id=83, peer_id=PeerChannel(channel_id=2672743939), date=datetime.datetime(2025, 5, 23, 12, 17, 14, tzinfo=datetime.timezone.utc), message='image test', out=False, mentioned=False, media_unread=False, silent=False, post=True, from_scheduled=False, legacy=False, edit_hide=False, pinned=False, noforwards=False, invert_media=False, offline=False, video_processing_pending=False, from_id=None, from_boosts_applied=None, saved_peer_id=None, fwd_from=None, via_bot_id=None, via_business_bot_id=None, reply_to=None, media=MessageMediaPhoto(spoiler=False, photo=Photo(id=5872708002040104841, access_hash=2799917039853593851, file_reference=b'\x05\x00\x00\x00\x00\x9fN\xd6\x03\x00\x00\x00Sh0gI\x8f3\xee\xc3X\xf1c>\xa3S%\x0cU\xb5\xb8q', date=datetime.datetime(2025, 5, 23, 12, 17, 13, tzinfo=datetime.timezone.utc), sizes=[PhotoStrippedSize(type='i', bytes=b"\x01\x17(\xcf\xe4\xf4\x14\xf1\x13\x94\xdf\xb4\xed'\x19\xa9\xd5Bc\x83VD\x8a\xd1y{0x\xf7\xadlA\x9e\xb0\x93\x9c\xd0c\xd8\xdc\x8c\xd6\x9f\x98\xd9\xfb\x8a?\xe0\x14\xc7q\xb7i\x8e<\x9e\xfby\xa1\xa0Fq\x00\x83\xc61EO*|\xa7\x14T\xec2\xd6\xd1\x8a\x8c\xae\x1b\x8a(\xad\t\x0cQ\x8e(\xa2\x93\x1a\x0cg4QE!\x9f"), PhotoSize(type='m', w=320, h=180, size=11676), PhotoSize(type='x', w=800, h=450, size=41568), PhotoSizeProgressive(type='y', w=1280, h=720, sizes=[8132, 14471, 19575, 26756, 48537])], dc_id=4, has_stickers=False, video_sizes=[]), ttl_seconds=None), reply_markup=None, entities=[], views=1, forwards=0, replies=None, edit_date=None, post_author=None, grouped_id=None, reactions=None, restriction_reason=[], ttl_period=None, quick_reply_shortcut_id=None, effect=None, factcheck=None)

## Album
Message(id=84, peer_id=PeerChannel(channel_id=2672743939), date=datetime.datetime(2025, 5, 23, 12, 18, 45, tzinfo=datetime.timezone.utc), message='Album test', out=False, mentioned=False, media_unread=False, silent=False, post=True, from_scheduled=False, legacy=False, edit_hide=False, pinned=False, noforwards=False, invert_media=False, offline=False, video_processing_pending=False, from_id=None, from_boosts_applied=None, saved_peer_id=None, fwd_from=None, via_bot_id=None, via_business_bot_id=None, reply_to=None, media=MessageMediaPhoto(spoiler=False, photo=Photo(id=5872708002040104842, access_hash=4257766869210463856, file_reference=b'\x05\x00\x00\x00\x00\x9fN\xd6\x03\x00\x00\x00Th0g\xa5i\xa9i\x9dUu\x85\xa0M\x0f\xd3\xfb\xf0\x95\xf6i', date=datetime.datetime(2025, 5, 23, 12, 18, 44, tzinfo=datetime.timezone.utc), sizes=[PhotoStrippedSize(type='i', bytes=b'\x01$(\xcb\xcd\x14\x94P\x02\xd2R\xd2P\x01E\x14P\x02\xf1\xebF)GN\x99\xa3\xf0\xa0\x04\xa2\x97\x9a9\xf4\xa0\x04\xa2\x83\xf4\xa2\x80\x14\x1c\n3E\x14\x00f\x82M\x14P\x02\x13\x9cQE\x14\x01'), PhotoSize(type='m', w=320, h=292, size=13480), PhotoSize(type='x', w=800, h=731, size=57509), PhotoSizeProgressive(type='y', w=1219, h=1114, sizes=[8726, 20279, 41008, 57692, 88516])], dc_id=4, has_stickers=False, video_sizes=[]), ttl_seconds=None), reply_markup=None, entities=[], views=1, forwards=0, replies=None, edit_date=None, post_author=None, grouped_id=13984021804080332, reactions=None, restriction_reason=[], ttl_period=None, quick_reply_shortcut_id=None, effect=None, factcheck=None)
✅ Processed new message from just_for_a_test: 84
Message(id=85, peer_id=PeerChannel(channel_id=2672743939), date=datetime.datetime(2025, 5, 23, 12, 18, 45, tzinfo=datetime.timezone.utc), message='', out=False, mentioned=False, media_unread=False, silent=False, post=True, from_scheduled=False, legacy=False, edit_hide=False, pinned=False, noforwards=False, invert_media=False, offline=False, video_processing_pending=False, from_id=None, from_boosts_applied=None, saved_peer_id=None, fwd_from=None, via_bot_id=None, via_business_bot_id=None, reply_to=None, media=MessageMediaPhoto(spoiler=False, photo=Photo(id=5872708002040104843, access_hash=-1416379684915169549, file_reference=b'\x05\x00\x00\x00\x00\x9fN\xd6\x03\x00\x00\x00Uh0g\xa5+^\xee.\x9f\xf1\x8a\xdbY\xe9\x89\xd1\x93\xdf\x98g', date=datetime.datetime(2025, 5, 23, 12, 18, 45, tzinfo=datetime.timezone.utc), sizes=[PhotoStrippedSize(type='i', bytes=b'\x01$(\xcc\x07\x8a3\x9aN\xd4s\xed@\x0b\xf9\xd2~tRP\x02\xe6\x8aJ(\x01x\xf7\xa3\x8ap\xe9\xd2\x8f\xc2\x80\x1bE;\x14b\x80\x1bE)\xe9E\x00 <R\xe4\xd1E\x00&O\xad\x19>\xb4Q@\x06O\xad\x14Q@\x1f'), PhotoSize(type='m', w=320, h=291, size=18882), PhotoSize(type='x', w=800, h=728, size=79441), PhotoSizeProgressive(type='y', w=1118, h=1017, sizes=[8947, 23512, 50910, 71816, 108859])], dc_id=4, has_stickers=False, video_sizes=[]), ttl_seconds=None), reply_markup=None, entities=[], views=1, forwards=0, replies=None, edit_date=None, post_author=None, grouped_id=13984021804080332, reactions=None, restriction_reason=[], ttl_period=None, quick_reply_shortcut_id=None, effect=None, factcheck=None)
✅ Processed new message from just_for_a_test: 85

'''

async def get_channel_about_and_participants_count(channel):
    # await asyncio.sleep(2)
    full_channel = await request_with_rate_limit(client, GetFullChannelRequest(channel))
    return full_channel.full_chat.about, full_channel.full_chat.participants_count

async def download_channel_thumbnail(channel, channel_id):
    """Download and upload channel thumbnail to Cloudinary"""
    try:
        # Download thumbnail bytes
        # await asyncio.sleep(2)  # Avoid hitting rate limits
        photo_bytes = await request_with_rate_limit(client.download_profile_photo, channel, file=bytes)
        if not photo_bytes:
            return None, None
        
        stored_hash = channel_id_to_full_info_map.get(channel_id, {}).get('thumbnail_hash')
        current_hash = hash_bytes(photo_bytes)
        if stored_hash and stored_hash == current_hash:
            print(f"Thumbnail for channel {channel.title} already exists in Cloudinary.")
            return channel_id_to_full_info_map[channel_id].get('thumbnail_url'), current_hash

        # print('❗❗ Uploading channel thumbnail to Cloudinary ❗❗')
        secure_url = await upload_channel_thumbnail(photo_bytes)
        return secure_url, current_hash 

    except Exception as e:
        print(f"Thumbnail upload failed for {channel.title}: {str(e)}")
        return None, None

async def fetch_channel_info(channel_id, entity):
    try:
        # await asyncio.sleep(2)  # Rate limiting
        about, participants_count = await get_channel_about_and_participants_count(entity)
        channel_info = {
            "telegram_id": entity.id,
            "title": entity.title,
            "username": entity.username,
            # "pool_entry_id": channel_pool_id,
            "description": about,
            "participants": participants_count,
            "date_created": entity.date.isoformat(),
            "verified": entity.verified,
            "thumbnail_url": None,  # Initialize as None
            "restricted": entity.restricted,  # Is the channel restricted?
            "scam": entity.scam,  # Is it marked as a scam?
            "has_link": entity.has_link,  # Does it have an invite link?
            "has_geo": entity.has_geo,  # Does it have a location?
            "photo_id": getattr(entity.photo, "photo_id", None) if entity.photo else None,  # Profile photo ID
        }
        
        # Add thumbnail URL if available
        thumbnail, hashval = await download_channel_thumbnail(entity, channel_id)
        channel_info["thumbnail_url"] = thumbnail
        channel_info["thumbnail_hash"] = hashval  # Store the hash for future checks
        
        return channel_info
    except Exception as e:
        print(f"Error fetching channel info: {str(e)}")
        return None

async def refresh_channels_periodically(client, interval_hours=24):
    global all_channels
    me = await request_with_rate_limit(client.get_me)
    while True:
        all_channels = fetch_all_channels()
        print(f"[INFO] Refreshed all_channels at {datetime.now(timezone.utc)}. {len(all_channels)} channels loaded.")
        # Attempt to join channels if not already a member

        for channel_id in all_channels:
            # channel_id = channel.get("telegram_id")
            # if not username:
            #     continue
            for i in range(2):
                try:
                    # await asyncio.sleep(2)  # Avoid hitting rate limits
                    entity = await request_with_rate_limit(client.get_entity, channel_id)
                    # Try to get participant info to check membership

                    if await is_participant(entity, me):
                        print(f"[INFO] Already a member of channel: {channel_id} > {entity.username}")
                    else:
                        print(f"[INFO] Attempting to join channel: {channel_id} > {entity.username}")
                        # await asyncio.sleep(2)  # Avoid hitting rate limits
                        username = entity.username
                        await request_with_rate_limit(client, JoinChannelRequest(username))
                        print(f"[INFO] Joined channel: {channel_id}")
                    
                    full_channel_info = await fetch_channel_info(channel_id, entity)
                    # update_document(collection_name, filter_query, update_query)
                    update_document("channels-realtime-test", { "telegram_id": channel_id }, full_channel_info)
                    channel_id_to_full_info_map[channel_id] = full_channel_info
                    break
                except Exception as e:
                    username = channel_id_to_full_info_map.get(channel_id, {}).get('username', '')
                    if username:
                        try:
                            entity = await request_with_rate_limit(client.get_entity, username)
                            channel_id = entity.id
                            print(f"✅ resolved a username {username} to channel ID {channel_id} for username {username}.")
                        except Exception as e:
                            print(f"[ERROR] Could not fetch entity for channel {channel_id}: {e}")
                            break
                    else:
                        print(f"[WARN] Could not process channel {channel_id}: {e}")
                        break

        print(f"[INFO] Completed refresh of all channels at {datetime.now(timezone.utc)}.")
        print(f"✅ Refreshed all channels: {len(all_channels)} channels loaded.")
        await asyncio.sleep(interval_hours * 3600)



async def process_album(grouped_id, chat_id):
    key = (grouped_id, chat_id)
    messages = album_buffer.pop(key, [])
    if key in album_timers:
        album_timers.pop(key)
    if messages:
        print(f"Album timeout reached: queueing album from channel={chat_id} with grouped_id={grouped_id}, {len(messages)} parts")
        await message_queue.put(messages) 


def schedule_album_processing(grouped_id, chat_id):
    key = (grouped_id, chat_id)
    if key in album_timers:
        album_timers[key].cancel()
    album_timers[key] = asyncio.get_event_loop().call_later(
        ALBUM_TIMEOUT, lambda: asyncio.create_task(process_album(grouped_id, chat_id))
    )


def message_text_cleaner(text):
    try:
        text = text.strip().replace('\n', ' ').replace('\r', '')
        latin_count = sum(1 for char in text if char.isalnum() and char.isascii())
        if text and latin_count / len(text) < 0.5:
            return ""
        
        return text
    except Exception as e:
        print(f"Error cleaning message text: {e}")
        return ""


def extract_message_data(message_obj, channel_mongo_id):
    try:
        message_id = message_obj.get('id')
        # description = message_obj.get('message', '')
        peer_id = message_obj.get('peer_id', {})
        channel_id = peer_id.get('channel_id') if peer_id.get('_') == 'PeerChannel' else None
        channel_mongo_id = channel_mongo_id
        date = message_obj.get('date')
        
        forwards = message_obj.get('forwards', 0)
        views = message_obj.get('views', 0)
        images = message_obj.get('images', [])
        
        reactions_data = []
        try:
            reactions = message_obj.get('reactions', [])
            if not reactions:
                reactions_data = []
            elif isinstance(reactions, list):
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
            'channel_id': ObjectId(channel_mongo_id) if not isinstance(channel_mongo_id, ObjectId) else channel_mongo_id,
            'date': date,
            # 'description': description,
            'forwards': forwards,
            'views': views,
            'reactions': reactions_data,
            'images': images,
            'updated_at': datetime.now(timezone.utc),
            # 'embedding': doc_embedding, 
            # 'title': extracted.get('title'),
            # 'price': extracted.get('price'),
            # 'location': extracted.get('location'),
            # 'phone': extracted.get('phone'),
            # 'link': extracted.get('link'),
            # 'categories': extracted.get('categories', [])
        }
        
    except Exception as e:
        print(f"Error processing message: {e}")
        return None

async def periodic_chat_update(limit):
    global last_request_time
    for channel_id in tqdm(all_channels, desc="Fetching messages for refresh: "):
        last_fetched_id = None
        old_messages = []
        try:
            for _ in range(2):
                if last_fetched_id is not None:
                    async with rate_limit_lock:
                        current_time = asyncio.get_event_loop().time()
                        if current_time - last_request_time < TG_RATE_LIMITING_SECONDS:
                            sleep_time = TG_RATE_LIMITING_SECONDS - (current_time - last_request_time)
                            await asyncio.sleep(sleep_time)
                        last_request_time = asyncio.get_event_loop().time()
                        async for message in client.iter_messages(channel_id, limit=limit, offset_id=last_fetched_id):
                            old_messages.append(message)
                            last_fetched_id = min(last_fetched_id or float('inf'), message.id)

                else:
                    async with rate_limit_lock:
                        current_time = asyncio.get_event_loop().time()
                        if current_time - last_request_time < TG_RATE_LIMITING_SECONDS:
                            sleep_time = TG_RATE_LIMITING_SECONDS - (current_time - last_request_time)
                            await asyncio.sleep(sleep_time)
                        last_request_time = asyncio.get_event_loop().time()
                        async for message in client.iter_messages(channel_id, limit=limit):
                            old_messages.append(message)
                            last_fetched_id = min(last_fetched_id or float('inf'), message.id)

        except Exception as e:
            print(f"Error fetching messages for channel {channel_id}: {e}")
            continue

        for message in old_messages:
            try:
                mongo_id = channel_id_to_full_info_map.get(channel_id, {}).get('_id')
                if mongo_id:
                    message_data = extract_message_data(message.to_dict(), mongo_id)
                    if message_data:
                        updated_values = {
                            'updated_at': datetime.now(timezone.utc),
                            'forwards': message_data['forwards'],
                            'views': message_data['views'],
                            'reactions': message_data['reactions'],
                        }
                        # Check if message already exists in DB
                        # existing_message = db.messages.find_one({
                        #     'message_id': message_data['message_id'],
                        #     'channel_id': message_data['telegram_channel_id']
                        # })
                        """
                        def find_documents(collection_name, query=None, sort_field=None, sort_order=1):
                            query = query or {}
                            cursor = db[collection_name].find(query)
                            if sort_field:
                                cursor = cursor.sort(sort_field, sort_order)
                            return list(cursor)
                        """

                        matches = find_documents("structured_products-realtime-test",
                            query={
                                'message_id': message_data['message_id'],
                                'channel_id': message_data['telegram_channel_id']
                            }
                        )
                        existing_message = matches[0] if matches else None
                        if existing_message:
                            # Update existing message
                            update_document_if_not_updated_by_seller("structured_products-realtime-test",
                                {'message_id': message_data['message_id'], 'telegram_channel_id': message_data['telegram_channel_id']},
                                updated_values
                            )
                        
            except Exception as e:
                print(f"Error processing message: {e}")

async def periodic_chat_update_runner():
    while True:
        try:
            print(f"[INFO] Starting periodic chat update at {datetime.now(timezone.utc)}")
            await periodic_chat_update(limit=30)
            print(f"✅ Completed periodic chat update at {datetime.now(timezone.utc)}")
        except Exception as e:
            print(f"[ERROR] Periodic chat update failed: {e}")
        await asyncio.sleep(24 * 3600)  # Run every 24 hours

async def image_worker(tg_client):
    while True:
        messages = await message_queue.get()
        # messages is always a list of (message, chat, type) tuples
        images = []
        message_text = []
        process_type = 0
        # get message text before processing images
        for message, chat, p_type in messages:
            insert_document("raw_data", message.to_dict())
            process_type = p_type
            try:
                if message.message:
                    message_text.append(message.message)
            except Exception as e:
                print(f"Error getting message text: {e}")
        
        message_text_str = " ".join(message_text)
        cleaned_text = message_text_cleaner(message_text_str)
        if cleaned_text:
            # todo ❗
            # extract title, price, categories, ... from cleaned_text
            extracted, doc_embedding = extract(cleaned_text)
            if extracted:
                mongo_id = channel_id_to_full_info_map.get(chat.id, {}).get('_id')
                message_data = extract_message_data(message.to_dict(), mongo_id) # ❗ change to mongo id
                if message_data:
                    # extracted elements 
                    message_data['description'] = message_text_str
                    message_data['embedding'] = doc_embedding
                    message_data['title'] = extracted.get('title')
                    message_data['price'] = extracted.get('price')
                    message_data['location'] = extracted.get('location')
                    message_data['phone'] = extracted.get('phone')
                    message_data['link'] = extracted.get('link')
                    message_data['categories'] = extracted.get('categories', [])
                    if process_type == 0:
                        message_data['upvotes'] = 0
                        message_data['downvotes'] = 0
                        message_data['shares'] = 0
                        message_data['clicks'] = 0
                        message_data['comments'] = 0
                        message_data['updated_by_seller'] = False
                        message_data['deleted_by_seller'] = False
                        message_data['is_available'] = True
                        message_data['created_at'] = message_data['updated_at']

                    for message, chat, p_type in messages:
                        try:
                            if message.media:
                                if hasattr(message.media, 'photo'):
                                    print(f"Downloading photo from {chat.username}: {message.id}")
                                    # dir_path = f"downloads/{chat.username}"
                                    # # pass asdlfjad
                                    # if not os.path.exists(dir_path):
                                    #     os.makedirs(dir_path)
                                    # await request_with_rate_limit(
                                    #     tg_client.download_media, message.media.photo, f"{dir_path}/{message.id}.jpg"
                                    # )
                                    
                                    # ❗ upload image to cloudinary
                                    # image_url = f"{dir_path}/{message.id}.jpg"
                                    # images.append(image_url)
                                    try:
                                        # Use request_with_rate_limit to avoid hitting rate limits
                                        file = await request_with_rate_limit(
                                            tg_client.download_media, message.media.photo
                                        )
                                        if file:
                                            image_asset = await upload_with_eviction(file)
                                            if image_asset:
                                                url = image_asset['url']
                                                images.append(url)
                                                # print(f"✅ Uploaded image for message ID {message.id} to {url}")
                                            else:
                                                print(f"Failed to upload image for message ID {message.id}")
                                        if file and os.path.exists(file):
                                            os.remove(file)
                                    except Exception as e:
                                        print(f"Error downloading/uploading/removing image: {e}")
                                    finally:
                                        if file and os.path.exists(file):
                                            os.remove(file)
                                    
                                elif hasattr(message.media, 'document'):
                                    print(f"⚠️ Skipped document in message {message.id} from {chat.username}")
                                    # Handle document if needed
                                else:
                                    print(f"⚠️ Unknown media type in message {message.id} from {chat.username}")

                            else:
                                print(f"⚠️ No media in message {message.id} from {chat.username}")
                        except Exception as e:
                            print(f"Error processing message: {e}")

                    # add images to message_data
                    message_data['images'] = images
                    # ❗ commit messages to DB 
                    if process_type == 0:
                        insert_document("structured_products-realtime-test", message_data)
                    else:
                        update_document_if_not_updated_by_seller("structured_products-realtime-test",
                            {'message_id': message_data['message_id'], 'telegram_channel_id': message_data['telegram_channel_id']},
                            message_data
                        )
                    print(f"✅ Processed message {message.id} from {chat.username} with {len(images)} images.")
                 
        message_queue.task_done()
    



async def realtimeRunner():
    await client.start()  # Start the client before any API requests
    # Start background task to refresh channels daily
    asyncio.create_task(refresh_channels_periodically(client))
    asyncio.create_task(image_worker(client))
    await asyncio.sleep(2*60)  # Wait for the client to start and channels to be fetched
    asyncio.create_task(periodic_chat_update_runner())
    await asyncio.sleep(2)  # Give time for first fetch
    # def get_username_to_mongo_id():
    #     return {c["username"]: c["_id"] for c in all_channels if "username" in c and "_id" in c}
    # def get_channel_usernames():
    #     return list(get_username_to_mongo_id().keys())

    def is_watched_channel(event):
        """
        Previosly:
        # Remove '@' if present for matching
        # username = getattr(event.chat, "username", None)
        # if not username:
        #     return False
        # usernames = {c["username"].lstrip("@") for c in all_channels if "username" in c}
        # return username.lstrip("@") in usernames
        """
        chat_id = getattr(event.chat, "id", None)
        if not chat_id:
            return False
        return chat_id in all_channels
        
    await asyncio.sleep(2)  # Give time for event handlers to be set up
    @client.on(events.NewMessage(func=is_watched_channel))
    async def handler(event):
        try:
            message = event.message
            chat = event.chat
            grouped_id = getattr(message, "grouped_id", None)
            chat_id = getattr(chat, "id", None)
            if grouped_id and chat_id:
                album_buffer[(grouped_id, chat_id)].append((message, chat, 0))
                schedule_album_processing(grouped_id, chat_id)
            else:
                await message_queue.put([(message, chat, 0)])  # Always put a list for consistency
                print(f"✅ {message.id} enqueued from {chat.username}")
        except Exception as e:
            print(f"Error processing new message: {e}")

    await asyncio.sleep(2)  
    @client.on(events.MessageEdited(func=is_watched_channel))
    async def edit_handler(event):
        try:
            message = event.message
            chat = event.chat
            grouped_id = getattr(message, "grouped_id", None)
            chat_id = getattr(chat, "id", None)
            if grouped_id and chat_id:
                album_buffer[(grouped_id, chat_id)].append((message, chat, 1))
                schedule_album_processing(grouped_id, chat_id)
            else:
                await message_queue.put([(message, chat, 1)])  # Always put a list for consistency
                print(f"✅ {message.id} edited and enqueued from {chat.username}")
        except Exception as e:
            print(f"Error processing edited message: {e}")
    
    await asyncio.sleep(2)  # Give time for event handlers to be set up
    @client.on(events.MessageDeleted(func=is_watched_channel))
    async def delete_handler(event):
        try:
            deleted_messages = event.deleted_ids
            chat = event.chat
            chat_id = getattr(chat, "id", None)
            print(f"Deleted messages: {deleted_messages} from {chat.username}")
            if chat_id:
                for msg_id in deleted_messages:
                    res = delete_document('structured_products-realtime-test', {"telegram_channel_id": chat_id, "message_id": msg_id})
                    if res:
                        print(f"✅ Deleted message {msg_id} from {chat.username}")
                    else:
                        print(f"Document not in DB")
                    
        except Exception as e:
            print(f"Error processing deleted message: {e}")

    await asyncio.sleep(2)  # Give time for event handlers to be set up

    print("Listening for new messages...")
    await client.run_until_disconnected()

