import os
import asyncio
from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from collections import defaultdict
import hashlib
import logging

from tqdm import tqdm
from datetime import datetime, timezone
from bson import ObjectId

from prometheus_client import start_http_server, Counter, Gauge, Histogram
import time

from dotenv import load_dotenv
load_dotenv()

# custom
from processing.extractor import extract
from storage.generic_store import insert_document, update_document, update_document_if_not_updated_by_seller, delete_document, find_documents, find_one_document
from storage.image_upload import upload_with_eviction, upload_channel_thumbnail, check_DB




# Start Prometheus metrics server on port 9000
start_http_server(9000)

MESSAGES_PROCESSED = Counter('messages_processed_total', 'Total messages processed')
MESSAGES_SKIPPED = Counter('messages_skipped_total', 'Messages skipped (duplicates/unchanged)')
ALBUMS_PROCESSED = Counter('albums_processed_total', 'Albums processed')
QUEUE_SIZE = Gauge('message_queue_size', 'Current message queue size')
API_CALLS = Counter('telegram_api_calls_total', 'Telegram API calls made')
RATE_LIMITS = Counter('rate_limit_events_total', 'Rate limit (FloodWait) events')
ERRORS = Counter('processing_errors_total', 'Total processing errors')
IMAGES_DOWNLOADED = Counter('images_downloaded_total', 'Images downloaded')
FAILED_DOWNLOADS = Counter('failed_downloads_total', 'Failed image downloads')
PROCESSING_TIME = Histogram('message_processing_seconds', 'Time spent processing a message')
AI_API_CALLS = Counter('ai_api_calls_total', 'Total AI API calls made')
AI_ERRORS = Counter('ai_api_errors_total', 'Total AI API errors')
AI_PROCESSING_TIME = Histogram('ai_processing_seconds', 'Time spent processing AI tasks')

# Setup logging
logger = logging.getLogger("CMP-Realtime")
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




API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")
SESSION_NAME = os.getenv("SESSION_NAME", 'telegram_client')

if not API_ID or not API_HASH:
    logger.error("API_ID and API_HASH must be set in environment variables.")
    raise ValueError("API_ID and API_HASH must be set in environment variables.")



client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
message_queue = asyncio.Queue()

channel_id_to_full_info_map = {}

album_buffer = defaultdict(list)
album_timers = {}

ALBUM_TIMEOUT = 2  # seconds to wait for all album parts
TG_RATE_LIMITING_SECONDS = 2  # max requests per second
last_request_time = 0  # to track last request time

rate_limit_lock = asyncio.Lock()

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
        API_CALLS.inc()
        try:
            return await func(*args, **kwargs)
        except errors.FloodWait as e:
            RATE_LIMITS.inc()
            logger.warning(f"FloodWait: sleeping for {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return await func(*args, **kwargs)
        except Exception as e:
            ERRORS.inc()
            raise

def fetch_all_channels(collection_name='channels'):
    global channel_id_to_full_info_map
    try:
        all_channels_from_db = find_documents(collection_name, query=None, sort_field=None, sort_order=1)
        all_channels_ids = []
        new_channels_info_map = {}
        for channel in all_channels_from_db:
            if channel.get('is_deleted') or channel.get('is_suspended') or channel.get('scam'):
                continue

            if channel and 'telegram_id' in channel:
                channel_id = channel['telegram_id']
                all_channels_ids.append(channel_id)
                new_channels_info_map[channel_id] = channel
        channel_id_to_full_info_map = new_channels_info_map
        return all_channels_ids
    except Exception as e:
        ERRORS.inc()
        logger.error(str(e))
    return []

all_channels = []

async def is_participant(channel, user) -> bool:
    try:
        await request_with_rate_limit(client.get_permissions, channel, user)
        return True
    except errors.UserNotParticipantError:
        return False
    except Exception as e:
        ERRORS.inc()
        logger.error(f"Error in is_participant: {e}")
        return False

async def get_channel_about_and_participants_count(channel):
    full_channel = await request_with_rate_limit(client, GetFullChannelRequest(channel))
    return full_channel.full_chat.about, full_channel.full_chat.participants_count

async def download_channel_thumbnail(channel, channel_id):
    try:
        photo_bytes = await request_with_rate_limit(client.download_profile_photo, channel, file=bytes)
        if not photo_bytes:
            return None, None
        stored_hash = channel_id_to_full_info_map.get(channel_id, {}).get('thumbnail_hash')
        current_hash = hash_bytes(photo_bytes)
        if stored_hash and stored_hash == current_hash:
            logger.info(f"Thumbnail for channel {channel.title} already exists in Cloudinary.")
            return channel_id_to_full_info_map[channel_id].get('thumbnail_url'), current_hash
        secure_url = await upload_channel_thumbnail(photo_bytes)
        return secure_url, current_hash 
    except Exception as e:
        ERRORS.inc()
        logger.error(f"Thumbnail upload failed for {channel.title}: {str(e)}")
        return None, None

async def fetch_channel_info(channel_id, entity):
    try:
        about, participants_count = await get_channel_about_and_participants_count(entity)
        channel_info = {
            "telegram_id": entity.id,
            "title": entity.title,
            "username": entity.username,
            "description": about,
            "participants": participants_count,
            "date_created": entity.date.isoformat(),
            "verified": entity.verified,
            "thumbnail_url": None,
            "restricted": entity.restricted,
            "scam": entity.scam,
            "has_link": entity.has_link,
            "has_geo": entity.has_geo,
            "photo_id": getattr(entity.photo, "photo_id", None) if entity.photo else None,
        }
        thumbnail, hashval = await download_channel_thumbnail(entity, channel_id)
        channel_info["thumbnail_url"] = thumbnail
        channel_info["thumbnail_hash"] = hashval
        return channel_info
    except Exception as e:
        ERRORS.inc()
        logger.error(f"Error fetching channel info: {str(e)}")
        return None

async def refresh_channels_periodically(client, interval_hours=24):
    global all_channels, channel_id_to_full_info_map
    me = await request_with_rate_limit(client.get_me)
    while True:
        all_channels = fetch_all_channels()
        logger.info(f"Refreshed all_channels at {datetime.now(timezone.utc)}. {len(all_channels)} channels loaded.")
        for channel_id in all_channels:
            for i in range(2):
                try:
                    entity = await request_with_rate_limit(client.get_entity, channel_id)
                    if await is_participant(entity, me):
                        logger.info(f"Already a member of channel: {channel_id} > {entity.username}")
                    else:
                        logger.info(f"Attempting to join channel: {channel_id} > {entity.username}")
                        username = entity.username
                        await request_with_rate_limit(client, JoinChannelRequest(username))
                        logger.info(f"Joined channel: {channel_id}")
                    full_channel_info = await fetch_channel_info(channel_id, entity)
                    
                    if not full_channel_info or channel_id_to_full_info_map.get(channel_id, {}).get('is_updated', False):
                        logger.info(f"Skipping update for channel {channel_id} as it is already updated by seller.")
                        continue 

                    upsert_id = update_document("channels", { "telegram_id": channel_id }, full_channel_info)
                    if upsert_id:
                        full_channel_info['_id'] = upsert_id
                    else:
                        full_channel_info['_id'] = channel_id_to_full_info_map.get(channel_id, {}).get('_id', None)
                    channel_id_to_full_info_map[channel_id] = full_channel_info
                    break
                except Exception as e:
                    username = channel_id_to_full_info_map.get(channel_id, {}).get('username', '')
                    if username:
                        try:
                            entity = await request_with_rate_limit(client.get_entity, username)
                            channel_id = entity.id
                            logger.info(f"Resolved a username {username} to channel ID {channel_id} for username {username}.")
                        except Exception as e:
                            ERRORS.inc()
                            logger.error(f"Could not fetch entity for channel {channel_id}: {e}")
                            break
                    else:
                        logger.warning(f"Could not process channel {channel_id}: {e}")
                        break
        logger.info(f"Completed refresh of all channels at {datetime.now(timezone.utc)}.")
        logger.info(f"Refreshed all channels: {len(all_channels)} channels loaded.")
        await asyncio.sleep(interval_hours * 3600)

async def process_album(grouped_id, chat_id):
    key = (grouped_id, chat_id)
    messages = album_buffer.pop(key, [])
    if key in album_timers:
        album_timers.pop(key)
    if messages:
        logger.info(f"Album timeout reached: queueing album from channel={chat_id} with grouped_id={grouped_id}, {len(messages)} parts")
        ALBUMS_PROCESSED.inc()
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
        ERRORS.inc()
        logger.error(f"Error cleaning message text: {e}")
        return ""

def extract_message_data(message_obj, channel_mongo_id):
    try:
        message_id = message_obj.get('id')
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
            ERRORS.inc()
            logger.error(f"Error extracting reactions: {e}")
        return {
            'message_id': message_id,
            'telegram_channel_id': channel_id,
            'channel_id': ObjectId(channel_mongo_id) if not isinstance(channel_mongo_id, ObjectId) else channel_mongo_id,
            'date': date,
            'forwards': forwards,
            'views': views,
            'reactions': reactions_data,
            'images': images,
            'updated_at': datetime.now(timezone.utc),
        }
    except Exception as e:
        ERRORS.inc()
        logger.error(f"Error processing message: {e}")
        return None

async def periodic_chat_update(limit):
    global last_request_time
    for channel_id in tqdm(all_channels, desc="Fetching messages for refresh: "):
        last_fetched_id = None
        old_messages = []
        entity = None
        
        try:
            entity = await request_with_rate_limit(client.get_entity, channel_id)

            for _ in range(3):
                if last_fetched_id is not None:
                    async with rate_limit_lock:
                        current_time = asyncio.get_event_loop().time()
                        if current_time - last_request_time < TG_RATE_LIMITING_SECONDS:
                            sleep_time = TG_RATE_LIMITING_SECONDS - (current_time - last_request_time)
                            await asyncio.sleep(sleep_time)
                        last_request_time = asyncio.get_event_loop().time()
                        API_CALLS.inc()
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
                        API_CALLS.inc()
                        async for message in client.iter_messages(channel_id, limit=limit):
                            old_messages.append(message)
                            last_fetched_id = min(last_fetched_id or float('inf'), message.id)
        except Exception as e:
            ERRORS.inc()
            logger.error(f"Error fetching messages for channel {channel_id}: {e}")
            continue
        
        logger.info(f"Fetched {len(old_messages)} messages from channel {channel_id}.")
        groups = defaultdict(list)
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
                        existing_message = find_one_document("structured_products",
                            filter_query={
                                'message_id': message_data['message_id'],
                                'telegram_channel_id': message_data['telegram_channel_id']
                            }
                        )
                        if existing_message:
                            update_document_if_not_updated_by_seller("structured_products",
                                {'message_id': message_data['message_id'], 'telegram_channel_id': message_data['telegram_channel_id']},
                                updated_values
                            )
                        else:
                            # enqueue no group messages for processing
                            # Always enqueue messages as a list of (message, entity, 2) tuples
                            
                            if not message.grouped_id:
                                await message_queue.put([(message, entity, 2)])
                                logger.info(f"Enqueued single message {message.id} from {channel_id} for processing.")
                            else:
                                groups[message.grouped_id].append((message, entity, 2))
                    else:
                        logger.warning(f"Message data extraction failed for message {message.id} in channel {channel_id}. Skipping.")
                        continue
                else:
                    logger.warning(f"Mongo ID not found for channel {channel_id}. Skipping message {message.id}.")
                    continue       
            except Exception as e:
                ERRORS.inc()
                logger.error(f"Error processing message: {e}")

        logger.info(f"Processing {len(groups)} old message groups from channel {channel_id}.")
        for grouped_id, messages in groups.items():
            await message_queue.put(messages)

async def periodic_chat_update_runner():
    while True:
        try:
            logger.info(f"Starting periodic chat update at {datetime.now(timezone.utc)}")
            await periodic_chat_update(limit=30)
            logger.info(f"Completed periodic chat update at {datetime.now(timezone.utc)}")
        except Exception as e:
            ERRORS.inc()
            logger.error(f"Periodic chat update failed: {e}")
        await asyncio.sleep(24 * 3600)

async def image_worker(tg_client):
    while True:
        QUEUE_SIZE.set(message_queue.qsize())
        messages = await message_queue.get()
        images = []
        message_text = []
        process_type = 0
        start_time = time.time()
        for message, chat, p_type in messages:
            insert_document("raw_data", message.to_dict())
            process_type = p_type
            try:
                if message.message:
                    message_text.append(message.message)
            except Exception as e:
                ERRORS.inc()
                logger.error(f"Error getting message text: {e}")
        message_text_str = " ".join(message_text)
        cleaned_text = message_text_cleaner(message_text_str)
        if cleaned_text:
            extracted, doc_embedding = extract(cleaned_text, AI_API_CALLS, AI_ERRORS, AI_PROCESSING_TIME)
            if extracted:
                mongo_id = channel_id_to_full_info_map.get(chat.id, {}).get('_id')
                message_data = extract_message_data(message.to_dict(), mongo_id)
                if message_data:
                    message_data['description'] = message_text_str
                    message_data['embedding'] = doc_embedding
                    message_data['title'] = extracted.get('title')
                    message_data['price'] = extracted.get('price')
                    message_data['location'] = extracted.get('location')
                    message_data['phone'] = extracted.get('phone')
                    message_data['link'] = extracted.get('link')
                    message_data['categories'] = extracted.get('categories', [])
                    if process_type == 0 or process_type == 2:
                        message_data['upvotes'] = 0
                        message_data['downvotes'] = 0
                        message_data['shares'] = 0
                        message_data['clicks'] = 0
                        message_data['comments'] = 0
                        message_data['is_updated'] = False
                        message_data['is_deleted'] = False
                        message_data['is_available'] = True
                        message_data['created_at'] = message_data['updated_at']
                    for message, chat, p_type in messages:
                        try:
                            if message.media:
                                if hasattr(message.media, 'photo'):
                                    logger.info(f"Downloading photo from {chat.username}: {message.id}")
                                    try:
                                        # check DB
                                        if process_type == 2:
                                            match = check_DB(message_data.get('message_id'), message_data.get('telegram_channel_id'))
                                            if match:
                                                url = match.get('url')
                                                if url:
                                                    images.append(url)
                                                    logger.info(f"Image already exists in DB for message {message.id} from {chat.username}")
                                                    continue
                                        file = await request_with_rate_limit(
                                            tg_client.download_media, message.media.photo
                                        )
                                        if file:
                                            IMAGES_DOWNLOADED.inc()
                                            # upload message_id, channel_id
                                            image_asset = await upload_with_eviction(file, message_id=message_data.get('message_id'), channel_id=message_data.get('telegram_channel_id'))
                                            if image_asset:
                                                url = image_asset['url']
                                                images.append(url)
                                            else:
                                                FAILED_DOWNLOADS.inc()
                                                logger.warning(f"Failed to upload image for message ID {message.id}")
                                        if file and os.path.exists(file):
                                            os.remove(file)
                                    except Exception as e:
                                        FAILED_DOWNLOADS.inc()
                                        ERRORS.inc()
                                        logger.error(f"Error downloading/uploading/removing image: {e}")
                                    finally:
                                        if file and os.path.exists(file):
                                            os.remove(file)
                                elif hasattr(message.media, 'document'):
                                    logger.warning(f"Skipped document in message {message.id} from {chat.username}")
                                else:
                                    logger.warning(f"Unknown media type in message {message.id} from {chat.username}")
                            else:
                                logger.warning(f"No media in message {message.id} from {chat.username}")
                        except Exception as e:
                            ERRORS.inc()
                            logger.error(f"Error processing message: {e}")
                    message_data['images'] = images
                    if process_type == 0 or process_type == 2:
                        insert_document("structured_products", message_data)
                    else:
                        update_document_if_not_updated_by_seller("structured_products",
                            {'message_id': message_data['message_id'], 'telegram_channel_id': message_data['telegram_channel_id']},
                            message_data
                        )
                    logger.info(f"Processed message {message.id} from {chat.username} with {len(images)} images.")
                    MESSAGES_PROCESSED.inc()
        PROCESSING_TIME.observe(time.time() - start_time)
        message_queue.task_done()

async def realtimeRunner():
    await client.start()
    asyncio.create_task(refresh_channels_periodically(client))
    asyncio.create_task(image_worker(client))
    await asyncio.sleep(10*60)
    asyncio.create_task(periodic_chat_update_runner())
    await asyncio.sleep(2)
    def is_watched_channel(event):
        chat_id = getattr(event.chat, "id", None)
        if not chat_id:
            return False
        return chat_id in all_channels
    await asyncio.sleep(2)

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
                await message_queue.put([(message, chat, 0)])
                logger.info(f"{message.id} enqueued from {chat.username}")
        except Exception as e:
            ERRORS.inc()
            logger.error(f"Error processing new message: {e}")
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
                await message_queue.put([(message, chat, 1)])
                logger.info(f"{message.id} edited and enqueued from {chat.username}")
        except Exception as e:
            ERRORS.inc()
            logger.error(f"Error processing edited message: {e}")
    await asyncio.sleep(2)

    @client.on(events.MessageDeleted(func=is_watched_channel))
    async def delete_handler(event):
        try:
            deleted_messages = event.deleted_ids
            chat = event.chat
            chat_id = getattr(chat, "id", None)
            logger.info(f"Deleted messages: {deleted_messages} from {chat.username}")
            if chat_id:
                for msg_id in deleted_messages:
                    res = delete_document('structured_products', {"telegram_channel_id": chat_id, "message_id": msg_id})
                    if res:
                        logger.info(f"Deleted message {msg_id} from {chat.username}")
                    else:
                        logger.info(f"Document not in DB")
        except Exception as e:
            ERRORS.inc()
            logger.error(f"Error processing deleted message: {e}")
    await asyncio.sleep(2)
    logger.info("Listening for new messages...")
    await client.run_until_disconnected()
