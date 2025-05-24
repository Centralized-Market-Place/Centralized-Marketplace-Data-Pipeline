import os
import asyncio
from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from collections import defaultdict

from pymongo import MongoClient
from tqdm import tqdm
# from dotenv import dotenv_values
from datetime import datetime, timezone

MONGO_URI="mongodb+srv://semahegnsahib:sahib@cluster0.vmyk3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME="centeral_marketplace"
# config = dotenv_values('secrets.env')
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DATABASE_NAME]

API_ID = "21879721"
API_HASH = "cadd93c819128f73ba3439a0f430e677"
SESSION_NAME = 'telegram_client'

client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
message_queue = asyncio.Queue()

# all_channels = []  # Will be updated once per day
all_channels = [
    { "username":"@just_for_a_test", "_id": "1234567890"},
    # { "username":"@just_for_a_test2", "_id": "1234567891"},
]

def fetch_all_channels(collection_name='channels'):
    return all_channels
    try:
        collection = db[collection_name]
        channels = list(collection.find())
        return channels
    except Exception as e:
        print(str(e))
    return []

async def is_participant(channel, user) -> bool:
    try:
        await asyncio.sleep(1)  # Avoid hitting rate limits
        await client.get_permissions(channel, user)
        return True
    except errors.UserNotParticipantError:
        return False

'''
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

async def refresh_channels_periodically(interval_hours=24):
    global all_channels
    me = await client.get_me()
    while True:
        all_channels = fetch_all_channels()
        print(f"[INFO] Refreshed all_channels at {datetime.now(timezone.utc)}. {len(all_channels)} channels loaded.")
        # Attempt to join channels if not already a member

        for channel in all_channels:
            username = channel.get("username")
            if not username:
                continue
            try:
                await asyncio.sleep(1)  # Avoid hitting rate limits
                entity = await client.get_entity(username)
                # Try to get participant info to check membership
                if await is_participant(entity, me):
                    print(f"[INFO] Already a member of channel: {username}")
                    continue
                print(f"[INFO] Attempting to join channel: {username}")
                await asyncio.sleep(1)  # Avoid hitting rate limits
                await client(JoinChannelRequest(username))
                print(f"[INFO] Joined channel: {username}")
            except Exception as e:
                print(f"[WARN] Could not join channel {username}: {e}")
        await asyncio.sleep(interval_hours * 3600)



album_buffer = defaultdict(list)
album_timers = {}

ALBUM_TIMEOUT = 2  # seconds to wait for all album parts


# request telegram api with rate limiting
def request_with_rate_limit(func, *args, **kwargs):
    async def wrapper():
        try:
            return await func(*args, **kwargs)
        except errors.FloodWaitError as e:
            print(f"Rate limit hit, waiting for {e.seconds} seconds")
            await asyncio.sleep(e.seconds)
            return await wrapper()  # Retry after waiting
    return asyncio.create_task(wrapper())

async def process_album(grouped_id):
    messages = album_buffer.pop(grouped_id, [])
    # You can sort messages by .id or .date if needed
    print(f"Processing album with grouped_id={grouped_id}, {len(messages)} parts")
    # Example: process all images together
    # await save_album_to_mongo(messages)

def schedule_album_processing(grouped_id):
    if grouped_id in album_timers:
        album_timers[grouped_id].cancel()
    album_timers[grouped_id] = asyncio.get_event_loop().call_later(
        ALBUM_TIMEOUT, lambda: asyncio.create_task(process_album(grouped_id))
    )

async def image_worker(tg_client):
    while True:
        message, chat = await message_queue.get()
        if not message or not chat:
            message_queue.task_done()
            continue
        try:
            if message.media:
                if hasattr(message.media, 'photo'):
                    # Handle photo
                    print(f"✅ Processed new message from {chat.username}: {message.id}")
                    # Save the image to MongoDB or perform any other processing
                    # Example: await save_image_to_mongo(message)
                elif hasattr(message.media, 'document'):
                    # Handle document (could be a photo or other file)
                    print(f"✅ Processed new document from {chat.username}: {message.id}")
                    # Example: await save_document_to_mongo(message)
                else:
                    print(f"⚠️ Unknown media type in message {message.id} from {chat.username}")
            else:
                print(f"⚠️ No media in message {message.id} from {chat.username}")
        except Exception as e:
            print(f"Error processing message: {e}")
        finally:
            message_queue.task_done()


async def main():
    await client.start()  # Start the client before any API requests
    # Start background task to refresh channels daily
    asyncio.create_task(refresh_channels_periodically())
    asyncio.create_task(image_worker(client))
    await asyncio.sleep(2)  # Give time for first fetch
    def get_username_to_mongo_id():
        return {c["username"]: c["_id"] for c in all_channels if "username" in c and "_id" in c}
    def get_channel_usernames():
        return list(get_username_to_mongo_id().keys())

    def is_watched_channel(event):
        # Remove '@' if present for matching
        username = getattr(event.chat, "username", None)
        if not username:
            return False
        usernames = {c["username"].lstrip("@") for c in all_channels if "username" in c}
        return username.lstrip("@") in usernames

    @client.on(events.NewMessage(func=is_watched_channel))
    async def handler(event):
        try:
            message = event.message
            chat = event.chat
            grouped_id = getattr(message, "grouped_id", None)
            if grouped_id:
                album_buffer[grouped_id].append((message, chat))
                schedule_album_processing(grouped_id)
            else:
                await message_queue.put((message, chat))
                print(f"✅ {message.id} enqueued from {chat.username}")
        except Exception as e:
            print(f"Error processing new message: {e}")

    await asyncio.sleep(2)  
    print("Listening for new messages...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())