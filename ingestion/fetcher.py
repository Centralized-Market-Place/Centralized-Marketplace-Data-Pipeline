import asyncio
import json
from telethon import TelegramClient
from ingestion.constants import CHANNEL_IDS
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.custom.message import Message
from telethon.tl.types import PeerChannel
from storage.store import store_raw_data, fetch_stored_messages, store_products
from collections import defaultdict
import cloudinary
import cloudinary.uploader
from cloudinary.utils import cloudinary_url
import tqdm
import os

api_id = "21879721"
api_hash = "cadd93c819128f73ba3439a0f430e677"

BATCH_SIZE = 10

# Configuration       
cloudinary.config( 
    cloud_name = "dkgm7qlfb", 
    api_key = "429625611193934", 
    api_secret = "eqxt9DA9_gbPr-j1Iiz3wYQo7EI",
    secure=True
)
# CLOUDINARY_URL=cloudinary://429625611193934:eqxt9DA9_gbPr-j1Iiz3wYQo7EI@dkgm7qlfb

def fetch_messages(factor=10):
    print('Loading Client')
    client = TelegramClient('telegram_client', api_id, api_hash)
    messages = []

    async def join_channel(channel):
        await client(JoinChannelRequest(channel))

    async def get_messages():
        last_fetched = defaultdict(int)
        await asyncio.sleep(2)
        await client.start()
        print('Fetching messages')
        for i in range(factor):
            for channel_id in CHANNEL_IDS:
                await asyncio.sleep(2)  # Rate limiting
                if last_fetched[channel_id]:
                    async for message in client.iter_messages(channel_id, limit=BATCH_SIZE, offset_id=last_fetched[channel_id]):
                        messages.append(message)
                    if messages:
                        last_fetched[channel_id] = messages[-1].id
                else:
                    async for message in client.iter_messages(channel_id, limit=BATCH_SIZE):
                        messages.append(message)
                    if messages:
                        last_fetched[channel_id] = messages[-1].id
                await asyncio.sleep(2)
                print(f"fetch {i} from {channel_id} completed.")
            await asyncio.sleep(6)

        print('Fetch completed!')
        print(last_fetched)
        # Store last fetched info in database
        last_fetched_info = [{"channel_id": channel_id, "last_fetched_id": last_fetched[channel_id]} for channel_id in last_fetched]
        store_raw_data(last_fetched_info, collection_name="last_fetched_info")
        print("Last fetched info stored.")
        await client.disconnect()

    client.loop.run_until_complete(get_messages())

    # Convert messages to JSON
    print('Decoding messages into JSON...')
    messages_json = [json.loads(message.to_json()) for message in messages]
    print('Messages decoded!')

    # Store messages in MongoDB
    print('Writing messages to database...')
    stored = store_raw_data(messages_json, collection_name="raw_data")
    if not stored:
        print('Storage failed !!!!!')

    return messages_json


def fetch_from_new_channels(factor=30):

    print('Loading Client')
    client = TelegramClient('telegram_client', api_id, api_hash)
    messages = []
    images = defaultdict(list)

    async def join_channel(channel):
        await client(JoinChannelRequest(channel))

    async def download_images():
        grouped_messages = defaultdict(list)
        
        for message in tqdm.tqdm(messages, desc="Downloading images"):
            uploaded_urls = []
            await asyncio.sleep(2)  # Rate limit
            
            if message.grouped_id:  # Grouped media (album)
                grouped_messages[message.grouped_id].append(message)

            elif message.media and hasattr(message.media, 'photo'):  # Single photo
                file = await client.download_media(message.media)
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
        for grouped_id, msgs in grouped_messages.items():
            for grouped_msg in msgs:
                if grouped_msg.media and hasattr(grouped_msg.media, 'photo'):
                    await asyncio.sleep(2)  # Rate limit
                    file = await client.download_media(grouped_msg.media)
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

            print(f"Grouped messages with grouped_id {grouped_id} processed.")
    
            if uploaded_urls:
                print(f"Message ID {message.id} - Uploaded {len(uploaded_urls)} images: {uploaded_urls}")

    async def fetch_and_add_image():
        messages = fetch_stored_messages(collection_name="products")
        images = defaultdict(list)
        filtered = []
        for message in messages:
            if message.get('images', []):
                continue
            filtered.append(message)
        messages = filtered

        for message in tqdm.tqdm(messages, desc="Inserting images for existing products"):
            message_id = message.get('message_id')
            """
            _id
            6783aa132f531bf8d7cbc73a
            message_id
            1856
            channel_id
            1293830821
            created_at
            "2025-02-02 19:08:01.295080"
            description
            "🔥Sale🔥
            SAMSUNG Galaxy S21 Ultra 5G Single Sim 256GB 12GB RAM 
            ✅Color…"
            forwards
            6
            posted_at
            "2024-12-21T14:56:29+00:00"

            reactions
            Array (2)
            updated_at
            "2025-02-02 19:08:01.295080"
            views
            2365
            """

            await asyncio.sleep(1)
            try:
                raw_message = await client.get_messages(message['channel_id'], ids=message_id)
            except ValueError as e:
                print(f"Error fetching entity for message ID {message_id}: {e}")
                continue
            
            await asyncio.sleep(1)
            try:
                if raw_message.media and hasattr(raw_message.media, 'photo'):
                    file = await client.download_media(raw_message.media)
                    if file:
                        response = cloudinary.uploader.upload(file)
                        if response:
                            message['images'] = [response['url']]
                            stored = store_products([message])
                        else:
                            print(f"Failed to upload image for message ID {message_id}")
                        os.remove(file)
            except Exception as e:
                print(f"Error processing message ID {message_id}: {e}")
                print(f"message: {message.get('description')}")
                continue
        


        



    async def get_messages():
        last_fetched = defaultdict(int)
        skip = defaultdict(bool)
        await asyncio.sleep(2)
        await client.start()
        await fetch_and_add_image()
        # print('Fetching messages')
        # for i in range(factor):
        #     for channel_id in CHANNEL_IDS:
        #         if skip[channel_id]:
        #             continue
        #         await asyncio.sleep(2)  # Rate limiting

        #         if last_fetched[channel_id]:
        #             fetched_messages = []
        #             async for message in client.iter_messages(channel_id, limit=BATCH_SIZE, offset_id=last_fetched[channel_id]):
        #                 fetched_messages.append(message)
        #             if fetched_messages:
        #                 messages.extend(fetched_messages)
        #                 last_fetched[channel_id] = fetched_messages[-1].id
        #             else:
        #                 skip[channel_id] = True
        #         else:
        #             fetched_messages = []
        #             async for message in client.iter_messages(channel_id, limit=BATCH_SIZE):
        #                 fetched_messages.append(message)
        #             if fetched_messages:
        #                 messages.extend(fetched_messages)
        #                 last_fetched[channel_id] = fetched_messages[-1].id
        #             else:

        #                 skip[channel_id] = True
        #     await asyncio.sleep(2)
        #     print(f"fetch {i} completed.")
        # print('Fetch completed!')
        # print(len(messages))

        # print("Setting up media fetch...")
        
        # await download_images()

        await client.disconnect()

    
    client.loop.run_until_complete(get_messages())
    return 



    for key, value in images.items():
        print(f"Channel ID: {key[0]}, Message ID: {key[1]} - {len(value)} images uploaded")

    # Convert messages to JSON
    print('Decoding messages into JSON...')
    # messages_json = [json.loads(message.to_json()) for message in messages]
    messages_json = []
    for message in tqdm(messages, desc="Decoding messages"):
        # skip if message has no text
        if not message.message:
            continue

        message_dict = json.loads(message.to_json())
        channel_id = message_dict.get('peer_id', {}).get('channel_id')
        message_id = message_dict.get('id')
        if message_dict.get('grouped_id'):
            message_dict['images'] = images[(channel_id, message_dict['grouped_id'])]
            messages_json.append(message_dict)
            continue
        message_dict['images'] = images[(channel_id, message_id)]
        messages_json.append(message_dict)
    
    # Store messages in MongoDB
    print('Writing messages to database...')
    stored = store_raw_data(messages_json, collection_name="raw_data_with_images")
    if not stored:
        print('Storage failed !!!!!')
    
    return messages_json


def fetch_new_messages():
    print('Loading Client')
    client = TelegramClient('telegram_client', api_id, api_hash)
    messages = []

    async def join_channel(channel):
        await client(JoinChannelRequest(channel))

    async def get_messages():
        await asyncio.sleep(2)
        await client.start()
        print('Fetching messages')
        for channel_id in CHANNEL_IDS:
            async for message in client.iter_messages(channel_id, limit=10):
                messages.append(message)
        print('Fetch completed!')
        await client.disconnect()

    client.loop.run_until_complete(get_messages())

    # Convert messages to JSON
    print('Decoding messages into JSON...')
    messages_json = [json.loads(message.to_json()) for message in messages]
    print('Messages decoded!')

    # Store messages in MongoDB
    print('Writing messages to database...')
    stored = store_raw_data(messages_json, collection_name="raw_data")
    if not stored:
        print('Storage failed !!!!!')

    return messages_json


def fetch_with_offset_id(offset_id, limit, factor=10):
    print('Loading Client')
    client = TelegramClient('telegram_client', api_id, api_hash)
    messages = []

    async def join_channel(channel):
        await client(JoinChannelRequest(channel))

    async def get_messages():
        await asyncio.sleep(2)
        await client.start()
        for channel_id in CHANNEL_IDS:
            print(f'Fetching messages with offset id from channel {channel_id}')
            async for message in client.iter_messages(channel_id, limit=limit, offset_id=offset_id):
                messages.append(message)
        print(f'Fetch from {channel_id} with offset id completed!')
        await client.disconnect()

    client.loop.run_until_complete(get_messages())

    # Convert messages to JSON
    print(f'Decoding messages from {channel_id} into JSON...')
    messages_json = [json.loads(message.to_json()) for message in messages]
    print('Messages decoded!')

    # Store messages in MongoDB
    print(f'Writing messages from {channel_id} to database...')
    stored = store_raw_data(messages_json, collection_name="raw_data")
    if not stored:
        print(f'Channel ID {channel_id} Storage failed !!!!!')
    else:
        print('Storage success!')

    return messages_json



def fetch_with_offset_date(channel_id, offset_date, limit):
    print('Loading Client')
    client = TelegramClient('telegram_client', api_id, api_hash)
    messages = []

    async def join_channel(channel):
        await client(JoinChannelRequest(channel))

    async def get_messages():
        await asyncio.sleep(2)
        await client.start()
        print(f'Fetching messages with offset date from channel {channel_id}')
        async for message in client.iter_messages(channel_id, limit=limit, offset_date=offset_date):
            messages.append(message)
        print(f'Fetch from {channel_id} with offset date completed!')
        await client.disconnect()

    client.loop.run_until_complete(get_messages())

    # Convert messages to JSON
    print(f'Decoding messages from {channel_id} into JSON...')
    messages_json = [json.loads(message.to_json()) for message in messages]
    print('Messages decoded!')

    # Store messages in MongoDB
    print(f'Writing messages from {channel_id} to database...')
    stored = store_raw_data(messages_json, collection_name="raw_data")
    if not stored:
        print(f'Channel ID {channel_id} Storage failed !!!!!')
    else:
        print('Storage success!')

    return messages_json



def filter_messages(messages):
    filtered = [] # todo
    return filtered



