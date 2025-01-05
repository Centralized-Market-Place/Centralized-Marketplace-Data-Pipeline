import asyncio
import json
from telethon import TelegramClient
from ingestion.constants import CHANNEL_IDS
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.custom.message import Message
from storage.store import store_raw_data
from collections import defaultdict

api_id = "21879721"
api_hash = "cadd93c819128f73ba3439a0f430e677"



BATCH_SIZE = 10

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

def filter_messages(messages):
    filtered = [] # todo
    return filtered



