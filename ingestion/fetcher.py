import asyncio
import json
from telethon import TelegramClient
from ingestion.constants import CHANNEL_IDS
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.custom.message import Message
from storage.store import store_raw_data


api_id = "21879721"
api_hash = "cadd93c819128f73ba3439a0f430e677"



BATCH_SIZE = 10

def fetch_messages():
    print('Loading Client')
    client = TelegramClient('telegram_client', api_id, api_hash)
    messages = []

    async def join_channel(channel):
        await client(JoinChannelRequest(channel))

    async def get_messages():
        last_fetched = 0
        await asyncio.sleep(2)
        await client.start()
        print('Fetching messages')
        for channel_id in CHANNEL_IDS:
            await asyncio.sleep(2)  # Rate limiting
            if last_fetched:
                async for message in client.iter_messages(channel_id, limit=BATCH_SIZE, offset_id=last_fetched):
                    messages.append(message)
                last_fetched = messages[-1].id
            else:
                async for message in client.iter_messages(channel_id, limit=BATCH_SIZE):
                    messages.append(message)
                last_fetched = messages[-1].id

        print('Fetch completed!')
        print(last_fetched)
        await client.disconnect()

    client.loop.run_until_complete(get_messages())

    # Convert messages to JSON
    messages_json = [json.loads(message.to_json()) for message in messages]

    # Store messages in MongoDB
    stored = store_raw_data(messages_json)
    if not stored:
        print('Storage failed !!!!!')
    return messages_json

def filter_messages(messages):
    filtered = [] # todo
    return filtered





# from pyrogram import Client
# from ingestion.constants import CHANNEL_IDS
# from telegram import Bot
# from telegram.ext import Updater


# def fetch_messages():
#     print('Loading Client')
#     with Client("telegram_client", api_id="25653559", api_hash="77ddfa0bb95b50d5b2a313e880fd6ec2") as app:
#         messages = []
#         print('Fetching messages')
#         for channel_id in CHANNEL_IDS:
#             for message in app.get_chat_history(channel_id, limit=100):
#                 messages.append(message)
        
#         print('Fetch completed!')
#         return messages
    

# def filter_messages(messages):
#     filtered = [] # todo
#     return filtered



# def fetch_messages2():
#     print('Loading Bot')
#     bot = Bot(token="YOUR_BOT_TOKEN")
#     updater = Updater(bot=bot, use_context=True)
#     messages = []
#     print('Fetching messages')
    
#     for channel_id in CHANNEL_IDS:
#         updates = bot.get_updates()
#         for update in updates:
#             if update.channel_post and update.channel_post.chat.id == channel_id:
#                 messages.append(update.channel_post)
    
#     print('Fetch completed!')
#     return messages