from pyrogram import Client
from ingestion.constants import CHANNEL_IDS

def fetch_messages():
    with Client("telegram_client", api_id="", api_hash="") as app:
        messages = []
        for channel_id in CHANNEL_IDS:
            for message in app.get_chat_history(channel, limit=100):
                messages.append(message)
        return messages
    

def filter_messages(messages):
    filtered = [] # todo
    return filtered

