from pyrogram import Client
from ingestion.constants import CHANNEL_IDS

def fetch_messages():
    print('Loading Client')
    with Client("telegram_client", api_id="25653559", api_hash="77ddfa0bb95b50d5b2a313e880fd6ec2") as app:
        messages = []
        print('Fetching messages')
        for channel_id in CHANNEL_IDS:
            for message in app.get_chat_history(channel_id, limit=100):
                messages.append(message)
        
        print('Fetch completed!')
        return messages
    

def filter_messages(messages):
    filtered = [] # todo
    return filtered

