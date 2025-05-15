import asyncio
from tqdm import tqdm
from telethon.sync import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from pymongo import MongoClient

MONGO_URI="mongodb+srv://semahegnsahib:sahib@cluster0.vmyk3.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DATABASE_NAME="centeral_marketplace"
# config = dotenv_values('secrets.env')
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[DATABASE_NAME]

async def get_channel_about_and_participants_count(channel, tg_client):
    await asyncio.sleep(2)
    full_channel  = await tg_client(GetFullChannelRequest(channel))
    return full_channel.full_chat.about, full_channel.full_chat.participants_count

async def fetch_channel_info(channel_username, channel_pool_id, tg_client):
    """Fetch channel info with thumbnail URL."""
    try:
        await asyncio.sleep(2)  # Rate limiting
        channel = await tg_client.get_entity(channel_username)
        
        about, participants_count = await get_channel_about_and_participants_count(channel, tg_client)
        channel_info = {
            "telegram_id": channel.id,
            "title": channel.title,
            "username": channel.username,
            "pool_entry_id": channel_pool_id,
            "description": about,
            "participants": participants_count,
            "date_created": channel.date.isoformat(),
            "verified": channel.verified,
            "restricted": channel.restricted,
            "scam": channel.scam,
            "has_link": channel.has_link,
            "has_geo": channel.has_geo,
            "photo_id": channel.photo.photo_id if channel.photo else None,
        }
        return channel_info
    except Exception as e:
        print(f"Error fetching channel info for {channel_username}: {str(e)}")
        return None

async def fetch_bulk_channel_info(channel_entries, tg_client):
    """channel_entries: List[Dict] with keys `username` and `id`."""
    channel_infos = []
    for entry in tqdm(channel_entries, desc="Fetching channel infos"):
        info = await fetch_channel_info(entry["channel"], entry["_id"], tg_client)
        if info:
            channel_infos.append(info)
    return channel_infos

async def fetch_all_channels_runner(client):
    await client.start()
    
    # Fetch from pool (DB or wherever you store channel usernames)
    channel_entries = get_channel_pool_entries()  # [{'username': 'xyz', 'id': 'abc'}, ...]
    print(len(channel_entries))
    # print(channel_entries)
    # Fetch from Telegram and store
    infos = await fetch_bulk_channel_info(channel_entries, client)
    for info in tqdm(infos, desc="Updating in DB..."):
        update_or_insert_channel(info)  # You define this to insert or update the DB
    
    await client.disconnect()




def get_channel_pool_entries():
    return list(db.channels_pool.find({}))

def update_or_insert_channel(channel_info):
    db.channels.update_one(
        {"telegram_id": channel_info["telegram_id"]},
        {"$set": channel_info},
        upsert=True
    )
API_ID = "21879721"
API_HASH = "cadd93c819128f73ba3439a0f430e677"
SESSION_NAME = 'telegram_client'
# if __name__ == "__main__":
#     client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
#     asyncio.run(fetch_all_channels_runner(client))
