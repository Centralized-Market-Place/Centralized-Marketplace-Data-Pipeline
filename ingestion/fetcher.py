import asyncio
import json
import os
import random
import string
from collections import defaultdict

# import cloudinary
# import cloudinary.uploader

from telethon import TelegramClient
from telethon.tl.functions.channels import JoinChannelRequest
from storage.store import store_raw_data
from ingestion.constants import CHANNEL_IDS

api_id = "21879721"
api_hash = "cadd93c819128f73ba3439a0f430e677"
BATCH_SIZE = 10

# cloudinary.config()


def fetch_messages(factor=10):
    print("Loading Client")
    client = TelegramClient("telegram_client", api_id, api_hash)
    messages = []

    async def join_channel(channel):
        await client(JoinChannelRequest(channel))

    async def get_messages():
        last_fetched = defaultdict(int)
        await asyncio.sleep(2)
        await client.start()
        print("Fetching messages")

        # 1) Fetch messages
        for i in range(1):
            for channel_id in CHANNEL_IDS:
                await asyncio.sleep(2)
                if last_fetched[channel_id]:
                    async for message in client.iter_messages(
                        channel_id, limit=BATCH_SIZE, offset_id=last_fetched[channel_id]
                    ):
                        messages.append(message)
                    if messages:
                        last_fetched[channel_id] = messages[-1].id
                else:
                    async for message in client.iter_messages(
                        channel_id, limit=BATCH_SIZE
                    ):
                        messages.append(message)
                    if messages:
                        last_fetched[channel_id] = messages[-1].id
                await asyncio.sleep(2)
                print(f"fetch {i} from {channel_id} completed.")
            await asyncio.sleep(6)

        print("Fetch completed!")
        print("Last fetched:", last_fetched)

        last_fetched_info = [
            {"channel_id": channel_id, "last_fetched_id": last_fetched[channel_id]}
            for channel_id in last_fetched
        ]
        store_raw_data(last_fetched_info, collection_name="last_fetched_info")
        print("Last fetched info stored.")

        grouped = defaultdict(list)
        for msg in messages:
            group_id = msg.grouped_id or msg.id
            grouped[group_id].append(msg)

        combined_posts = []
        for group_id, msgs_in_group in grouped.items():
            main_msg = msgs_in_group[0]
            main_msg_dict = json.loads(main_msg.to_json())

            main_msg_dict["filenames"] = []

            for m in msgs_in_group:
                if m.media and m.photo:
                    random_name = "".join(
                        random.choices(string.ascii_letters + string.digits, k=16)
                    )
                    filename = f"{random_name}.jpg"
                    local_path = f"media/images/{filename}"

                    downloaded_path = await client.download_media(m, file=local_path)
                    if downloaded_path:
                        main_msg_dict["filenames"].append(filename)
                        # cloudinary.uploader.upload(downloaded_path)
                        # main_msg_dict["cloudinary_urls"].append(cloudinary.uploader.upload(downloaded_path)["url"])
                        # os.remove(downloaded_path)

            combined_posts.append(main_msg_dict)

        stored = store_raw_data(combined_posts, collection_name="raw_data")
        if not stored:
            print("Storage failed !!!!!")

        await client.disconnect()

    client.loop.run_until_complete(get_messages())


def filter_messages(messages):
    filtered = []  # todo
    return filtered
