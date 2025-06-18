import os
from dotenv import load_dotenv
from telegram import Bot
import asyncio
load_dotenv()


TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_CHANNEL_ID = os.getenv("ADMIN_CHANNEL_ID") 




async def issue_handler(issue):
    """Send an issue report to the admin channel."""
    if not TELEGRAM_BOT_TOKEN or not ADMIN_CHANNEL_ID:
        print("Telegram bot token or admin channel ID is not set.")
        return

    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    
    try:
        message = f"New issue reported:\n\n{issue}"
        await bot.send_message(chat_id=ADMIN_CHANNEL_ID, text=message)
        print("Issue reported successfully.")
    except Exception as e:
        print(f"Failed to send issue report: {str(e)}")


def handle_issue_sync(issue):
    try:
        """Synchronous wrapper for issue_handler."""
        loop = asyncio.get_event_loop()
        loop.run_until_complete(issue_handler(issue))
        loop.close()
    except Exception as e:
        print(f"Error in handle_issue_sync: {str(e)}")
        
