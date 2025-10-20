# collector/telegram_sender.py

import os
import asyncio
import telegram
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.request import HTTPXRequest

# خواندن متغیرهای محیطی
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
PROXY_URL = os.environ.get("PROXY_URL")

def init_bot():
    """
    یک نمونه Bot با تنظیمات پروکسی ایجاد می‌کند.
    """
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ TELEGRAM_BOT_TOKEN or CHAT_ID not set. Skipping Telegram.")
        return None

    request_instance = None
    if PROXY_URL:
        print(f"Connecting to Telegram via proxy: {PROXY_URL}")
        proxy_settings = {'http://': PROXY_URL, 'https://': PROXY_URL}
        request_instance = HTTPXRequest(proxies=proxy_settings)
    else:
        print("Connecting to Telegram directly.")
    
    return Bot(token=BOT_TOKEN, request=request_instance)

async def send_telegram_message(bot_instance, message_text):
    """
    یک پیام متنی را با استفاده از یک نمونه bot موجود ارسال می‌کند.
    """
    if not message_text or not CHAT_ID or not bot_instance:
        return
    
    # فرمت‌بندی MarkdownV2 برای تلگرام
    safe_text = message_text.replace(".", "\.") \
                            .replace("-", "\-") \
                            .replace("(", "\(") \
                            .replace(")", "\)") \
                            .replace("!", "\!") \
                            .replace("+", "\+") \
                            .replace("=", "\=") \
                            .replace("|", "\|") \
                            .replace("{", "\{") \
                            .replace("}", "\}") \
                            .replace("#", "\#") \
                            .replace("$", "\$")
    try:
        await bot_instance.send_message(
            chat_id=CHAT_ID,
            text=safe_text,
            parse_mode=ParseMode.MARKDOWN_V2,
            disable_web_page_preview=True
        )
    except TelegramError as e:
        print(f"❌ Error sending telegram message: {e}")
    except Exception as e:
        print(f"❌ Unexpected error in send_telegram_message: {e}")