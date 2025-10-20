# collector/send_to_telegram.py

import os
import sys
import asyncio
import telegram
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.request import HTTPXRequest

# خواندن اطلاعات از متغیرهای محیطی
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
PROXY_URL = os.environ.get("PROXY_URL")

async def send_message(message_text):
    """
    یک پیام متنی را با پشتیبانی از پروکسی به تلگرام ارسال می‌کند.
    """
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ Error: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID is not set.")
        return

    if not message_text:
        print("No message text provided.")
        return

    try:
        request_instance = None
        if PROXY_URL:
            print(f"Connecting via proxy: {PROXY_URL}")
            proxy_settings = {'http://': PROXY_URL, 'https://': PROXY_URL}
            request_instance = HTTPXRequest(proxy_settings=proxy_settings)
        else:
            print("Connecting directly (no proxy configured).")

        bot = Bot(token=BOT_TOKEN, request=request_instance)
        
        print(f"Sending message to chat_id: {CHAT_ID}")
        try:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=message_text,
                parse_mode=ParseMode.MARKDOWN_V2, # استفاده از MarkdownV2 برای فرمت‌بندی
                disable_web_page_preview=True
            )
        except TelegramError as e:
            print(f"❌ Error sending message: {e}")

    except Exception as e:
        print(f"❌ An unexpected error occurred in send_message: {e}")

if __name__ == "__main__":
    # تمام آرگومان‌های ورودی را به هم می‌چسباند تا یک پیام واحد بسازد
    # این به ما اجازه می‌دهد پیام‌های چند خطی ارسال کنیم
    message = " ".join(sys.argv[1:])
    
    if message:
        asyncio.run(send_message(message))
    else:
        print("No message text provided to send.")