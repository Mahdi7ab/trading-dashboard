# collector/telegram_sender.py

import os
import asyncio
import telegram
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError

# 🔽 (جدید) ماژول Application.builder را وارد می‌کنیم
from telegram.ext import Application 

# (دیگر به HTTPXRequest نیازی نداریم)
# from telegram.request import HTTPXRequest 

# خواندن متغیرهای محیطی
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
PROXY_URL = os.environ.get("PROXY_URL")

def init_bot():
    """
    یک نمونه Bot با تنظیمات پروکسی با استفاده از Application.builder ایجاد می‌کند.
    """
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ TELEGRAM_BOT_TOKEN or CHAT_ID not set. Skipping Telegram.")
        return None

    try:
        # -------------------------------------------------
        # 🔽 (فیکس نهایی) استفاده از Application.builder 🔽
        # -------------------------------------------------
        
        # ۱. ساختن builder
        builder = Application.builder().token(BOT_TOKEN)

        if PROXY_URL:
            print(f"Connecting to Telegram via proxy: {PROXY_URL}")
            # ۲. تنظیم پروکسی با متد مخصوص
            builder.proxy_url(PROXY_URL)
            # (این مورد برای اطمینان است، گرچه ما get_updates را صدا نمی‌زنیم)
            builder.get_updates_proxy_url(PROXY_URL)
        else:
            print("Connecting to Telegram directly.")
        
        # ۳. ساخت اپلیکیشن
        # (build() فقط آبجکت را می‌سازد، آن را اجرا نمی‌کند)
        application = builder.build()
        
        # ۴. برگرداندن خود Bot از داخل اپلیکیشن
        # application.bot یک نمونه از Bot است که به درستی با پروکسی پیکربندی شده
        return application.bot
        # -------------------------------------------------

    except Exception as e:
        print(f"❌ An unexpected error occurred in init_bot: {e}")
        return None


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