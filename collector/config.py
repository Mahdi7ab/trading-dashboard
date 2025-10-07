import os

# آدرس API و هدرهای درخواست
API_URL = "https://api.hyperliquid.xyz/info"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}

# اطلاعات اتصال به دیتابیس از متغیرهای محیطی خوانده می‌شود
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://myuser:mysecretpassword@localhost:5432/trading_db")

# لیستی از آدرس‌های تریدرهایی که می‌خواهی پوزیشن‌های باز آن‌ها را دنبال کنی
TOP_TRADERS_ADDRESSES = [
    "0x15b325660a1c4a9582a7d834c31119c0cb9e3a42",
    "0x8af700ba841f30e0a3fcb0ee4c4a9d223e1efa05",
    "0x1d52fe9bde2694f6172192381111a91e24304397",
    "0x56498e5f90c14060499b62b6f459b3e3fb9280c5",
    "0x2ba553d9f990a3b66b03b2dc0d030dfc1c061036"
]