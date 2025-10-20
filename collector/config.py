import os

# 🔽 این خط جدید را اضافه کنید 🔽
LEADERBOARD_API_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"

# آدرس API و هدرهای درخواست
API_URL = "https://api.hyperliquid.xyz/info"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}

# اطلاعات اتصال به دیتابیس از متغیرهای محیطی خوانده می‌شود
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://myuser:mysecretpassword@localhost:5432/trading_db")

# لیستی از آدرس‌های تریدرهایی که می‌خواهی پوزیشن‌های باز آن‌ها را دنبال کنی
# 🔴 ما دیگر به این لیست ثابت نیاز نداریم، اما فعلاً بگذارید بماند
TOP_TRADERS_ADDRESSES = [
    "0x15b325660a1c4a9582a7d834c31119c0cb9e3a42",
    "0x8af700ba841f30e0a3fcb0ee4c4a9d223e1efa05",
    "0x1d52fe9bde2694f6172192381111a91e24304397",
    "0x56498e5f90c14060499b62b6f459b3e3fb9280c5",
    "0x2ba553d9f990a3b66b03b2dc0d030dfc1c061036",
    "0x2ea18c23f72a4b6172c55b411823cdc5335923f4",
    "0xe4178ba889ac1c931861533b98bb86cb9d4858c7",
    "0x044d0932b02f5045bc00e0a6818b7f98ef504681",
    "0x020ca66c30bec2c4fe3861a94e4db4a498a35872",
    "0x8e096995c3e4a3f0bc5b3ea1cba94de2aa4d70c9"
]