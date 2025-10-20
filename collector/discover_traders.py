# collector/discover_traders.py

import requests
from config import LEADERBOARD_API_URL, HEADERS
from database import Base, engine, SessionLocal, TrackedTrader
from sqlalchemy.exc import IntegrityError

# -------------------------------------------------
# 🔽 (جدید) متغیر برای تنظیم تعداد تریدرها 🔽
# -------------------------------------------------
# ⭐️ می‌توانید این عدد را به دلخواه تغییر دهید (مثلاً 50، 100 یا 200)
MAX_TRADERS_TO_TRACK = 5
# -------------------------------------------------


def get_all_time_pnl(performances):
    """
    از لیست 'windowPerformances'، سود 'allTime' را استخراج می‌کند.
    """
    try:
        for window in performances:
            if window[0] == "allTime":
                return float(window[1].get('pnl'))
    except (IndexError, ValueError, TypeError, AttributeError):
        return None
    return None

def fetch_leaderboard():
    """از API لیدربورد جدید (stats-data) داده‌ها را فراخوانی می‌کند."""
    print(f"🚀 Fetching leaderboard from: {LEADERBOARD_API_URL}")
    try:
        response = requests.get(LEADERBOARD_API_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        return response.json().get('leaderboardRows', [])
    
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching leaderboard: {e}")
        return None
    except Exception as e:
        print(f"❌ Error parsing leaderboard JSON: {e}")
        return None

def update_tracked_traders():
    """
    جدول tracked_traders را با تریدرهای سودده جدید به‌روز می‌کند.
    """
    leaderboard_data = fetch_leaderboard()
    
    if not leaderboard_data:
        print("No leaderboard data fetched. Exiting.")
        return

    new_traders_list = []
    
    for trader_data in leaderboard_data:
        try:
            address = trader_data.get('ethAddress')
            performances = trader_data.get('windowPerformances', [])
            pnl_value = get_all_time_pnl(performances)

            # (نکته) اگر خواستید بر اساس حداقل سود فیلتر کنید،
            # می‌توانید این شرط را تغییر دهید:
            # if address and pnl_value is not None and pnl_value > 10000:
            
            if address and pnl_value is not None and pnl_value > 0:
                new_traders_list.append(
                    TrackedTrader(user_address=address, pnl=pnl_value)
                )
        except Exception:
            continue
    
    if not new_traders_list:
        print("🤷 No profitable traders found in the new data.")
        return

    print(f"✅ Found {len(new_traders_list)} total profitable traders.")

    # -------------------------------------------------
    # 🔽 (جدید) فیلتر کردن و مرتب‌سازی لیست 🔽
    # -------------------------------------------------
    # 1. مرتب‌سازی لیست بر اساس PNL (سود) به صورت نزولی (از زیاد به کم)
    sorted_traders = sorted(new_traders_list, key=lambda trader: trader.pnl, reverse=True)
    
    # 2. برش لیست به تعداد MAX_TRADERS_TO_TRACK
    filtered_traders_list = sorted_traders[:MAX_TRADERS_TO_TRACK]
    
    print(f"Filtering down to top {len(filtered_traders_list)} traders (based on PNL).")
    # -------------------------------------------------

    # اطمینان از ساخته شدن جدول
    Base.metadata.create_all(bind=engine)
    
    with SessionLocal() as session:
        try:
            # 1. پاک کردن لیست قدیمی
            print("🧹 Clearing old tracked traders...")
            session.query(TrackedTrader).delete()
            
            # 2. اضافه کردن لیست جدید (فیلتر شده)
            print(f"✨ Adding {len(filtered_traders_list)} new top traders to the database...")
            # 🔽 (تغییر) از لیست فیلتر شده استفاده می‌کنیم
            session.add_all(filtered_traders_list)
            
            # 3. ذخیره تغییرات
            session.commit()
            print(f"🎉 Successfully updated tracked_traders table.")
            
        except IntegrityError:
            print("❌ Database integrity error (e.g., duplicate address). Rolling back.")
            session.rollback()
        except Exception as e:
            print(f"❌ An unexpected database error occurred: {e}")
            session.rollback()

if __name__ == "__main__":
    update_tracked_traders()