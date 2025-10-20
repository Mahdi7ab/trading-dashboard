# collector/collector.py

import requests
import time # 🔽 (جدید) ماژول زمان را برای تاخیر اضافه می‌کنیم
# 🔽 (جدید) خطاهای خاص requests را برای مدیریت بهتر وارد می‌کنیم
from requests.exceptions import HTTPError, RequestException
from config import API_URL, HEADERS
from database import Base, engine, SessionLocal, Fill, TrackedTrader
from sqlalchemy.dialects.postgresql import insert

# -------------------------------------------------
# 🔽 (بازنویسی شده) تابع دریافت اطلاعات با مکانیزم تلاش مجدد 🔽
# -------------------------------------------------
def get_user_fills(user_address):
    """
    اطلاعات معاملات کاربر را با مکانیزم تلاش مجدد برای خطای 429 (Rate Limit) دریافت می‌کند.
    """
    payload = {"type": "userFills", "user": user_address}
    
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 5 # 5 ثانیه صبر پس از خطای 429
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
            response.raise_for_status() # این دستور برای خطاهای 4xx/5xx خطا صادر می‌کند
            return response.json()
        
        except HTTPError as e:
            # بررسی دقیق خطای 429 (Too Many Requests)
            if e.response.status_code == 429:
                if attempt < MAX_RETRIES - 1:
                    print(f"⚠️ Rate limit hit for {user_address}. Retrying in {RETRY_DELAY_SECONDS}s... (Attempt {attempt + 1}/{MAX_RETRIES})")
                    time.sleep(RETRY_DELAY_SECONDS) # صبر کردن قبل از تلاش مجدد
                else:
                    print(f"❌ Rate limit failed for {user_address} after {MAX_RETRIES} attempts. Skipping.")
                    return None # پس از 3 بار تلاش، از این کاربر صرف نظر کن
            else:
                # یک خطای HTTP دیگر رخ داده (مثل 404, 500)
                print(f"❌ HTTP Error fetching fills for {user_address}: {e}")
                return None
        
        except RequestException as e:
            # خطای کلی‌تر (مثل قطع اتصال، تایم‌اوت)
            print(f"❌ Request Error fetching fills for {user_address}: {e}")
            return None # در این موارد تلاش مجدد نکن

    return None # اگر حلقه تمام شد و موفقیتی نبود


def run_collector():
    print("🚀 Collector started... (Append-Only Mode)")
    Base.metadata.create_all(bind=engine)
    
    with SessionLocal() as session:
        try:
            print("Fetching trader addresses from 'tracked_traders' table...")
            traders_to_track = session.query(TrackedTrader).all()
            
            if not traders_to_track:
                print("🤷 No traders found in 'tracked_traders' table. Did you run discover_traders.py first?")
                return

            addresses_list = [trader.user_address for trader in traders_to_track]
            print(f"✅ Found {len(addresses_list)} traders to collect data for.")
            
            total_inserted_count = 0
            
            # 🔽 (جدید) استفاده از enumerate برای شماره‌گذاری
            for i, address in enumerate(addresses_list):
                # 🔽 (جدید) اضافه کردن لاگ برای ردیابی پیشرفت
                print(f"[{i+1}/{len(addresses_list)}] Fetching fills for: {address}")
                fills_data = get_user_fills(address)
                
                if not fills_data:
                    # get_user_fills خودش دلیل خطا را پرینت می‌کند
                    continue

                api_hashes = {fill.get('hash') for fill in fills_data if fill.get('hash')}
                if not api_hashes:
                    continue 

                existing_hashes = session.query(Fill.hash).filter(
                    Fill.user_address == address,
                    Fill.hash.in_(api_hashes)
                ).all()
                existing_hashes_set = {h[0] for h in existing_hashes}

                fills_to_insert = []
                for fill in fills_data:
                    fill_hash = fill.get('hash')
                    if fill_hash not in existing_hashes_set:
                        pnl_str = fill.get('closedPnl')
                        direction_str = fill.get('dir', '')
                        
                        new_fill = Fill(
                            hash=fill.get('hash'),
                            oid=fill.get('oid'),
                            user_address=address,
                            asset=fill.get('coin'),
                            price=float(fill.get('px')),
                            size=float(fill.get('sz')),
                            direction=direction_str,
                            is_buy="Open Long" in direction_str or "Close Short" in direction_str,
                            pnl=float(pnl_str) if pnl_str else None,
                            timestamp=int(fill.get('time'))
                        )
                        fills_to_insert.append(new_fill)

                if not fills_to_insert:
                    print(f"No new fills for user {address}.")
                    continue
                
                session.add_all(fills_to_insert)
                session.commit() 
                
                total_inserted_count += len(fills_to_insert)
                print(f"✅ Inserted {len(fills_to_insert)} new fill records for user {address}.")

                # -------------------------------------------------
                # 🔽 (جدید) تاخیر پیشگیرانه 🔽
                # -------------------------------------------------
                # 0.5 ثانیه مکث بین هر درخواست برای جلوگیری از فشار به API
                time.sleep(0.5) 
                # -------------------------------------------------


            print(f"🎉 Successfully inserted a total of {total_inserted_count} new records across all users.")
            
        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}")
            session.rollback()
        finally:
            print("Collector finished run.")

if __name__ == "__main__":
    run_collector()