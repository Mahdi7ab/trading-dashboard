# collector/collector.py

import requests
from config import API_URL, HEADERS
from database import Base, engine, SessionLocal, Fill, TrackedTrader
from sqlalchemy.dialects.postgresql import insert # 🔽 (جدید) برای درج هوشمند

def get_user_fills(user_address):
    """Fetches the transaction history (fills) for a specific user."""
    payload = {"type": "userFills", "user": user_address}
    try:
        response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching fills for user {user_address}: {e}")
        return None

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
            
            # -------------------------------------------------
            # 🔽 (تغییر) خط پاک کردن دیتابیس حذف شد 🔽
            # print("🧹 Clearing old records from the 'fills' table...")
            # session.query(Fill).delete()
            # -------------------------------------------------

            total_inserted_count = 0
            for address in addresses_list:
                print(f"Fetching fills for: {address}")
                fills_data = get_user_fills(address)
                
                if not fills_data:
                    continue

                # -------------------------------------------------
                # 🔽 (جدید) منطق جلوگیری از تکرار 🔽
                # -------------------------------------------------
                
                # ۱. گرفتن هش‌های تمام معاملاتی که از API آمدند
                api_hashes = {fill.get('hash') for fill in fills_data if fill.get('hash')}
                if not api_hashes:
                    continue # اگر هیچ هشی وجود نداشت، رد شو

                # ۲. پیدا کردن کدام یک از این هش‌ها *از قبل* در دیتابیس ما برای این کاربر وجود دارند
                existing_hashes = session.query(Fill.hash).filter(
                    Fill.user_address == address,
                    Fill.hash.in_(api_hashes)
                ).all()
                existing_hashes_set = {h[0] for h in existing_hashes}

                fills_to_insert = []
                for fill in fills_data:
                    fill_hash = fill.get('hash')
                    # ۳. فقط معاملاتی را اضافه کن که هش آن‌ها در دیتابیس *نبود*
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
                # -------------------------------------------------

                if not fills_to_insert:
                    print(f"No new fills for user {address}.")
                    continue
                
                # Add the new records
                session.add_all(fills_to_insert)
                session.commit() # 🔽 (جدید) کامیت در هر حلقه برای جلوگیری از خطای تکرار
                
                total_inserted_count += len(fills_to_insert)
                print(f"✅ Inserted {len(fills_to_insert)} new fill records for user {address}.")

            # -------------------------------------------------
            # (توجه: کامیت نهایی به داخل حلقه منتقل شد)
            # -------------------------------------------------
            print(f"🎉 Successfully inserted a total of {total_inserted_count} new records across all users.")
            
        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}")
            session.rollback()
        finally:
            print("Collector finished run.")

if __name__ == "__main__":
    run_collector()