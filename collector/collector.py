# collector/collector.py

import requests
# 🔽 (تغییر ۱) دیگر لیست آدرس‌ها را از config وارد نمی‌کنیم
from config import API_URL, HEADERS
# 🔽 (تغییر ۲) مدل TrackedTrader را برای خواندن آدرس‌ها وارد می‌کنیم
from database import Base, engine, SessionLocal, Fill, TrackedTrader

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
    print("🚀 Collector started... Will clear the DB before each run.")
    Base.metadata.create_all(bind=engine)
    
    with SessionLocal() as session:
        try:
            # -------------------------------------------------
            # 🔽 (تغییر ۳) خواندن لیست آدرس‌ها از دیتابیس 🔽
            # -------------------------------------------------
            print("Fetching trader addresses from 'tracked_traders' table...")
            traders_to_track = session.query(TrackedTrader).all()
            
            if not traders_to_track:
                print("🤷 No traders found in 'tracked_traders' table. Did you run discover_traders.py first?")
                return

            # استخراج آدرس‌ها از آبجکت‌ها
            addresses_list = [trader.user_address for trader in traders_to_track]
            print(f"✅ Found {len(addresses_list)} traders to collect data for.")
            # -------------------------------------------------
            
            # --- KEY CHANGE ---
            # Delete all existing records from the 'fills' table first.
            print("🧹 Clearing old records from the 'fills' table...")
            session.query(Fill).delete()
            # ------------------

            total_inserted_count = 0
            
            # 🔽 (تغییر ۴) حلقه‌ی for حالا از لیست آدرس‌های دیتابیس استفاده می‌کند
            for address in addresses_list:
                print(f"Fetching fills for: {address}")
                fills_data = get_user_fills(address)
                
                if not fills_data:
                    continue

                fills_to_insert = []
                for fill in fills_data:
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
                    continue
                
                # Add the new records
                session.add_all(fills_to_insert)
                
                total_inserted_count += len(fills_to_insert)
                print(f"✅ Prepared {len(fills_to_insert)} new fill records for user {address}.")

            # Commit the transaction (deletes and inserts)
            session.commit()
            print(f"🎉 Successfully cleared and saved a total of {total_inserted_count} new records.")
            
        except Exception as e:
            print(f"❌ An unexpected error occurred: {e}")
            session.rollback()
        finally:
            print("Collector finished run.")

if __name__ == "__main__":
    run_collector()