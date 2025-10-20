# collector/analyzer.py

import time
import os
import asyncio
from datetime import datetime
from collections import defaultdict
from sqlalchemy import func

# وارد کردن توابع از ماژول‌های جدا شده
from database import SessionLocal, Fill, TrackedTrader
from analysis_logic import (
    get_open_positions, 
    aggregate_sentiment, 
    get_market_context
)
from reporting import (
    print_sentiment_table, 
    OUTPUT_DIR
)
from telegram_sender import (
    init_bot, 
    send_telegram_message
)

# -------------------------------------------------
# تحلیل‌گرهای تصویری (خلاصه)
# -------------------------------------------------
def analyze_recent_activity(timestamp_str, theme='light'):
    session = SessionLocal()
    try:
        cutoff_ms = (time.time() - 86400) * 1000 # 24h
        recent_fills_query = session.query(Fill).filter(Fill.timestamp >= cutoff_ms)
        positions = get_open_positions(session, fills_query=recent_fills_query)
        if not positions:
            print("No recent (24h) fills found.")
            return
        sentiment = aggregate_sentiment(positions, weights_map=None)
        print_sentiment_table(sentiment, "📈 سطح ۲: سنتیمنت فعالیت اخیر (۲۴ ساعت گذشته)",
                              base_filename="sentiment_recent_24h", timestamp_str=timestamp_str, theme=theme)
    finally:
        session.close()

def analyze_weighted_sentiment(timestamp_str, theme='light'):
    session = SessionLocal()
    try:
        positions = get_open_positions(session)
        if not positions:
            return
        traders = session.query(TrackedTrader).all()
        weights_map = {t.user_address: t.pnl for t in traders if t.pnl and t.pnl > 0}
        if not weights_map:
            print("No trader PNL data found. Skipping weighted sentiment.")
            return
        sentiment = aggregate_sentiment(positions, weights_map=weights_map)
        print_sentiment_table(sentiment, "👑 سطح ۳: سنتیمنت وزن‌دهی شده (بر اساس PNL)",
                              base_filename="sentiment_weighted_pnl", timestamp_str=timestamp_str, theme=theme)
    finally:
        session.close()

# -------------------------------------------------
# تحلیل‌گرهای متنی (سیگنال‌های فوری)
# -------------------------------------------------
async def track_new_trades(bot, timestamp_str, theme='light'):
    session = SessionLocal()
    try:
        minutes_ago = 10
        cutoff_ms = (time.time() - (minutes_ago * 60)) * 1000
        new_trades_query = session.query(Fill).filter(
            Fill.timestamp >= cutoff_ms,
            Fill.direction.like('Open %')
        ).order_by(Fill.timestamp.desc())
        new_trades = new_trades_query.all()
        
        if not new_trades:
            print(f"\n--- 🛰️ No new trades found in the last {minutes_ago} minutes ---")
            return

        print(f"\n--- 🛰️ New Trades Opened in Last {minutes_ago} Minutes ---")
        
        for trade in new_trades:
            trade_time = datetime.fromtimestamp(trade.timestamp / 1000).strftime('%H:%M')
            position_value = trade.size * trade.price
            direction_emoji = "🟢" if "Long" in trade.direction else "🔴"
            
            message = (
                f"{direction_emoji} *New Trade Signal* {direction_emoji}\n"
                f"*Asset:* `{trade.asset}`\n"
                f"*Direction:* `{trade.direction}`\n"
                f"*Price:* `${trade.price:,.2f}`\n"
                f"*Value:* `${position_value:,.2f}`\n"
                f"*Time:* `{trade_time} (UTC)`\n"
                f"*Source:* `{trade.user_address}`"
            )
            
            print(f"Sending signal to Telegram: {trade.direction} {trade.asset}")
            await send_telegram_message(bot, message)
            await asyncio.sleep(0.5)
            
    finally:
        session.close()

async def analyze_trade_consensus(bot, timestamp_str, theme='light'):
    session = SessionLocal()
    try:
        print(f"\n--- ⚡️ TOP 10: Signal Consensus Analysis (Last 10 Min) ---")
        market_context = get_market_context()
        traders = session.query(TrackedTrader).all()
        trader_pnl_map = {t.user_address: t.pnl for t in traders if t.pnl and t.pnl > 0}
        if not trader_pnl_map:
            print("No trader PNL data found. Skipping consensus analysis.")
            return

        minutes_ago = 10
        cutoff_ms = (time.time() - (minutes_ago * 60)) * 1000
        new_trades_query = session.query(Fill).filter(
            Fill.timestamp >= cutoff_ms,
            Fill.direction.like('Open %')
        )
        new_trades = new_trades_query.all()
        if not new_trades:
            print(f"No new trades found in the last {minutes_ago} minutes.")
            return

        MIN_TRADE_VALUE = 10000
        consensus_data = defaultdict(lambda: {"traders": set(), "pnl_backing": 0.0, "total_value": 0.0, "direction": ""})

        for trade in new_trades:
            trade_value = trade.size * trade.price
            if trade.user_address in trader_pnl_map and trade_value >= MIN_TRADE_VALUE:
                key = (trade.asset, "Long" if "Long" in trade.direction else "Short")
                trader_pnl = trader_pnl_map[trade.user_address]
                consensus_data[key]["traders"].add(trade.user_address)
                consensus_data[key]["pnl_backing"] += trader_pnl
                consensus_data[key]["total_value"] += trade_value
                consensus_data[key]["direction"] = key[1]

        if not consensus_data:
            print("No consensus signals found above min value threshold.")
            return

        processed_consensus = []
        for (asset, direction), data in consensus_data.items():
            processed_consensus.append({"asset": asset, "direction": direction, "trader_count": len(data["traders"]),
                                       "pnl_backing": data["pnl_backing"], "total_value": data["total_value"]})
        sorted_consensus = sorted(processed_consensus, key=lambda x: x["pnl_backing"], reverse=True)

        for signal in sorted_consensus[:10]:
            direction_emoji = "🟢" if "Long" in signal['direction'] else "🔴"
            change_percent = market_context.get(signal["asset"])
            change_str = "N/A"
            if change_percent is not None:
                change_str = f"{change_percent:+.2f}%"

            message = (
                f"⚡️ *Consensus Signal* ⚡️\n"
                f"{direction_emoji} *{signal['direction']}* on *{signal['asset']}*\n\n"
                f"*Trader Count:* `{signal['trader_count']}`\n"
                f"*Total Value:* `${signal['total_value']:,.0f}`\n"
                f"*Smart Money:* `${signal['pnl_backing']:,.0f} (PNL)`\n"
                f"*24h Change:* `{change_str}`"
            )

            print(f"Sending consensus signal to Telegram: {signal['direction']} {signal['asset']}")
            await send_telegram_message(bot, message)
            await asyncio.sleep(0.5)
            
    finally:
        session.close()

# -------------------------------------------------
# تابع Main (ارکستراتور)
# -------------------------------------------------
async def main():
    print("Starting automated analysis...")
    timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M')
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    except Exception as e:
        print(f"❌ Error creating output directory '{OUTPUT_DIR}': {e}")
        return

    # ۱. ساخت نمونه Bot با تنظیمات پروکسی
    bot_instance = init_bot()

    # ۲. اجرای تحلیل‌های متنی (ارسال به تلگرام)
    if bot_instance:
        await analyze_trade_consensus(bot_instance, timestamp_str=timestamp_str, theme='dark')
        await track_new_trades(bot_instance, timestamp_str=timestamp_str, theme='dark')
    else:
        print("Skipping Telegram send functions as bot is not configured.")

    # ۳. اجرای تحلیل‌های تصویری (ذخیره در فایل)
    analyze_weighted_sentiment(timestamp_str=timestamp_str, theme='dark')
    analyze_recent_activity(timestamp_str=timestamp_str, theme='dark')
    
    print(f"✅ All analyses complete for timestamp {timestamp_str}.")

if __name__ == "__main__":
    asyncio.run(main())