# collector/query_example.py

import sys
import csv
import time
import os
from datetime import datetime
from sqlalchemy import func
from database import SessionLocal, Fill, TrackedTrader
from prettytable import PrettyTable
from collections import defaultdict
from PIL import Image, ImageDraw, ImageFont

OUTPUT_DIR = "results"

# --- (تابع ذخیره عکس - بدون تغییر) ---
def save_table_as_image(table_string, base_filename, timestamp_str, theme='light'):
    filename = os.path.join(OUTPUT_DIR, f"{base_filename}_{timestamp_str}.png")
    print(f"🖼️  Saving table to {filename} with '{theme}' theme...")
    if theme == 'dark': bg_color, text_color = '#2c2f33', '#ffffff'
    else: bg_color, text_color = '#ffffff', '#000000'
    try:
        font = ImageFont.truetype("DejaVuSansMono.ttf", 15)
    except IOError:
        print("Warning: 'DejaVuSansMono.ttf' not found. Using default font.")
        font = ImageFont.load_default()
    
    # ... (بقیه تابع بدون تغییر) ...
    dummy_draw = ImageDraw.Draw(Image.new('RGB', (0, 0)))
    text_bbox = dummy_draw.multiline_textbbox((0, 0), table_string, font=font)
    padding = 25
    img = Image.new('RGB', (text_bbox[2] + 2 * padding, text_bbox[3] + 2 * padding), color=bg_color)
    draw = ImageDraw.Draw(img)
    draw.multiline_text((padding, padding), table_string, font=font, fill=text_color)
    img.save(filename)
    print(f"✅ Image saved successfully as {filename}")

# --- (تابع ذخیره CSV - بدون تغییر) ---
def save_data_to_csv(header, data_rows, base_filename, timestamp_str):
    filename = os.path.join(OUTPUT_DIR, f"{base_filename}_{timestamp_str}.csv")
    print(f"💾 Saving data to {filename}...")
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        writer.writerows(data_rows)
    print(f"✅ Data saved successfully as {filename}")

# --- (توابع get_open_positions و aggregate_sentiment - بدون تغییر) ---
def get_open_positions(session, fills_query=None):
    if fills_query is None:
        fills_query = session.query(Fill)
    all_fills = fills_query.all()
    if not all_fills:
        return []
    positions_data = defaultdict(lambda: {
        "buy_volume": 0.0, "sell_volume": 0.0, "weighted_buy_sum": 0.0, 
        "weighted_sell_sum": 0.0
    })
    for fill in all_fills:
        key = (fill.user_address, fill.asset)
        if fill.is_buy:
            positions_data[key]["buy_volume"] += fill.size
            positions_data[key]["weighted_buy_sum"] += fill.size * fill.price
        else:
            positions_data[key]["sell_volume"] += fill.size
            positions_data[key]["weighted_sell_sum"] += fill.size * fill.price
    processed_positions = []
    for (user, asset), data in positions_data.items():
        net_volume = data["buy_volume"] - data["sell_volume"]
        if abs(net_volume) > 1e-9:
            if net_volume > 0:
                side = "Long"
                avg_price = data["weighted_buy_sum"] / data["buy_volume"] if data["buy_volume"] > 0 else 0
            else:
                side = "Short"
                avg_price = data["weighted_sell_sum"] / data["sell_volume"] if data["sell_volume"] > 0 else 0
            position_value = abs(net_volume) * avg_price
            processed_positions.append({
                "user": user, "asset": asset, "side": side, "net_volume": net_volume, 
                "avg_price": avg_price, "position_value": position_value
            })
    return processed_positions

def aggregate_sentiment(processed_positions, weights_map=None):
    sentiment_data = defaultdict(lambda: {
        "weighted_long_count": 0.0, "weighted_short_count": 0.0,
        "long_value": 0.0, "short_value": 0.0,
        "long_traders_raw": 0, "short_traders_raw": 0
    })
    for pos in processed_positions:
        asset = pos["asset"]
        weight = 1.0
        if weights_map:
            weight = weights_map.get(pos["user"], 1.0) 
        if pos["side"] == "Long":
            sentiment_data[asset]["weighted_long_count"] += weight
            sentiment_data[asset]["long_value"] += pos["position_value"]
            sentiment_data[asset]["long_traders_raw"] += 1
        else: # Short
            sentiment_data[asset]["weighted_short_count"] += weight
            sentiment_data[asset]["short_value"] += pos["position_value"]
            sentiment_data[asset]["short_traders_raw"] += 1
    processed_sentiment = []
    for asset, data in sentiment_data.items():
        total_weight = data["weighted_long_count"] + data["weighted_short_count"]
        net_value = data["long_value"] - data["short_value"]
        sentiment_percent = 0
        if total_weight > 0:
            sentiment_percent = ((data["weighted_long_count"] - data["weighted_short_count"]) / total_weight) * 100
        processed_sentiment.append({
            "asset": asset, "net_value": net_value, "sentiment_percent": sentiment_percent,
            "long_traders_raw": data["long_traders_raw"], "short_traders_raw": data["short_traders_raw"]
        })
    return sorted(processed_sentiment, key=lambda s: s["long_traders_raw"] + s["short_traders_raw"], reverse=True)

# --- (توابع analyze_all_history و analyze_open_positions - بدون تغییر) ---
# ... (این توابع طولانی هستند و تغییری نکرده‌اند، فرض می‌کنیم وجود دارند) ...

# -------------------------------------------------
# 🔽 (بازنویسی شده) تابع چاپ سنتیمنت 🔽
# -------------------------------------------------
def print_sentiment_table(sorted_sentiment, title, base_filename, timestamp_str, theme='light'):
    """
    دو جدول می‌سازد: یکی رنگی برای کنسول، یکی تمیز با ایموجی برای عکس.
    """
    print(f"\n--- {title} ---")
    
    header = ["Asset", "Long Traders", "Short Traders", "Net Value ($)", "Sentiment %"]
    
    # --- ۱. ساخت جدول برای عکس (تمیز + ایموجی) ---
    image_table = PrettyTable(header)
    image_table.align = "l"
    image_table.align["Long Traders"] = "r"; image_table.align["Short Traders"] = "r"
    image_table.align["Net Value ($)"] = "r"; image_table.align["Sentiment %"] = "r"
    
    # --- ۲. ساخت جدول برای کنسول (رنگی) ---
    console_table = PrettyTable(header)
    console_table.align = "l"
    console_table.align["Long Traders"] = "r"; console_table.align["Short Traders"] = "r"
    console_table.align["Net Value ($)"] = "r"; console_table.align["Sentiment %"] = "r"
    
    csv_rows = [["Asset", "Long Traders", "Short Traders", "Net Value ($)", "Sentiment %"]]
    
    for item in sorted_sentiment:
        net_value_str = f"${item['net_value']:,.2f}"
        sentiment_percent_str = f"{item['sentiment_percent']:.1f}%"
        
        # داده CSV
        csv_rows.append([
            item["asset"], item["long_traders_raw"], item["short_traders_raw"],
            item["net_value"], item["sentiment_percent"]
        ])

        # داده برای جداول
        if item["sentiment_percent"] > 25:
            asset_name_console = f"\033[92m{item['asset']}\033[0m"
            sentiment_str_console = f"\033[92m{sentiment_percent_str}  bullish\033[0m"
            asset_name_image = f"🟢 {item['asset']}"
            sentiment_str_image = f"{sentiment_percent_str}  bullish"
        elif item["sentiment_percent"] < -25:
            asset_name_console = f"\033[91m{item['asset']}\033[0m"
            sentiment_str_console = f"\033[91m{sentiment_percent_str} bearish\033[0m"
            asset_name_image = f"🔴 {item['asset']}"
            sentiment_str_image = f"{sentiment_percent_str} bearish"
        else:
            asset_name_console = item['asset']
            sentiment_str_console = f"{sentiment_percent_str} neutral"
            asset_name_image = f"⚪️ {item['asset']}"
            sentiment_str_image = f"{sentiment_percent_str} neutral"

        console_table.add_row([
            asset_name_console, item["long_traders_raw"], item["short_traders_raw"], 
            net_value_str, sentiment_str_console
        ])
        
        image_table.add_row([
            asset_name_image, item["long_traders_raw"], item["short_traders_raw"], 
            net_value_str, sentiment_str_image
        ])

    # --- ۳. چاپ جدول کنسول ---
    print(console_table)
    
    # --- ۴. ذخیره جدول عکس ---
    save_table_as_image(image_table.get_string(), base_filename=base_filename, timestamp_str=timestamp_str, theme=theme)
    save_data_to_csv(header, csv_rows, base_filename=base_filename, timestamp_str=timestamp_str)


# --- (توابع analyze_... - بدون تغییر) ---
def analyze_market_sentiment(timestamp_str, theme='light'):
    session = SessionLocal()
    try:
        positions = get_open_positions(session)
        if not positions: return
        sentiment = aggregate_sentiment(positions, weights_map=None)
        print_sentiment_table(sentiment, "📊 سطح ۱: سنتیمنت کلی بازار",
                              base_filename="sentiment_trader_count", timestamp_str=timestamp_str, theme=theme)
    finally:
        session.close()

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
        if not positions: return
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
# 🔽 (بازنویسی شده) تابع ردیابی معاملات جدید 🔽
# -------------------------------------------------
def track_new_trades(timestamp_str, theme='light'):
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

        title = f"🛰️ New Trades Opened in Last {minutes_ago} Minutes"
        print(f"\n--- {title} ---")
        
        header = ["Timestamp", "User Address", "Asset", "Direction", "Price", "Size ($)"]
        
        # --- ۱. ساخت جدول برای عکس (تمیز + ایموجی) ---
        image_table = PrettyTable(header)
        image_table.align = "l"
        image_table.align["Price"] = "r"; image_table.align["Size ($)"] = "r"
        
        # --- ۲. ساخت جدول برای کنسول (رنگی) ---
        console_table = PrettyTable(header)
        console_table.align = "l"
        console_table.align["Price"] = "r"; console_table.align["Size ($)"] = "r"
        
        csv_rows = [header]
        
        for trade in new_trades:
            trade_time = datetime.fromtimestamp(trade.timestamp / 1000).strftime('%Y/%m/%d %H:%M:%S')
            position_value = trade.size * trade.price
            
            # داده CSV
            csv_rows.append([
                trade_time, trade.user_address, trade.asset,
                trade.direction, trade.price, position_value
            ])
            
            # داده برای جداول
            if "Long" in trade.direction:
                direction_console = f"\033[92m{trade.direction}\033[0m"
                direction_image = f"🟢 {trade.direction}"
            else:
                direction_console = f"\033[91m{trade.direction}\033[0m"
                direction_image = f"🔴 {trade.direction}"

            console_table.add_row([
                trade_time, trade.user_address, trade.asset,
                direction_console, f"${trade.price:,.2f}", f"${position_value:,.2f}"
            ])
            
            image_table.add_row([
                trade_time, trade.user_address, trade.asset,
                direction_image, f"${trade.price:,.2f}", f"${position_value:,.2f}"
            ])

        # --- ۳. چاپ جدول کنسول ---
        print(console_table)
        
        # --- ۴. ذخیره جدول عکس ---
        base_filename = "signals_new_trades"
        save_table_as_image(image_table.get_string(), base_filename=base_filename, timestamp_str=timestamp_str, theme=theme)
        save_data_to_csv(header, csv_rows, base_filename=base_filename, timestamp_str=timestamp_str)
        
    finally:
        session.close()

# -------------------------------------------------
# 🔽 (بازنویسی شده) تابع تحلیل اجماع 🔽
# -------------------------------------------------
def analyze_trade_consensus(timestamp_str, theme='light'):
    session = SessionLocal()
    try:
        print(f"\n--- ⚡️ TOP 10: Signal Consensus Analysis (Last 10 Min) ---")
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
            print(f"No new trades found in the last {minutes_ago} minutes to analyze consensus.")
            return

        consensus_data = defaultdict(lambda: {
            "traders": set(), "pnl_backing": 0.0,
            "total_value": 0.0, "direction": ""
        })

        for trade in new_trades:
            if trade.user_address in trader_pnl_map:
                key = (trade.asset, "Long" if "Long" in trade.direction else "Short")
                trader_pnl = trader_pnl_map[trade.user_address]
                consensus_data[key]["traders"].add(trade.user_address)
                consensus_data[key]["pnl_backing"] += trader_pnl
                consensus_data[key]["total_value"] += (trade.size * trade.price)
                consensus_data[key]["direction"] = key[1]

        if not consensus_data:
            print("No consensus signals found among tracked traders.")
            return

        processed_consensus = []
        for (asset, direction), data in consensus_data.items():
            processed_consensus.append({
                "asset": asset, "direction": direction, "trader_count": len(data["traders"]),
                "pnl_backing": data["pnl_backing"], "total_value": data["total_value"]
            })
        sorted_consensus = sorted(processed_consensus, key=lambda x: x["pnl_backing"], reverse=True)

        header = ["Asset", "Direction", "Trader Count", "Position Value ($)", "Smart Money ($)"]
        
        # --- ۱. ساخت جدول برای عکس (تمیز + ایموجی) ---
        image_table = PrettyTable(header)
        image_table.align = "l"
        image_table.align["Trader Count"] = "r"; image_table.align["Position Value ($)"] = "r"; image_table.align["Smart Money ($)"] = "r"
        
        # --- ۲. ساخت جدول برای کنسول (رنگی) ---
        console_table = PrettyTable(header)
        console_table.align = "l"
        console_table.align["Trader Count"] = "r"; console_table.align["Position Value ($)"] = "r"; console_table.align["Smart Money ($)"] = "r"
        
        csv_rows = [header]

        for signal in sorted_consensus[:10]:
            csv_rows.append([
                signal["asset"], signal["direction"], signal["trader_count"],
                signal["total_value"], signal["pnl_backing"]
            ])

            if "Long" in signal['direction']:
                direction_console = f"\033[92m{signal['direction']}\033[0m"
                direction_image = f"🟢 {signal['direction']}"
            else:
                direction_console = f"\033[91m{signal['direction']}\033[0m"
                direction_image = f"🔴 {signal['direction']}"
            
            console_table.add_row([
                signal["asset"], direction_console, signal["trader_count"],
                f"${signal['total_value']:,.2f}", f"${signal['pnl_backing']:,.2f}"
            ])
            
            image_table.add_row([
                signal["asset"], direction_image, signal["trader_count"],
                f"${signal['total_value']:,.2f}", f"${signal['pnl_backing']:,.2f}"
            ])

        # --- ۳. چاپ جدول کنسول ---
        print(console_table)
        
        # --- ۴. ذخیره جدول عکس ---
        base_filename = "signals_top_10_consensus"
        save_table_as_image(image_table.get_string(), base_filename=base_filename, timestamp_str=timestamp_str, theme='dark')
        save_data_to_csv(header, csv_rows, base_filename=base_filename, timestamp_str=timestamp_str)
        
    finally:
        session.close()

# --- (تابع Main - بدون تغییر) ---
def main():
    print("Starting automated analysis...")
    timestamp_str = datetime.now().strftime('%Y-%m-%d_%H-%M')
    try:
        os.makedirs(OUTPUT_DIR, exist_ok=True)
    except Exception as e:
        print(f"❌ Error creating output directory '{OUTPUT_DIR}': {e}")
        return

    # اجرای تحلیل‌ها
    analyze_trade_consensus(timestamp_str=timestamp_str, theme='dark')
    track_new_trades(timestamp_str=timestamp_str, theme='dark')
    analyze_weighted_sentiment(timestamp_str=timestamp_str, theme='dark')
    analyze_recent_activity(timestamp_str=timestamp_str, theme='dark')
    
    print(f"✅ All analyses complete for timestamp {timestamp_str}.")

if __name__ == "__main__":
    # این بخش باید شامل تمام توابع analyze_... و توابع کمکی باشد
    # ...
    main()