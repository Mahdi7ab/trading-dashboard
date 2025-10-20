# collector/query_example.py

import sys
import csv
import time  # 🔽 (جدید) برای محاسبات زمانی
from datetime import datetime
from sqlalchemy import func
from database import SessionLocal, Fill, TrackedTrader
from prettytable import PrettyTable
from collections import defaultdict
from PIL import Image, ImageDraw, ImageFont

# ... توابع save_table_as_image و save_data_to_csv بدون تغییر باقی می‌مانند ...
def save_table_as_image(table_string, filename="analysis_results.png", theme='light'):
    print(f"🖼️  Saving table to {filename} with '{theme}' theme...")
    if theme == 'dark': bg_color, text_color = '#2c2f33', '#ffffff'
    else: bg_color, text_color = '#ffffff', '#000000'
    try: font = ImageFont.truetype("cour.ttf", 15)
    except IOError:
        print("Warning: 'cour.ttf' not found. Using default font.")
        font = ImageFont.load_default()
    dummy_draw = ImageDraw.Draw(Image.new('RGB', (0, 0)))
    text_bbox = dummy_draw.multiline_textbbox((0, 0), table_string, font=font)
    padding = 25
    img = Image.new('RGB', (text_bbox[2] + 2 * padding, text_bbox[3] + 2 * padding), color=bg_color)
    draw = ImageDraw.Draw(img)
    draw.multiline_text((padding, padding), table_string, font=font, fill=text_color)
    img.save(filename)
    print(f"✅ Image saved successfully as {filename}")

def save_data_to_csv(header, data_rows, filename="analysis_results.csv"):
    print(f"💾 Saving data to {filename}...")
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        writer.writerows(data_rows)
    print(f"✅ Data saved successfully as {filename}")

# -------------------------------------------------
# 🔽 (بازنویسی شده) تابع اصلی محاسبه پوزیشن‌های باز 🔽
# -------------------------------------------------
def get_open_positions(session, fills_query=None):
    """
    پوزیشن‌های باز را بر اساس یک کوئری fills خاص محاسبه می‌کند.
    اگر کوئری داده نشود، تمام fills ها را در نظر می‌گیرد.
    """
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

# -------------------------------------------------
# 🔽 (جدید) تابع کمکی برای تجمیع سنتیمنت 🔽
# -------------------------------------------------
def aggregate_sentiment(processed_positions, weights_map=None):
    """
    لیست پوزیشن‌های باز را به سنتیمنت تجمیعی تبدیل می‌کند.
    اگر weights_map داده شود، از PNL تریدر به عنوان وزن استفاده می‌کند.
    اگر داده نشود، هر تریدر را 1 رای در نظر می‌گیرد.
    """
    sentiment_data = defaultdict(lambda: {
        "weighted_long_count": 0.0,
        "weighted_short_count": 0.0,
        "long_value": 0.0,
        "short_value": 0.0,
        "long_traders_raw": 0, # شمارش خام تریدرها
        "short_traders_raw": 0 # شمارش خام تریدرها
    })

    for pos in processed_positions:
        asset = pos["asset"]
        
        # تعیین وزن: یا 1 (رای‌گیری عادی) یا PNL تریدر (رای‌گیری وزن‌دار)
        weight = 1.0
        if weights_map:
            # از 1.0 استفاده می‌کنیم تا تریدرهای بدون PNL هم حداقل 1 رای داشته باشند
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
            "asset": asset,
            "net_value": net_value,
            "sentiment_percent": sentiment_percent,
            "long_traders_raw": data["long_traders_raw"],
            "short_traders_raw": data["short_traders_raw"]
        })
    
    # مرتب‌سازی بر اساس مجموع وزن‌ها (مهم‌ترین‌ها اول)
    return sorted(processed_sentiment, key=lambda s: s["long_traders_raw"] + s["short_traders_raw"], reverse=True)


# -------------------------------------------------
# 🔽 (جدید) تابع کمکی برای چاپ جدول سنتیمنت 🔽
# -------------------------------------------------
def print_sentiment_table(sorted_sentiment, title, save_image=False, save_csv=False, theme='light', filename='sentiment'):
    """
    جدول PrettyTable را برای هر نوع تحلیل سنتیمنت چاپ می‌کند.
    """
    print(f"\n--- {title} ---")
    
    header = ["Asset", "Long Traders", "Short Traders", "Net Value ($)", "Sentiment %"]
    table = PrettyTable(header)
    table.align = "l"
    table.align["Long Traders"] = "r"; table.align["Short Traders"] = "r"
    table.align["Net Value ($)"] = "r"; table.align["Sentiment %"] = "r"
    
    csv_rows = [["Asset", "Long Traders", "Short Traders", "Net Value ($)", "Sentiment %"]]
    
    for item in sorted_sentiment:
        net_value_str = f"${item['net_value']:,.2f}"
        sentiment_str = f"{item['sentiment_percent']:.1f}%"
        
        # رنگی کردن خروجی
        if item["sentiment_percent"] > 25:
            asset_name = f"\033[92m{item['asset']}\033[0m"
            sentiment_str = f"\033[92m{sentiment_str}  bullish\033[0m"
        elif item["sentiment_percent"] < -25:
            asset_name = f"\033[91m{item['asset']}\033[0m"
            sentiment_str = f"\033[91m{sentiment_str} bearish\033[0m"
        else:
            asset_name = item['asset']
            sentiment_str = f"{sentiment_str} neutral"

        table.add_row([
            asset_name,
            item["long_traders_raw"],
            item["short_traders_raw"],
            net_value_str,
            sentiment_str
        ])
        
        csv_rows.append([
            item["asset"], item["long_traders_raw"], item["short_traders_raw"],
            item["net_value"], item["sentiment_percent"]
        ])

    print(table)
    if save_image: save_table_as_image(table.get_string(), filename=f"{filename}.png", theme=theme)
    if save_csv: save_data_to_csv(header, csv_rows, filename=f"{filename}.csv")


# --- تابع تحلیل تاریخچه (بدون تغییر) ---
def analyze_all_history(save_image=False, save_csv=False, theme='light'):
    session = SessionLocal()
    try:
        results = (
            session.query(
                Fill.user_address, Fill.asset, Fill.is_buy,
                func.count(Fill.id).label("trade_count"),
                func.sum(Fill.size).label("total_volume"),
                func.min(Fill.timestamp).label("first_trade"),
                func.max(Fill.timestamp).label("last_trade")
            )
            .group_by(Fill.user_address, Fill.asset, Fill.is_buy)
            .order_by(Fill.user_address, Fill.asset, Fill.is_buy.desc())
            .all()
        )
        if not results: 
            print("No 'Fill' data found in database.")
            return

        header = ["User Address", "Asset", "Side", "Trade Count", "Total Volume", "First Trade", "Last Trade"]
        table = PrettyTable(header)
        table.align = "l"
        table.align["Trade Count"] = "r"; table.align["Total Volume"] = "r"
        
        csv_rows = []
        current_user = None
        for user, asset, is_buy, count, volume, first_ts, last_ts in results:
            first_trade_time = datetime.fromtimestamp(first_ts / 1000).strftime('%Y/%m/%d %H:%M:%S')
            last_trade_time = datetime.fromtimestamp(last_ts / 1000).strftime('%Y/%m/%d %H:%M:%S')
            side = "Buy" if is_buy else "Sell"
            
            csv_rows.append([user, asset, side, count, f"{volume:.4f}", first_trade_time, last_trade_time])

            if user != current_user:
                if current_user is not None: table.add_row(['-' * 44, '-' * 8, '-' * 5, '-' * 12, '-' * 15, '-'*20, '-'*20], divider=True)
                display_user = user; current_user = user
            else: display_user = ""
            table.add_row([display_user, asset, side, count, f"{volume:,.4f}", first_trade_time, last_trade_time])
        
        print("\n--- Full Trade History Analysis ---")
        print(table)
        if save_image: save_table_as_image(table.get_string(), filename="history_analysis.png", theme=theme)
        if save_csv: save_data_to_csv(header, csv_rows, filename="history_analysis.csv")

    finally:
        session.close()

# --- تابع تحلیل پوزیشن‌های باز (بدون تغییر) ---
def analyze_open_positions(save_image=False, save_csv=False, theme='light'):
    session = SessionLocal()
    try:
        processed_positions = get_open_positions(session)
        if not processed_positions:
            print("No open positions found.")
            return

        sorted_positions = sorted(processed_positions, key=lambda p: (p["user"], -p["position_value"]))
        
        header = ["User Address", "Asset", "Side", "Net Volume", "Avg. Entry Price", "Position Value"]
        table = PrettyTable(header)
        table.align = "l"
        table.align["Net Volume"] = "r"; table.align["Avg. Entry Price"] = "r"; table.align["Position Value"] = "r"
        
        csv_rows = []
        current_user = None
        for pos in sorted_positions:
            csv_rows.append([
                pos["user"], pos["asset"], pos["side"], f"{abs(pos['net_volume']):.4f}",
                f"{pos['avg_price']:.2f}", f"{pos['position_value']:.2f}"
            ])

            if pos["user"] != current_user:
                if current_user is not None: table.add_row(['-' * 44, '-' * 8, '-' * 5, '-' * 15, '-' * 18, '-'*18], divider=True)
                display_user = pos["user"]; current_user = pos["user"]
            else: display_user = ""
            table.add_row([display_user, pos["asset"], pos["side"], f"{abs(pos['net_volume']):,.4f}", f"${pos['avg_price']:,.2f}", f"${pos['position_value']:,.2f}"])
        
        print("\n--- Open Positions Analysis (Sorted by Position Value) ---")
        print(table)
        if save_image: save_table_as_image(table.get_string(), filename="open_positions.png", theme=theme)
        if save_csv: save_data_to_csv(header, csv_rows, filename="open_positions.csv")

    finally:
        session.close()


# --- سطح ۱: سنتیمنت کلی (بازنویسی شده) ---
def analyze_market_sentiment(save_image=False, save_csv=False, theme='light'):
    session = SessionLocal()
    try:
        # ۱. گرفتن تمام پوزیشن‌های باز
        positions = get_open_positions(session)
        if not positions:
            print("No open positions found to analyze sentiment.")
            return
            
        # ۲. تجمیع سنتیمنت (روش عادی: ۱ تریدر = ۱ رای)
        sentiment = aggregate_sentiment(positions, weights_map=None)
        
        # ۳. چاپ جدول
        print_sentiment_table(sentiment, "📊 سطح ۱: سنتیمنت کلی بازار (بر اساس تعداد تریدر)",
                              save_image, save_csv, theme, filename="sentiment_trader_count")
    finally:
        session.close()

# -------------------------------------------------
# 🔽 --- سطح ۲: سنتیمنت فعالیت اخیر (جدید) --- 🔽
# -------------------------------------------------
def analyze_recent_activity(save_image=False, save_csv=False, theme='light'):
    session = SessionLocal()
    try:
        # ۱. محاسبه زمان برای ۲۴ ساعت گذشته (بر حسب میلی‌ثانیه)
        cutoff_ms = (time.time() - 86400) * 1000
        
        # ۲. ساخت کوئری فقط برای fills های ۲۴ ساعت گذشته
        recent_fills_query = session.query(Fill).filter(Fill.timestamp >= cutoff_ms)
        
        # ۳. گرفتن پوزیشن‌های باز *فقط بر اساس* معاملات اخیر
        # این به ما "جریان" (flow) جدید پول را نشان می‌دهد
        positions = get_open_positions(session, fills_query=recent_fills_query)
        
        if not positions:
            print("No recent (24h) fills found to analyze activity.")
            return
            
        # ۴. تجمیع سنتیمنت (روش عادی: ۱ تریدر = ۱ رای)
        sentiment = aggregate_sentiment(positions, weights_map=None)
        
        # ۵. چاپ جدول
        print_sentiment_table(sentiment, "📈 سطح ۲: سنتیمنت فعالیت اخیر (۲۴ ساعت گذشته)",
                              save_image, save_csv, theme, filename="sentiment_recent_24h")
    finally:
        session.close()

# -------------------------------------------------
# 🔽 --- سطح ۳: سنتیمنت وزن‌دهی شده (جدید) --- 🔽
# -------------------------------------------------
def analyze_weighted_sentiment(save_image=False, save_csv=False, theme='light'):
    session = SessionLocal()
    try:
        # ۱. گرفتن تمام پوزیشن‌های باز
        positions = get_open_positions(session)
        if not positions:
            print("No open positions found to analyze sentiment.")
            return

        # ۲. گرفتن وزن تریدرها (PNL) از دیتابیس
        traders = session.query(TrackedTrader).all()
        # فقط PNL های مثبت را به عنوان وزن در نظر می‌گیریم
        weights_map = {t.user_address: t.pnl for t in traders if t.pnl and t.pnl > 0}
        
        if not weights_map:
            print("No trader PNL data found in 'tracked_traders'. Running standard sentiment.")
            # اگر دیتای PNL نبود، برمی‌گردیم به سطح ۱
            analyze_market_sentiment(save_image, save_csv, theme)
            return

        # ۳. تجمیع سنتیمنت (روش وزن‌دار: ۱ تریدر = PNL او)
        sentiment = aggregate_sentiment(positions, weights_map=weights_map)
        
        # ۴. چاپ جدول
        print_sentiment_table(sentiment, "👑 سطح ۳: سنتیمنت وزن‌دهی شده (بر اساس PNL تریدر)",
                              save_image, save_csv, theme, filename="sentiment_weighted_pnl")
    finally:
        session.close()


# --- تابع Main (به‌روز شده برای پشتیبانی از حالت‌های جدید) ---
def main():
    args = sys.argv[1:]
    
    save_image_flag = '--save-image' in args
    if save_image_flag: args.remove('--save-image')

    save_csv_flag = '--csv' in args
    if save_csv_flag: args.remove('--csv')
    
    theme = 'light'
    if '--theme' in args:
        theme_index = args.index('--theme')
        if theme_index + 1 < len(args):
            theme = args[theme_index + 1].lower()
            args.pop(theme_index); args.pop(theme_index)
        else: args.remove('--theme')
        
    # 🔽 (تغییر) حالت پیش‌فرض را به 'weighted' (سطح ۳) تغییر می‌دهیم
    mode = 'weighted'
    if args: mode = args[0].lower()

    if mode == "all":
        analyze_all_history(save_image=save_image_flag, save_csv=save_csv_flag, theme=theme)
    elif mode == "open":
        analyze_open_positions(save_image=save_image_flag, save_csv=save_csv_flag, theme=theme)
    elif mode == "sentiment": # سطح ۱
        analyze_market_sentiment(save_image=save_image_flag, save_csv=save_csv_flag, theme=theme)
    # -------------------------------------------------
    # 🔽 (جدید) اضافه کردن حالت‌های سطح ۲ و ۳ 🔽
    # -------------------------------------------------
    elif mode == "recent": # سطح ۲
        analyze_recent_activity(save_image=save_image_flag, save_csv=save_csv_flag, theme=theme)
    elif mode == "weighted": # سطح ۳
        analyze_weighted_sentiment(save_image=save_image_flag, save_csv=save_csv_flag, theme=theme)
    # -------------------------------------------------
    else:
        print(f"Error: Unknown mode '{mode}'.")
        print("Usage: python query_example.py [all|open|sentiment|recent|weighted] [--save-image] [--csv] [--theme dark|light]")

if __name__ == "__main__":
    main()