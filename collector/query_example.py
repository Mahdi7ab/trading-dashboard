# query_example.py

import sys
import csv  # <-- کتابخانه جدید برای کار با فایل‌های CSV
from datetime import datetime
from sqlalchemy import func
from database import SessionLocal, Fill
from prettytable import PrettyTable
from collections import defaultdict
from PIL import Image, ImageDraw, ImageFont

# ... تابع save_table_as_image بدون تغییر باقی می‌ماند ...
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
    """
    داده‌های پردازش شده را در یک فایل CSV ذخیره می‌کند.
    """
    print(f"💾 Saving data to {filename}...")
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)  # نوشتن هدر ستون‌ها
        writer.writerows(data_rows) # نوشتن تمام ردیف‌های داده
    print(f"✅ Data saved successfully as {filename}")

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
        if not results: return

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
            
            # آماده‌سازی داده‌ها برای CSV
            csv_rows.append([user, asset, side, count, f"{volume:.4f}", first_trade_time, last_trade_time])

            # آماده‌سازی داده‌ها برای جدول نمایشی
            if user != current_user:
                if current_user is not None: table.add_row(['-' * 44, '-' * 8, '-' * 5, '-' * 12, '-' * 15, '-'*20, '-'*20], divider=True)
                display_user = user; current_user = user
            else: display_user = ""
            table.add_row([display_user, asset, side, count, f"{volume:,.4f}", first_trade_time, last_trade_time])
        
        print("\n--- Full Trade History Analysis ---")
        print(table)
        if save_image: save_table_as_image(table.get_string(), theme=theme)
        if save_csv: save_data_to_csv(header, csv_rows)

    finally:
        session.close()

def analyze_open_positions(save_image=False, save_csv=False, theme='light'):
    session = SessionLocal()
    try:
        all_fills = session.query(Fill).all()
        if not all_fills: return

        positions_data = defaultdict(lambda: {
            "buy_volume": 0.0, "sell_volume": 0.0, "weighted_buy_sum": 0.0, 
            "weighted_sell_sum": 0.0, "min_timestamp": float('inf'), "max_timestamp": 0
        })

        for fill in all_fills:
            key = (fill.user_address, fill.asset)
            if fill.is_buy:
                positions_data[key]["buy_volume"] += fill.size; positions_data[key]["weighted_buy_sum"] += fill.size * fill.price
            else:
                positions_data[key]["sell_volume"] += fill.size; positions_data[key]["weighted_sell_sum"] += fill.size * fill.price
            positions_data[key]["min_timestamp"] = min(positions_data[key]["min_timestamp"], fill.timestamp)
            positions_data[key]["max_timestamp"] = max(positions_data[key]["max_timestamp"], fill.timestamp)

        processed_positions = []
        for (user, asset), data in positions_data.items():
            net_volume = data["buy_volume"] - data["sell_volume"]
            if abs(net_volume) > 1e-9:
                if net_volume > 0: side = "Long"; avg_price = data["weighted_buy_sum"] / data["buy_volume"] if data["buy_volume"] > 0 else 0
                else: side = "Short"; avg_price = data["weighted_sell_sum"] / data["sell_volume"] if data["sell_volume"] > 0 else 0
                position_value = abs(net_volume) * avg_price
                processed_positions.append({
                    "user": user, "asset": asset, "side": side, "net_volume": net_volume, 
                    "avg_price": avg_price, "position_value": position_value,
                    "first_trade": data["min_timestamp"], "last_trade": data["max_timestamp"]
                })

        sorted_positions = sorted(processed_positions, key=lambda p: (p["user"], -p["position_value"]))
        
        header = ["User Address", "Asset", "Side", "Net Volume", "Avg. Entry Price", "Position Value", "First Trade", "Last Trade"]
        table = PrettyTable(header)
        table.align = "l"
        table.align["Net Volume"] = "r"; table.align["Avg. Entry Price"] = "r"; table.align["Position Value"] = "r"
        
        csv_rows = []
        current_user = None
        for pos in sorted_positions:
            first_trade_time = datetime.fromtimestamp(pos["first_trade"] / 1000).strftime('%Y/%m/%d %H:%M:%S')
            last_trade_time = datetime.fromtimestamp(pos["last_trade"] / 1000).strftime('%Y/%m/%d %H:%M:%S')
            
            csv_rows.append([
                pos["user"], pos["asset"], pos["side"], f"{abs(pos['net_volume']):.4f}",
                f"{pos['avg_price']:.2f}", f"{pos['position_value']:.2f}",
                first_trade_time, last_trade_time
            ])

            if pos["user"] != current_user:
                if current_user is not None: table.add_row(['-' * 44, '-' * 8, '-' * 5, '-' * 15, '-' * 18, '-'*18, '-'*20, '-'*20], divider=True)
                display_user = pos["user"]; current_user = pos["user"]
            else: display_user = ""
            table.add_row([display_user, pos["asset"], pos["side"], f"{abs(pos['net_volume']):,.4f}", f"${pos['avg_price']:,.2f}", f"${pos['position_value']:,.2f}", first_trade_time, last_trade_time])
        
        print("\n--- Open Positions Analysis (Sorted by Position Value) ---")
        print(table)
        if save_image: save_table_as_image(table.get_string(), theme=theme)
        if save_csv: save_data_to_csv(header, csv_rows)

    finally:
        session.close()

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
        
    mode = 'all'
    if args: mode = args[0].lower()

    if mode == "all":
        analyze_all_history(save_image=save_image_flag, save_csv=save_csv_flag, theme=theme)
    elif mode == "open":
        analyze_open_positions(save_image=save_image_flag, save_csv=save_csv_flag, theme=theme)
    else:
        print(f"Error: Unknown mode '{mode}'. Please use 'all' or 'open'.")
        print("Usage: python query_example.py [all|open] [--save-image] [--csv] [--theme dark|light]")

if __name__ == "__main__":
    main()