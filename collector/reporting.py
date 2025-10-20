# collector/reporting.py

import os
import csv
from prettytable import PrettyTable
from PIL import Image, ImageDraw, ImageFont

OUTPUT_DIR = "results"

def save_table_as_image(table_string, base_filename, timestamp_str, theme='light'):
    """
    جدول را به عنوان عکس با فونت DejaVuSansMono ذخیره می‌کند.
    """
    filename = os.path.join(OUTPUT_DIR, f"{base_filename}_{timestamp_str}.png")
    print(f"🖼️  Saving table to {filename} with '{theme}' theme...")
    if theme == 'dark':
        bg_color, text_color = '#2c2f33', '#ffffff'
    else:
        bg_color, text_color = '#ffffff', '#000000'
    
    try:
        font = ImageFont.truetype("DejaVuSansMono.ttf", 15)
    except IOError:
        print("Warning: 'DejaVuSansMono.ttf' not found. Using default font.")
        font = ImageFont.load_default()
    
    dummy_draw = ImageDraw.Draw(Image.new('RGB', (0, 0)))
    text_bbox = dummy_draw.multiline_textbbox((0, 0), table_string, font=font)
    padding = 25
    img = Image.new('RGB', (text_bbox[2] + 2 * padding, text_bbox[3] + 2 * padding), color=bg_color)
    draw = ImageDraw.Draw(img)
    draw.multiline_text((padding, padding), table_string, font=font, fill=text_color)
    img.save(filename)
    print(f"✅ Image saved successfully as {filename}")

def save_data_to_csv(header, data_rows, base_filename, timestamp_str):
    """
    داده‌ها را در یک فایل CSV ذخیره می‌کند.
    """
    filename = os.path.join(OUTPUT_DIR, f"{base_filename}_{timestamp_str}.csv")
    print(f"💾 Saving data to {filename}...")
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        writer.writerows(data_rows)
    print(f"✅ Data saved successfully as {filename}")

def print_sentiment_table(sorted_sentiment, title, base_filename, timestamp_str, theme='light'):
    """
    جدول خلاصه سنتیمنت را برای کنسول و عکس می‌سازد.
    """
    print(f"\n--- {title} ---")
    header = ["Asset", "Long Traders", "Short Traders", "Net Value ($)", "Sentiment %"]
    
    # جدول عکس (ایموجی)
    image_table = PrettyTable(header)
    image_table.align = "l"
    
    # جدول کنسول (رنگی)
    console_table = PrettyTable(header)
    console_table.align = "l"
    
    for align_col in ["Long Traders", "Short Traders", "Net Value ($)", "Sentiment %"]:
        image_table.align[align_col] = "r"
        console_table.align[align_col] = "r"
        
    csv_rows = [header]
    
    for item in sorted_sentiment:
        net_value_str = f"${item['net_value']:,.2f}"
        sentiment_percent_str = f"{item['sentiment_percent']:.1f}%"
        csv_rows.append([item["asset"], item["long_traders_raw"], item["short_traders_raw"], item["net_value"], item["sentiment_percent"]])

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
            
        console_table.add_row([asset_name_console, item["long_traders_raw"], item["short_traders_raw"], net_value_str, sentiment_str_console])
        image_table.add_row([asset_name_image, item["long_traders_raw"], item["short_traders_raw"], net_value_str, sentiment_str_image])

    print(console_table)
    save_table_as_image(image_table.get_string(), base_filename=base_filename, timestamp_str=timestamp_str, theme=theme)
    save_data_to_csv(header, csv_rows, base_filename=base_filename, timestamp_str=timestamp_str)