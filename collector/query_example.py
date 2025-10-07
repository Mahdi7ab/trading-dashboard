# query_example.py

import sys
from sqlalchemy import func
from database import SessionLocal, Fill
from prettytable import PrettyTable
from collections import defaultdict
from PIL import Image, ImageDraw, ImageFont

def save_table_as_image(table_string, filename="analysis_results.png", theme='light'):
    """
    Converts a string (from PrettyTable) into an image file with a specified theme.
    """
    print(f"🖼️  Saving table to {filename} with '{theme}' theme...")

    # --- Color and Font Settings ---
    if theme == 'dark':
        bg_color = '#2c2f33'  # Dark gray
        text_color = '#ffffff' # White
    else: # Default to light theme
        bg_color = '#ffffff'  # White
        text_color = '#000000' # Black

    try:
        font = ImageFont.truetype("cour.ttf", 15)
    except IOError:
        print("Warning: 'cour.ttf' not found. Using default font (alignment may be off).")
        font = ImageFont.load_default()

    # --- Image Generation ---
    dummy_draw = ImageDraw.Draw(Image.new('RGB', (0, 0)))
    text_bbox = dummy_draw.multiline_textbbox((0, 0), table_string, font=font)
    
    padding = 25
    image_width = text_bbox[2] + 2 * padding
    image_height = text_bbox[3] + 2 * padding
    
    img = Image.new('RGB', (image_width, image_height), color=bg_color)
    draw = ImageDraw.Draw(img)
    
    draw.multiline_text((padding, padding), table_string, font=font, fill=text_color)
    
    img.save(filename)
    print(f"✅ Image saved successfully as {filename}")

def analyze_all_history(save_image=False, theme='light'):
    """
    Analyzes the complete trade history.
    """
    session = SessionLocal()
    try:
        # The query and data processing are identical to before
        results = (
            session.query(
                Fill.user_address, Fill.asset, Fill.is_buy,
                func.count(Fill.id).label("trade_count"),
                func.sum(Fill.size).label("total_volume")
            )
            .group_by(Fill.user_address, Fill.asset, Fill.is_buy)
            .order_by(Fill.user_address, Fill.asset, Fill.is_buy.desc())
            .all()
        )
        if not results:
            print("No historical data found.")
            return

        table = PrettyTable()
        table.field_names = ["User Address", "Asset", "Side", "Trade Count", "Total Volume"]
        # ... table formatting ...
        
        # This part remains the same, just populating the table
        current_user = None
        for user, asset, is_buy, count, volume in results:
            if user != current_user:
                if current_user is not None: table.add_row(['-' * 44, '-' * 8, '-' * 5, '-' * 12, '-' * 15], divider=True)
                display_user = user; current_user = user
            else: display_user = ""
            table.add_row([display_user, asset, "Buy" if is_buy else "Sell", count, f"{volume:,.4f}"])
        
        print("\n--- Full Trade History Analysis ---")
        print(table)
        
        if save_image:
            save_table_as_image(table.get_string(), theme=theme)
    finally:
        session.close()

def analyze_open_positions(save_image=False, theme='light'):
    """
    Calculates and displays open positions with sorting and theming.
    """
    session = SessionLocal()
    try:
        # The data calculation logic remains the same
        all_fills = session.query(Fill).all()
        if not all_fills:
            print("No data to analyze.")
            return
            
        positions_data = defaultdict(lambda: {"buy_volume": 0.0, "sell_volume": 0.0, "weighted_buy_sum": 0.0, "weighted_sell_sum": 0.0})
        for fill in all_fills:
            key = (fill.user_address, fill.asset)
            if fill.is_buy:
                positions_data[key]["buy_volume"] += fill.size; positions_data[key]["weighted_buy_sum"] += fill.size * fill.price
            else:
                positions_data[key]["sell_volume"] += fill.size; positions_data[key]["weighted_sell_sum"] += fill.size * fill.price
        
        processed_positions = []
        for (user, asset), data in positions_data.items():
            net_volume = data["buy_volume"] - data["sell_volume"]
            if abs(net_volume) > 1e-9:
                if net_volume > 0:
                    side = "Long"; avg_price = data["weighted_buy_sum"] / data["buy_volume"] if data["buy_volume"] > 0 else 0
                else:
                    side = "Short"; avg_price = data["weighted_sell_sum"] / data["sell_volume"] if data["sell_volume"] > 0 else 0
                position_value = abs(net_volume) * avg_price
                processed_positions.append({"user": user, "asset": asset, "side": side, "net_volume": net_volume, "avg_price": avg_price, "position_value": position_value})
        
        sorted_positions = sorted(processed_positions, key=lambda p: (p["user"], -p["position_value"]))
        
        table = PrettyTable()
        table.field_names = ["User Address", "Asset", "Side", "Net Volume", "Avg. Entry Price", "Position Value"]
        # ... table formatting ...
        
        # This part remains the same, just populating the table
        current_user = None
        for pos in sorted_positions:
            if pos["user"] != current_user:
                if current_user is not None: table.add_row(['-' * 44, '-' * 8, '-' * 5, '-' * 15, '-' * 18, '-'*18], divider=True)
                display_user = pos["user"]; current_user = pos["user"]
            else: display_user = ""
            table.add_row([display_user, pos["asset"], pos["side"], f"{abs(pos['net_volume']):,.4f}", f"${pos['avg_price']:,.2f}", f"${pos['position_value']:,.2f}"])
        
        print("\n--- Open Positions Analysis (Sorted by Position Value) ---")
        print(table)
        
        if save_image:
            save_table_as_image(table.get_string(), theme=theme)
    finally:
        session.close()

def main():
    """
    Parses command-line arguments to run the correct analysis with options.
    """
    args = sys.argv[1:]
    
    # Check for flags
    save_image_flag = '--save-image' in args
    if save_image_flag:
        args.remove('--save-image')

    theme = 'light'
    if '--theme' in args:
        theme_index = args.index('--theme')
        if theme_index + 1 < len(args):
            theme = args[theme_index + 1].lower()
            # Remove the flag and its value
            args.pop(theme_index)
            args.pop(theme_index)
        else:
            args.remove('--theme') # Remove flag if no value is provided

    # Determine mode
    mode = 'all' # Default mode
    if args:
        mode = args[0].lower()

    if mode == "all":
        analyze_all_history(save_image=save_image_flag, theme=theme)
    elif mode == "open":
        analyze_open_positions(save_image=save_image_flag, theme=theme)
    else:
        print(f"Error: Unknown mode '{mode}'. Please use 'all' or 'open'.")
        print("Usage: python query_example.py [all|open] [--save-image] [--theme dark|light]")

if __name__ == "__main__":
    main()