# collector/analysis_logic.py

import requests
from collections import defaultdict
from sqlalchemy import func
from database import Fill
from config import API_URL, HEADERS

def get_open_positions(session, fills_query=None):
    """
    پوزیشن‌های باز را بر اساس یک کوئری fills خاص محاسبه می‌کند.
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

def aggregate_sentiment(processed_positions, weights_map=None):
    """
    لیست پوزیشن‌های باز را به سنتیمنت تجمیعی تبدیل می‌کند.
    """
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

def get_market_context():
    """
    داده‌های متا و تغییرات ۲۴ ساعته را از API HyperLiquid می‌گیرد.
    """
    print("Fetching market context from API...")
    try:
        payload = {"type": "meta"}
        response = requests.post(API_URL, headers=HEADERS, json=payload, timeout=10)
        response.raise_for_status()
        
        context_map = {}
        for asset_data in response.json().get('universe', []):
            try:
                asset_name = asset_data.get('name')
                prev_px_str = asset_data.get('dayNtlVlm', {}).get('24h')
                mark_px_str = asset_data.get('markPx')

                if asset_name and prev_px_str and mark_px_str:
                    mark_px = float(mark_px_str)
                    prev_px = float(prev_px_str)
                    if prev_px > 0:
                        change_percent = ((mark_px - prev_px) / prev_px) * 100
                        context_map[asset_name] = change_percent
            except (ValueError, TypeError):
                continue
        return context_map
    except Exception as e:
        print(f"❌ Error fetching market context: {e}")
        return {}