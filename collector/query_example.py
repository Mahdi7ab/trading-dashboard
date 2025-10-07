# query_example.py

from sqlalchemy import func
from database import SessionLocal, Fill
from prettytable import PrettyTable

def analyze_and_display_data():
    print("🔍 Analyzing data, grouped by Asset and Side...")
    
    session = SessionLocal()
    
    try:
        # Query to group by asset and side.
        # It calculates the number of trades and the total volume for each group.
        results = (
            session.query(
                Fill.asset,
                Fill.is_buy,
                func.count(Fill.id).label("trade_count"),
                func.sum(Fill.size).label("total_volume")
            )
            .group_by(Fill.asset, Fill.is_buy)
            .order_by(Fill.asset, Fill.is_buy.desc()) # Sort by asset name, then side
            .all()
        )
        
        if not results:
            print("No data found to display.")
            return

        # Create the table with new headers
        table = PrettyTable()
        table.field_names = ["Asset", "Side", "Trade Count", "Total Volume"]
        table.align = "l"
        table.align["Trade Count"] = "r"
        table.align["Total Volume"] = "r"
        
        # Add the query results to the table
        for asset, is_buy, trade_count, total_volume in results:
            side = "Buy" if is_buy else "Sell"
            table.add_row([
                asset,
                side,
                trade_count,
                f"{total_volume:,.4f}" # Format volume with commas and 4 decimal places
            ])

        print(table)
            
    except Exception as e:
        print(f"An error occurred during analysis: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    analyze_and_display_data()