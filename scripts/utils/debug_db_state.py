import sqlite3
import os
import json

DB_PATH = os.path.join("data", "trading_brain.db")

def main():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database not found at {DB_PATH}")
        return

    print("=== CONNECTING TO DATABASE ===")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 1. Active Watchlist & Features
    print("\n--- Fresh Technical Features (Last 5 Updates) ---")
    cursor.execute("""
        SELECT ticker, close_price, adx, hurst_exponent, volume_ratio, sentiment_score, timestamp 
        FROM market_data 
        WHERE close_price > 0 
        ORDER BY timestamp DESC 
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"Ticker: {row[0]} | Price: {row[1]:.2f} | ADX: {row[2]} | Hurst: {row[3]} | VolRatio: {row[4]} | Sentiment: {row[5]} | Time: {row[6]}")

    # 2. AI Strategy Selector Predictions
    print("\n--- AI Selector Predictions (Last 5 Predictions) ---")
    cursor.execute("""
        SELECT ticker, recommended_category, recommended_strategy, confidence, timestamp 
        FROM ai_selector_predictions 
        ORDER BY timestamp DESC 
        LIMIT 5
    """)
    for row in cursor.fetchall():
        print(f"Ticker: {row[0]} | Category: {row[1]} | Strategy: {row[2]} | Conf: {row[3]:.2f} | Time: {row[4]}")

    # 3. Strategy Weights
    print("\n--- Strategy Weights ---")
    cursor.execute("SELECT strategy, weight FROM strategy_weights")
    for row in cursor.fetchall():
        print(f"Strategy: {row[0]:<20} | Weight: {row[1]:.4f}")

    # 4. Check for any generated signals
    print("\n--- Recent Signal Logs ---")
    try:
        cursor.execute("SELECT ticker, signal_type, target_strategy, confidence, timestamp FROM strategy_performance ORDER BY timestamp DESC LIMIT 5")
        rows = cursor.fetchall()
        if not rows:
            print("No signals found in strategy_performance.")
        for row in rows:
            print(f"Ticker: {row[0]} | Signal: {row[1]} | Strategy: {row[2]} | Conf: {row[3]:.2f} | Time: {row[4]}")
    except sqlite3.OperationalError:
        print("Table 'strategy_performance' or similar does not have the expected schema or doesn't exist yet.")

    conn.close()

if __name__ == "__main__":
    main()
