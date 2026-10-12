"""
Integration Test for Phase 5 News Breakthrough Sentiment Engine
==============================================================
Validates the end-to-end news trading flow:
1. Ingestion of mock news headline.
2. Alias matching to watchlist symbol (NVIDIA -> NVDA).
3. Sentiment scoring using NLTK VADER.
4. Validation via volume ratio breakout query from database.
5. Signal construction and risk manager trade routing.
"""
import sys
import os
import sqlite3
import datetime
import unittest

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src', 'pipeline'))

from event_bus import get_bus, Event, Topics
import sentiment_engine
import risk_manager

from paths import DB_PATH

class TestNewsSentimentPipeline(unittest.TestCase):

    def setUp(self):
        # 1. Setup fresh SQLite tables
        self.conn = sqlite3.connect(DB_PATH, timeout=5)
        self.cursor = self.conn.cursor()
        
        # Enable WAL mode for concurrent testing stability
        self.conn.execute('PRAGMA journal_mode=WAL')
        
        # Clean existing test data
        self.cursor.execute("DELETE FROM daily_watchlist")
        self.cursor.execute("DELETE FROM market_data")
        self.cursor.execute("DELETE FROM trade_history")
        self.cursor.execute("DELETE FROM portfolio_risk")
        self.cursor.execute("DELETE FROM circuit_breaker")
        
        # Insert mock watchlist and market volume data
        self.cursor.execute("INSERT INTO daily_watchlist (ticker, sector) VALUES ('NVDA', 'Technology')")
        
        # Insert market data with a high volume ratio (2.5) to trigger the breakout validation rule
        self.cursor.execute('''
            INSERT INTO market_data (
                ticker, timestamp, close_price, open_price, high_price, low_price, volume, volume_ratio
            ) VALUES ('NVDA', ?, 150.0, 149.0, 151.0, 148.0, 5000000, 2.5)
        ''', (datetime.datetime.now().isoformat(),))
        
        # Initialize default portfolio limits
        self.conn.commit()

        # Wire handlers together
        sentiment_engine.DB_PATH = DB_PATH
        risk_manager.DB_PATH = DB_PATH
        
        # Create a mock broker to simulate risk manager connections
        class MockBroker:
            name = "Interactive Brokers"
            def is_connected(self):
                return True
                
        risk_manager.broker = MockBroker()

        # Clear event bus subscribers by forcing a fresh InMemoryBus
        import event_bus
        event_bus._bus_instance = None
        bus = get_bus(use_redis=False)
        bus._subscribers.clear()
        
        # Override module level event bus references
        risk_manager.event_bus = bus
        
        # Subscribe modules' handlers manually to bypass infinite loops
        bus.subscribe(Topics.NEWS_ARRIVED, sentiment_engine.on_news_arrived)
        bus.subscribe(Topics.NEWS_SENTIMENT_ALERT, risk_manager.on_news_sentiment_alert)
        bus.subscribe(Topics.SIGNAL_GENERATED, risk_manager.on_signal_generated)

    def tearDown(self):
        self.conn.close()

    def test_end_to_end_news_breakthrough(self):
        print("\n--- Starting V15 News Sentiment Integration Test ---")
        bus = get_bus(use_redis=False)
        
        # Capture approved orders from the event bus
        approved_orders = []
        def capture_order_approved(event):
            approved_orders.append(event.data)
            
        bus.subscribe(Topics.ORDER_APPROVED, capture_order_approved)
        
        # 1. Trigger news arrival event on bus
        # Headline contains Jensen, Nvidia, and Trillion which have custom boosted positive weights
        headline = "Jensen Huang announces Nvidia Blackwell chips reach 1 Trillion sales milestone"
        summary = "The chipmaker shares surged following the keynote announcement."
        
        print(f"Ingesting mock news headline: '{headline}'")
        bus.publish(Topics.NEWS_ARRIVED, Event(
            source="test_rss_feed",
            data={
                "title": headline,
                "summary": summary,
                "url": "https://example.com/nvidia-surge",
                "published_at": datetime.datetime.now().isoformat()
            }
        ))
        
        # Wait a short moment for event loop propagation
        import time
        time.sleep(0.5)
        
        # 2. Assertions
        # Verify that the order was validated, approved, and logged to trade history
        self.cursor.execute("SELECT ticker, action, quantity, strategy, status FROM trade_history")
        rows = self.cursor.fetchall()
        
        print("Trade history contents after news processing:")
        for r in rows:
            print(f"  Ticker: {r[0]} | Action: {r[1]} | Qty: {r[2]:.4f} | Strategy: {r[3]} | Status: {r[4]}")
            
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], 'NVDA')
        self.assertEqual(rows[0][1], 'BUY')
        self.assertEqual(rows[0][3], 'news_breakthrough_momentum')
        self.assertEqual(rows[0][4], 'OPEN')
        
        # Verify event bus transmitted the order approved event
        self.assertEqual(len(approved_orders), 1)
        self.assertEqual(approved_orders[0]['ticker'], 'NVDA')
        self.assertEqual(approved_orders[0]['action'], 'BUY')
        self.assertEqual(approved_orders[0]['strategy'], 'news_breakthrough_momentum')
        
        print("✅ V15 News Sentiment Integration Test PASSED")

if __name__ == '__main__':
    unittest.main()
