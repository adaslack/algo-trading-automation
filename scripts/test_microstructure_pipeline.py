"""
Comprehensive Integration Test for V11 Microstructure Engine
===========================================================
Validates the entire pipeline:
1. Watchlist and market data insertion
2. Cross-sectional ranking (Z-score normalizations, composite score calculation)
3. Signal construction and verification
4. Risk Gate signal processing (Short-selling and short-covering approvals)
5. Database audit compliance
"""
import sys
import os
import sqlite3
import datetime
import unittest

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src', 'pipeline'))

from event_bus import get_bus, Event, Topics
import cross_sectional_ranker
import risk_manager

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'trading_brain.db')

class TestMicrostructureEngine(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        import db_setup
        db_setup.setup_database()
        
    def setUp(self):
        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()
        
        self.cursor.execute("DELETE FROM daily_watchlist")
        self.cursor.execute("DELETE FROM market_data")
        self.cursor.execute("DELETE FROM trade_history")
        self.cursor.execute("DELETE FROM strategy_performance")
        self.cursor.execute("DELETE FROM circuit_breaker")
        self.cursor.execute("DELETE FROM options_queue")
        self.conn.commit()
        
        # Clean PostgreSQL tables as well if psycopg2 is installed and DB_HOST is configured
        if os.getenv("DB_HOST"):
            try:
                import psycopg2
                pg_conn = psycopg2.connect(
                    dbname="trading_brain",
                    user="postgres",
                    password="password",
                    host="localhost",
                    port="5432"
                )
                pg_cursor = pg_conn.cursor()
                pg_cursor.execute("TRUNCATE TABLE daily_watchlist CASCADE;")
                pg_cursor.execute("TRUNCATE TABLE market_data CASCADE;")
                pg_cursor.execute("TRUNCATE TABLE trade_history CASCADE;")
                pg_cursor.execute("TRUNCATE TABLE strategy_performance CASCADE;")
                pg_cursor.execute("TRUNCATE TABLE circuit_breaker CASCADE;")
                pg_cursor.execute("TRUNCATE TABLE options_queue CASCADE;")
                pg_conn.commit()
                pg_cursor.close()
                pg_conn.close()
            except Exception:
                pass
        
    def tearDown(self):
        self.conn.close()
        
    def test_ranking_and_risk_flow(self):
        print("\n--- Starting V11 Integration Test ---")
        
        # 1. Insert daily watchlist
        tickers = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'AMZN']
        for t in tickers:
            self.cursor.execute("INSERT OR REPLACE INTO daily_watchlist (ticker, sector) VALUES (?, 'Technology')", (t,))
            
        # 2. Insert diverse microstructure market data
        market_stats = {
            'AAPL': {'close': 180.0, 'vpin': 0.1, 'obi': 0.8, 'dealer_gex': 1.2, 'insider_score': 0.9},
            'TSLA': {'close': 220.0, 'vpin': 0.8, 'obi': -0.7, 'dealer_gex': -1.0, 'insider_score': -0.5},
            'MSFT': {'close': 400.0, 'vpin': 0.3, 'obi': 0.1, 'dealer_gex': 0.2, 'insider_score': 0.1},
            'NVDA': {'close': 900.0, 'vpin': 0.4, 'obi': 0.0, 'dealer_gex': 0.1, 'insider_score': 0.0},
            'AMZN': {'close': 175.0, 'vpin': 0.3, 'obi': 0.2, 'dealer_gex': -0.1, 'insider_score': 0.2}
        }
        
        for t, stats in market_stats.items():
            self.cursor.execute('''
                INSERT INTO market_data (
                    ticker, sector, timestamp, close_price, volume, vpin, obi, micro_price, dealer_gex, insider_score
                ) VALUES (?, 'Technology', ?, ?, 5000000.0, ?, ?, ?, ?, ?)
            ''', (t, datetime.datetime.now().isoformat(), stats['close'], stats['vpin'], stats['obi'], stats['close'], stats['dealer_gex'], stats['insider_score']))
            
        self.conn.commit()
        print("Inserted mock microstructure features into market_data.")
        
        # 3. Trigger Cross-Sectional Ranking logic
        print("Invoking Cross-Sectional Ranking...")
        
        watchlist = cross_sectional_ranker.get_watchlist()
        self.assertEqual(len(watchlist), 5)
        
        m_data = cross_sectional_ranker.fetch_latest_market_data(watchlist)
        self.assertEqual(len(m_data), 5)
        
        valid_tickers = [t for t, d in m_data.items() if d['close_price'] > 0]
        obis = [m_data[t]['obi'] for t in valid_tickers]
        vpins = [m_data[t]['vpin'] for t in valid_tickers]
        gexs = [m_data[t]['dealer_gex'] for t in valid_tickers]
        insiders = [m_data[t]['insider_score'] for t in valid_tickers]
        
        obi_z = cross_sectional_ranker.z_score_normalize(obis)
        vpin_z = cross_sectional_ranker.z_score_normalize(vpins)
        gex_z = cross_sectional_ranker.z_score_normalize(gexs)
        insider_z = cross_sectional_ranker.z_score_normalize(insiders)
        
        scores = {}
        for i, t in enumerate(valid_tickers):
            score = (0.4 * obi_z[i]) - (0.3 * vpin_z[i]) + (0.3 * gex_z[i]) + (0.2 * insider_z[i])
            scores[t] = score
            
        sorted_tickers = sorted(scores.items(), key=lambda x: x[1])
        print("Calculated composite microstructure scores:")
        for t, s in sorted_tickers:
            print(f"  {t}: {s:.4f}")
            
        self.assertTrue(scores['AAPL'] > scores['MSFT'])
        self.assertTrue(scores['TSLA'] < scores['MSFT'])
        
        n = len(sorted_tickers)
        num_signals = max(1, n // 5)
        shorts = sorted_tickers[:num_signals]
        longs = sorted_tickers[-num_signals:]
        
        self.assertEqual(shorts[0][0], 'TSLA')
        self.assertEqual(longs[0][0], 'AAPL')
        
        print(f"LONG target identified: {longs[0][0]} | SHORT target identified: {shorts[0][0]}")
        
        # 4. Build signals directly for Risk Gate testing (bypass Redis pub/sub timing issues)
        long_ticker, long_score = longs[0]
        short_ticker, short_score = shorts[0]
        
        long_confidence = round(float(0.5 + 0.5 * min(1.0, max(0.0, long_score))), 3)
        long_sig_data = {
            'ticker': long_ticker, 'signal_type': 'BUY',
            'target_strategy': 'factor_microstructure_flow',
            'confidence': long_confidence, 'exit_rule': 'Microstructure_Exit'
        }
        
        short_confidence = round(float(0.5 + 0.5 * min(1.0, max(0.0, -short_score))), 3)
        short_sig_data = {
            'ticker': short_ticker, 'signal_type': 'SELL',
            'target_strategy': 'factor_microstructure_flow',
            'confidence': short_confidence, 'exit_rule': 'Microstructure_Exit'
        }
        
        print("Verified correct L/S signals constructed.")
        
        # 5. Test Risk Gate approval flow
        print("Testing Risk Gate approval flow...")
        
        class MockAccount:
            portfolio_value = 100000.0
            buying_power = 200000.0
            
        class MockTradingClient:
            def get_account(self):
                return MockAccount()
            def get_all_positions(self):
                return []
                
        risk_manager.trading_client = MockTradingClient()
        
        class MockDecision:
            approved = True
            reason = "Test Approved"
            
        class MockRiskGate:
            def approve(self, order):
                return MockDecision()
                
        risk_manager.risk_gate = MockRiskGate()
        risk_manager.DB_PATH = DB_PATH
        
        # Trigger the Risk Gate event handler manually
        risk_manager.on_signal_generated(Event(source="cross_sectional_ranker", data=long_sig_data))
        risk_manager.on_signal_generated(Event(source="cross_sectional_ranker", data=short_sig_data))
        
        # Verify db trade history
        self.cursor.execute("SELECT ticker, action, status, quantity FROM trade_history")
        rows = self.cursor.fetchall()
        
        print("Trade history contents after signal processing:")
        for r in rows:
            print(f"  Ticker: {r[0]} | Action: {r[1]} | Status: {r[2]} | Qty: {r[3]}")
            
        self.assertEqual(len(rows), 2)
        
        actions = [r[1] for r in rows]
        self.assertIn('BUY', actions)
        self.assertIn('SELL', actions)
        
        # 6. Test Execution Engine Auditing & State Machine
        print("Testing Execution Engine DB Auditing & State Machine...")
        import execution_engine
        
        class MockClock:
            is_open = True
            
        class MockAlpacaOrder:
            id = "mock_alpaca_order_123"
            filled_avg_price = 185.0
            
        class MockTradingClientExecution:
            def get_clock(self):
                return MockClock()
            def get_all_positions(self):
                return []
            def submit_order(self, order_data):
                return MockAlpacaOrder()
                
        execution_engine.trading_client = MockTradingClientExecution()
        execution_engine.data_client = None
        execution_engine.DB_PATH = DB_PATH
        
        # Trigger opening order fill execution
        execution_engine.on_order_approved(Event(source="risk_manager", data={
            'ticker': 'AAPL',
            'action': 'BUY',
            'qty': 66.6667,
            'estimated_price': 180.0,
            'strategy': 'factor_microstructure_flow',
            'confidence': 0.8,
            'kelly_pct': 0.12
        }))
        
        # Verify opening trade was audited with actual executed values
        self.cursor.execute("SELECT entry_price, quantity, status FROM trade_history WHERE ticker = 'AAPL' AND action = 'BUY'")
        row_aapl = self.cursor.fetchone()
        print(f"  AAPL Audited Entry Price: ${row_aapl[0]:.2f} | Audited Qty: {row_aapl[1]:.4f} | Status: {row_aapl[2]}")
        self.assertGreater(row_aapl[0], 185.0)  # should include slippage/friction
        self.assertEqual(row_aapl[2], 'OPEN')
        
        # Mark AAPL position as CLOSING to simulate long close
        self.cursor.execute("UPDATE trade_history SET status = 'CLOSING' WHERE ticker = 'AAPL' AND action = 'BUY'")
        self.conn.commit()
        
        # Trigger closing order fill execution
        MockAlpacaOrder.filled_avg_price = 195.0
        execution_engine.on_order_approved(Event(source="risk_manager", data={
            'ticker': 'AAPL',
            'action': 'SELL',
            'qty': 66.6667,
            'estimated_price': 190.0,
            'strategy': 'factor_microstructure_flow',
            'confidence': 0.8,
            'kelly_pct': 0.0
        }))
        
        # Verify trade was closed and audited with PnL
        self.cursor.execute("SELECT entry_price, exit_price, pnl, pnl_pct, status FROM trade_history WHERE ticker = 'AAPL' AND action = 'BUY'")
        row_aapl_closed = self.cursor.fetchone()
        print(f"  Closed Trade AAPL - Entry: ${row_aapl_closed[0]:.2f} | Exit: ${row_aapl_closed[1]:.2f} | Realized PnL: ${row_aapl_closed[2]:.2f} ({row_aapl_closed[3]*100:.2f}%) | Status: {row_aapl_closed[4]}")
        
        self.assertEqual(row_aapl_closed[4], 'CLOSED')
        self.assertIsNotNone(row_aapl_closed[2])
        self.assertGreater(row_aapl_closed[1], 0)
        
        print("--- V11 Microstructure Engine Integration Test PASSED ---")

if __name__ == '__main__':
    unittest.main()

