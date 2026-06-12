"""
Comprehensive Test Suite for Research Core Engine
=================================================
Validates all institutional components:
1. Deterministic Tick Replay Engine with adverse selection and fill latency models.
2. Transaction-Cost-Aware Portfolio Optimizer under square-root market impact constraints.
3. Dynamic Covariance Forecaster capturing regime-switching volatilities.
4. Live-vs-Backtest Sharpe Drift Tracking with Information Coefficient alerts.
5. Alpha Cemetery auto-retirement protocols.
"""
import sys
import os
import unittest
import numpy as np

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src', 'pipeline'))

import research_core
import db_setup

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'trading_brain.db')

class TestResearchEngine(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Reset DB using db_setup
        db_setup.setup_database()
        
    def test_tick_replay_engine(self):
        print("\n--- Testing Tick Replay Engine (High vs Low Toxicity) ---")
        engine = research_core.TickReplayEngine(ticker='AAPL')
        
        # Test 1: Limit price far from market price (No fill)
        res_no_fill = engine.simulate_order_fill(price=180.0, side='BUY', target_price=175.0, tick_volume=100)
        self.assertFalse(res_no_fill['filled'])
        
        # Test 2: Limit price crossing spread under low toxicity
        res_low_toxic = engine.simulate_order_fill(price=180.0, side='BUY', target_price=181.0, tick_volume=100000, toxic_vpin=0.1)
        self.assertTrue(res_low_toxic['filled'])
        self.assertEqual(res_low_toxic['slippage_bps'], 0.0)
        self.assertTrue(res_low_toxic['latency_ms'] > 0)
        
        # Test 3: Limit price crossing spread under high toxicity
        res_high_toxic = engine.simulate_order_fill(price=180.0, side='BUY', target_price=181.0, tick_volume=100000, toxic_vpin=0.8)
        self.assertTrue(res_high_toxic['filled'])
        self.assertTrue(res_high_toxic['slippage_bps'] > 0.0)
        print(f"Low toxicity fill slippage: {res_low_toxic['slippage_bps']} bps")
        print(f"High toxicity fill slippage: {res_high_toxic['slippage_bps']:.2f} bps | Price: ${res_high_toxic['fill_price']:.2f}")

    def test_transaction_cost_optimizer(self):
        print("\n--- Testing Transaction-Cost-Aware Optimizer & Capacity Curve ---")
        optimizer = research_core.TransactionCostOptimizer(risk_aversion=1.0, turnover_penalty=0.0005, impact_scaling=0.5)
        
        # Mock 3 assets: AAPL, MSFT, TSLA
        alphas = np.array([0.05, 0.02, -0.04]) # Alpha signals
        current_weights = np.array([0.0, 0.0, 0.0]) # No initial positions
        cov_matrix = np.array([
            [0.0004, 0.0001, 0.0],
            [0.0001, 0.0003, 0.0],
            [0.0,    0.0,    0.0009]
        ])
        daily_volumes = np.array([5000000.0, 3000000.0, 2000000.0])
        price_estimates = np.array([180.0, 400.0, 220.0])
        
        # 1. Optimize portfolio weights under $10,000,000 AUM
        weights = optimizer.optimize(alphas, current_weights, cov_matrix, daily_volumes, price_estimates, aum=10000000.0)
        print(f"Optimal weights: {weights}")
        
        # Ensure beta-neutrality: sum of weights = 0.0 (within precision bounds)
        self.assertAlmostEqual(np.sum(weights), 0.0, places=4)
        
        # Positive alpha assets (AAPL) should have positive weights, negative (TSLA) should have negative weights
        self.assertTrue(weights[0] > 0)
        self.assertTrue(weights[2] < 0)
        
        # 2. Generate capacity curve to observe return decay as AUM scales
        capacity_df = optimizer.generate_capacity_curve(alphas, cov_matrix, daily_volumes, price_estimates)
        print("\nGenerated Portfolio Capacity Curve:")
        print(capacity_df.to_string(index=False))
        
        # Realized returns should drop as AUM increases due to rising participation/market impact
        high_aum_realized = capacity_df.iloc[-1]['Realized_Return']
        low_aum_realized = capacity_df.iloc[0]['Realized_Return']
        self.assertTrue(high_aum_realized < low_aum_realized)

    def test_dynamic_covariance_forecaster(self):
        print("\n--- Testing Dynamic Covariance Forecaster ---")
        forecaster = research_core.DynamicCovarianceForecaster(num_assets=3, decay=0.94)
        
        # Generate baseline covariance
        initial_cov = np.copy(forecaster.cov_matrix)
        
        # Simulate high-volatility returns update
        heavy_stress_returns = [0.08, -0.07, 0.09]
        updated_cov = forecaster.update_covariance(heavy_stress_returns)
        
        # Volatilities and covariances should increase significantly due to clustered returns
        self.assertTrue(updated_cov[0, 0] > initial_cov[0, 0])
        self.assertTrue(updated_cov[2, 2] > initial_cov[2, 2])
        print("Dynamic Covariance successfully adapted to toxic stress returns.")

    def test_live_backtest_divergence_engine(self):
        print("\n--- Testing Live-vs-Backtest Sharpe Drift Engine ---")
        engine = research_core.LiveBacktestDivergenceEngine()
        
        for _ in range(10):
            engine.record_prediction('factor_momentum', 2.0)
            engine.record_realized_trade('factor_momentum', -0.02)
            
        metrics = engine.calculate_sharpe_drift('factor_momentum')
        print(f"MACD Sharpe Drift Report: {metrics}")
        
        # Drift should be positive and trigger decay collapse alerts
        self.assertTrue(metrics['drift'] > 1.5)
        self.assertEqual(metrics['status'], 'CRITICAL_ALPHA_COLLAPSE')

    def test_alpha_cemetery(self):
        print("\n--- Testing Alpha Cemetery & Graveyard Audits ---")
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Ensure Williams_R is pre-inserted in weights and metrics
        cursor.execute("INSERT OR IGNORE INTO strategy_weights (strategy, weight) VALUES ('Williams_R', 0.25)")
        cursor.execute("INSERT OR IGNORE INTO strategy_metrics (strategy) VALUES ('Williams_R')")
        
        # Mock decaying strategy metrics
        cursor.execute("UPDATE strategy_metrics SET sharpe_ratio = 0.1, win_rate = 0.38, total_trades = 20 WHERE strategy = 'Williams_R'")
        conn.commit()
        conn.close()
        
        cemetery = research_core.AlphaCemetery(db_path=DB_PATH)
        retired = cemetery.audit_and_retire_strategies()
        
        print(f"Retired strategies: {retired}")
        self.assertIn('Williams_R', retired)
        
        # Verify db status
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT weight FROM strategy_weights WHERE strategy = 'Williams_R'")
        w = cursor.fetchone()[0]
        cursor.execute("SELECT total_trades FROM strategy_metrics WHERE strategy = 'Williams_R'")
        t = cursor.fetchone()[0]
        conn.close()
        
        self.assertEqual(w, 0.0)
        self.assertEqual(t, -1)
        print("Williams_R successfully retired to the Graveyard.")
        
    def test_statistical_defense_court(self):
        print("\n--- Testing V11 Statistical Defense Court (DSR & WRC) ---")
        court = research_core.StatisticalDefenseCourt(db_path=DB_PATH)
        
        np.random.seed(42)
        
        good_returns = np.random.normal(0.002, 0.01, 100)
        dsr_good = court.calculate_dsr(good_returns, trials_count=10)
        print(f"Genuine Alpha DSR Report: {dsr_good}")
        self.assertTrue(dsr_good['observed_sharpe'] > 0)
        
        noisy_returns = np.random.normal(0.0, 0.03, 100)
        dsr_noise = court.calculate_dsr(noisy_returns, trials_count=50)
        print(f"Overfitted Noise DSR Report: {dsr_noise}")
        self.assertEqual(dsr_noise['status'], 'REJECT_OVERFIT')
        
        wrc_report = court.run_whites_reality_check(good_returns, num_bootstraps=100)
        print(f"White's Reality Check Report: {wrc_report}")
        self.assertTrue('wrc_p_value' in wrc_report)

if __name__ == '__main__':
    unittest.main()

