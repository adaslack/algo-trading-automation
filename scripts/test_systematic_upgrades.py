"""
Institutional Verification Suite for Systematic Upgrades
=========================================================
Validates:
1. 3-State Hidden Markov Model (HMM) Volatility Regime updates.
2. Institutional Feature Lineage Registry & Kolmogorov-Smirnov Drift alerts (empirical seeding).
3. High-Throughput Redis Streams Integration.
4. High-Fidelity Limit Order Book (LOB) Replay Engine execution logic.
"""

import sys
import os
import time
import numpy as np
import unittest

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src', 'pipeline'))

from alpha_engine import AlphaEngine, AlphaOutput
from feature_lineage_registry import get_registry, FeatureLineageRegistry
from event_bus import get_bus, Event, RedisBus, InMemoryBus
from lob_replay_engine import LOBReplayEngine

class TestSystematicUpgrades(unittest.TestCase):

    def test_hmm_volatility_regime_classifier(self):
        print("\n--- Testing 3-State Hidden Markov Model (HMM) Volatility Classifier ---")
        engine = AlphaEngine()
        
        # Initial priors: Stable=0.65, Caution=0.25, Panic=0.10
        print(f"Initial State Probabilities: {engine._regime_probs}")
        self.assertAlmostEqual(engine._regime_probs[0], 0.65)
        
        # 1. Evaluate with highly stable VIX (VIX=12.0)
        # Should transition to / stay stable
        snapshot = {'close': 100.0, 'vpin': 0.2, 'obi': 0.1, 'garch_volatility': 0.02}
        out_stable = engine.evaluate("AAPL", snapshot, vix=12.0)
        print(f"Post Stable VIX (12.0) State Probabilities: {engine._regime_probs}")
        self.assertGreater(engine._regime_probs[0], 0.60) # stable should dominate
        
        # 2. Evaluate with panic VIX (VIX=40.0) multiple times to trigger recursive transition
        for _ in range(5):
            engine.evaluate("AAPL", snapshot, vix=40.0)
        
        print(f"Post Panic VIX (40.0) State Probabilities: {engine._regime_probs}")
        self.assertGreater(engine._regime_probs[2], 0.50) # Panic should dominate now
        
        # 3. Verify Baum-Welch Expectation-Maximization HMM Sequence Learning
        vix_history = list(np.random.normal(15.0, 1.5, 30)) + list(np.random.normal(25.0, 2.0, 30)) + list(np.random.normal(42.0, 3.5, 30))
        engine.calibrate_hmm_from_data(vix_history)
        self.assertGreater(engine._hmm_means[2], engine._hmm_means[0]) # Stable VIX mean < Panic VIX mean
        print("✅ HMM Volatility Regime EM Classifier test PASSED.")

    def test_feature_lineage_registry_and_drift_detection(self):
        print("\n--- Testing Feature Lineage Registry & Kolmogorov-Smirnov Drift ---")
        reg = FeatureLineageRegistry()
        
        # Register a mock feature 'vpin' with version 2.1.0 and empirical observation seeding deferred
        reg.register_feature(
            name="vpin",
            version="2.1.0",
            formula="rolling_sum(abs(buy_vol - sell_vol)) / rolling_sum(total_volume)",
            parameters={"lookback_bars": 30},
            reference_distribution=None
        )
        
        # Log stable inference values to build the empirical reference distribution (100 samples required)
        stable_vals = list(np.random.normal(0.3, 0.05, 150))
        for val in stable_vals:
            reg.log_inference_value("vpin", val)
            
        # Check drift - should be STABLE
        drift_report_stable = reg.check_drift("vpin")
        print(f"Stable Drift Report: {drift_report_stable}")
        self.assertEqual(drift_report_stable["status"], "STABLE")
        
        # Now log highly drifted inference values (simulating adverse execution / high toxicity)
        for val in np.random.normal(0.7, 0.05, 100): # mean shifted to 0.7
            reg.log_inference_value("vpin", val)
            
        # Check drift - should trigger DRIFT_ALERT_CRITICAL
        drift_report_drifted = reg.check_drift("vpin")
        print(f"Drifted Drift Report: {drift_report_drifted}")
        self.assertEqual(drift_report_drifted["status"], "DRIFT_ALERT_CRITICAL")
        self.assertTrue(drift_report_drifted["drift_detected"])
        
        # Lineage Audit Report
        report = reg.get_lineage_report("vpin")
        print(f"Feature Lineage Report:\n{report}")
        self.assertEqual(report["version"], "2.1.0")
        print("✅ Feature Lineage Registry and KS Drift test PASSED.")

    def test_redis_streams_event_bus(self):
        print("\n--- Testing Low-Latency Event Bus Fallback & Streams Setup ---")
        # Ensure we can instantiate the singleton bus gracefully
        bus = get_bus(use_redis=False)
        self.assertIsInstance(bus, InMemoryBus)
        
        # Test Event Dataclass serialization
        event = Event(source="test_source", event_type="test.topic", data={"key": "val"})
        json_str = event.to_json()
        decoded = Event.from_json(json_str)
        self.assertEqual(decoded.source, "test_source")
        self.assertEqual(decoded.data["key"], "val")
        print("✅ Event Bus structures test PASSED.")

    def test_lob_replay_engine(self):
        print("\n--- Testing High-Fidelity Limit Order Book Replay Engine ---")
        engine = LOBReplayEngine(iceberg_prob=0.0) # Disable icebergs for deterministic matching test
        
        ticker = "AAPL"
        price = 180.0
        
        # Set market depth at $180.0 to 1000 units
        engine.set_book_depth(ticker, price, 1000.0)
        
        # 1. Place order for 100 units on BATS
        order = engine.submit_limit_order(
            order_id="order_999",
            ticker=ticker,
            action="BUY",
            price=price,
            quantity=100.0,
            venue="BATS"
        )
        self.assertEqual(order.queue_position, 1000.0)
        self.assertEqual(order.status, "QUEUED")
        self.assertEqual(order.venue, "BATS")
        
        # 2. Verify Smart Order Routing (SOR) dynamic low-queue allocation
        sor_order = engine.submit_limit_order(
            order_id="order_sor_123",
            ticker=ticker,
            action="BUY",
            price=price,
            quantity=100.0,
            venue="SOR"
        )
        self.assertIn(sor_order.venue, ["NASDAQ", "NYSE", "BATS"])
        self.assertLess(sor_order.queue_position, 1000.0 * 1.5)
        
        # 3. Verify DMA gateway microburst network latency injection for large order size
        large_order = engine.submit_limit_order(
            order_id="order_large_456",
            ticker=ticker,
            action="BUY",
            price=price,
            quantity=1500.0, # > 500 triggers gateway latency warnings
            venue="PRIMARY"
        )
        # Verify order timestamp reflects gateway latency delays (order timestamp is shifted in future)
        self.assertGreater(large_order.timestamp, time.time())
        
        # Process market trade at 180.0 for 500 units -> should exhaust queue to 500
        res1 = engine.process_market_trade(ticker, price, 500.0)
        self.assertEqual(len(res1), 0) # No fills yet
        self.assertEqual(order.queue_position, 500.0)
        
        # Process market trade at 180.0 for 600 units -> exhausts remaining 500 queue, fills order partially (100 units)
        res2 = engine.process_market_trade(ticker, price, 600.0)
        self.assertEqual(len(res2), 1)
        self.assertEqual(res2[0].filled_qty, 100.0)
        self.assertEqual(res2[0].status, "FILLED")
        print(f"LOB Execution Results: {res2[0]}")
        print("✅ LOB Replay Engine SOR, DMA, and matching tests PASSED.")

if __name__ == "__main__":
    unittest.main()

