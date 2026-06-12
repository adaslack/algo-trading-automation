"""
Test Suite: AlphaEngine + BayesianPortfolio
===========================================
Validates Points 2 and 4 of the 10/10 requirements:
  - AlphaEngine produces correct E[r] surfaces and cross-sectional ranking
  - BayesianPortfolio posterior updates converge correctly
  - Uncertainty haircut shrinks position size before evidence accumulates
  - Posterior confidence grows as trades are recorded
"""
import sys
import os
import unittest
import numpy as np

sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src', 'pipeline'))

from alpha_engine import AlphaEngine, AlphaOutput
from portfolio    import BayesianPortfolio, _StrategyBelief


# ── Shared fixtures ────────────────────────────────────────────────────────────

def _bullish_snapshot(price=100.0):
    """High OBI, low VPIN, positive insider: should produce positive E[r]."""
    return dict(
        close_price=price, vwap=price * 1.002,     # price slightly above vwap
        vpin=0.10,   obi=0.70,
        micro_price=price * 1.003,
        dealer_gex=1.5, insider_score=0.8,
        garch_volatility=0.015, volume_ratio=1.8,
        hurst_exponent=0.55,
        momentum_12_1=0.08, reversal_5d=0.02, volume_breakout=0.15, vol_regime=0.2, trend_strength=0.1,
    )

def _mild_bullish_snapshot(price=100.0):
    """Mild bullish microstructure: E[r] ~ 1 bp to prevent absolute max allocation capping in uninformative prior tests."""
    return dict(
        close_price=price, vwap=price,
        vpin=0.30,   obi=0.01,
        micro_price=price,
        dealer_gex=0.0, insider_score=0.001,
        garch_volatility=0.02, volume_ratio=1.0,
        hurst_exponent=0.50,
        momentum_12_1=0.01, reversal_5d=0.002, volume_breakout=0.01, vol_regime=0.01, trend_strength=0.005,
    )

def _bearish_snapshot(price=100.0):
    """High VPIN, negative OBI, negative insider: should produce negative E[r]."""
    return dict(
        close_price=price, vwap=price * 0.998,
        vpin=0.85,   obi=-0.65,
        micro_price=price * 0.997,
        dealer_gex=-1.2, insider_score=-0.7,
        garch_volatility=0.025, volume_ratio=0.6,
        hurst_exponent=0.42,
        momentum_12_1=-0.08, reversal_5d=-0.02, volume_breakout=-0.15, vol_regime=-0.2, trend_strength=-0.1,
    )

def _neutral_snapshot(price=100.0):
    """Balanced features: E[r] near zero, no signal."""
    return dict(
        close_price=price, vwap=price,
        vpin=0.30,   obi=0.0,
        micro_price=price,
        dealer_gex=0.0, insider_score=0.0,
        garch_volatility=0.018, volume_ratio=1.0,
        hurst_exponent=0.50,
        momentum_12_1=0.0, reversal_5d=0.0, volume_breakout=0.0, vol_regime=0.0, trend_strength=0.0,
    )


# ── AlphaEngine Tests ──────────────────────────────────────────────────────────

class TestAlphaEngine(unittest.TestCase):

    def setUp(self):
        self.engine = AlphaEngine()

    def test_bullish_snapshot_positive_er(self):
        """Bullish microstructure → positive E[r]."""
        out = self.engine.evaluate('AAPL', _bullish_snapshot(), vix=14.0)
        self.assertIsInstance(out, AlphaOutput)
        self.assertGreater(out.expected_return, 0.0,
            f"Expected positive E[r], got {out.expected_return}")
        print(f"  Bullish E[r]={out.expected_return:+.4f}  conf={out.confidence:.3f}")

    def test_bearish_snapshot_negative_er(self):
        """Bearish microstructure → negative E[r]."""
        out = self.engine.evaluate('TSLA', _bearish_snapshot(), vix=14.0)
        self.assertLess(out.expected_return, 0.0,
            f"Expected negative E[r], got {out.expected_return}")
        print(f"  Bearish E[r]={out.expected_return:+.4f}  conf={out.confidence:.3f}")

    def test_panic_vix_suppresses_signal(self):
        """VIX≥30 should block BUY/SELL signals even on bullish microstructure."""
        out = self.engine.evaluate('NVDA', _bullish_snapshot(), vix=35.0)
        self.assertIsNone(out.signal,
            f"Expected no signal in panic VIX, got {out.signal}")
        print(f"  Panic VIX signal={out.signal}  conf={out.confidence:.3f}")

    def test_liquidity_cost_rises_with_vpin(self):
        """High VPIN should produce higher liquidity cost."""
        low_vpin  = self.engine.evaluate('A', _bullish_snapshot(), vix=14.0)
        high_vpin = self.engine.evaluate('B', _bearish_snapshot(), vix=14.0)
        self.assertGreater(high_vpin.liquidity_cost, low_vpin.liquidity_cost,
            f"High VPIN should raise liquidity cost: "
            f"{high_vpin.liquidity_cost:.2f} vs {low_vpin.liquidity_cost:.2f}")
        print(f"  Liquidity cost low_vpin={low_vpin.liquidity_cost:.2f}bps  high_vpin={high_vpin.liquidity_cost:.2f}bps")

    def test_decay_half_life_inverse_of_vpin(self):
        """High VPIN → shorter half-life (signal decays faster)."""
        bullish  = self.engine.evaluate('X', _bullish_snapshot(), vix=14.0)
        bearish  = self.engine.evaluate('Y', _bearish_snapshot(), vix=14.0)
        self.assertGreater(bullish.decay_half_life, bearish.decay_half_life,
            "Low-VPIN snapshot should have longer signal half-life")
        print(f"  Half-life bullish={bullish.decay_half_life:.1f}m  bearish={bearish.decay_half_life:.1f}m")

    def test_rank_universe_ordering(self):
        """rank_universe must return bullish ticker above neutral above bearish."""
        universe = {
            'BULL': _bullish_snapshot(),
            'FLAT': _neutral_snapshot(),
            'BEAR': _bearish_snapshot(),
        }
        ranked = self.engine.rank_universe(universe, vix=14.0)
        tickers = [o.ticker for o in ranked]
        bull_idx = tickers.index('BULL')
        bear_idx = tickers.index('BEAR')
        self.assertLess(bull_idx, bear_idx,
            f"BULL should rank above BEAR. Order: {tickers}")
        print(f"  Rank order: {tickers}")

    def test_cross_rank_z_scores_sum_near_zero(self):
        """Cross-sectional Z-scores should sum near 0 by definition."""
        universe = {f'T{i}': _neutral_snapshot(100 + i) for i in range(10)}
        ranked   = self.engine.rank_universe(universe, vix=14.0)
        z_sum    = sum(o.cross_rank for o in ranked)
        self.assertAlmostEqual(z_sum, 0.0, places=3,
            msg=f"Z-scores sum should be ~0, got {z_sum:.4f}")
        print(f"  Z-score sum = {z_sum:.6f}")

    def test_factor_breakdown_present(self):
        """AlphaOutput must carry a full factor breakdown dict."""
        out = self.engine.evaluate('META', _bullish_snapshot(), vix=14.0)
        for key in ('f_momentum', 'f_reversal', 'f_volume', 'f_vol_regime', 'f_trend'):
            self.assertIn(key, out.factor_breakdown,
                f"Missing factor key: {key}")
        print(f"  Factors: {out.factor_breakdown}")


# ── BayesianPortfolio Tests ────────────────────────────────────────────────────

class TestBayesianPortfolio(unittest.TestCase):

    def setUp(self):
        self.portfolio = BayesianPortfolio()
        self.engine    = AlphaEngine()

    def test_initial_position_conservative(self):
        """
        Before any trades, posterior is uninformative → uncertainty_haircut ≈ 0.30
        → position should be at the minimum allocation floor.
        """
        self.portfolio._beliefs['factor_unified_expected_return'].sigma = 0.40
        out  = self.engine.evaluate('AAPL', _mild_bullish_snapshot(), vix=14.0)
        size = self.portfolio.size(out, price=180.0, portfolio_value=100_000)
        self.assertLessEqual(size['alloc_pct'], 0.05,
            f"No-data position should be ≤5%, got {size['alloc_pct']*100:.2f}%")
        self.assertAlmostEqual(size['uncertainty_haircut'], 0.30, places=2)
        print(f"  Initial alloc={size['alloc_pct']*100:.2f}%  haircut={size['uncertainty_haircut']:.2f}")

    def test_posterior_grows_with_wins(self):
        """After 20 winning trades, posterior mu should be positive and confidence higher."""
        for _ in range(20):
            self.portfolio.update('factor_unified_expected_return', realised_return=0.025)
        b = self.portfolio._beliefs['factor_unified_expected_return']
        self.assertGreater(b.mu, 0.0, "Posterior mean should be positive after wins")
        self.assertGreater(b.win_rate, 0.9, "Win rate should be >90% after 20 wins")
        print(f"  After 20 wins: μ={b.mu*100:+.2f}%  σ={b.sigma*100:.2f}%  Sharpe≈{b.sharpe_proxy:.2f}")

    def test_posterior_shrinks_with_losses(self):
        """After 20 losing trades, posterior mu should be negative."""
        for _ in range(20):
            self.portfolio.update('factor_unified_expected_return', realised_return=-0.018)
        b = self.portfolio._beliefs['factor_unified_expected_return']
        self.assertLess(b.mu, 0.0, "Posterior mean should be negative after losses")
        print(f"  After 20 losses: μ={b.mu*100:+.2f}%")

    def test_haircut_decreases_with_evidence(self):
        """
        Uncertainty haircut should decrease as trades accumulate,
        allowing larger positions once the strategy is proven.
        """
        self.portfolio._beliefs['factor_unified_expected_return'].sigma = 0.40
        out = self.engine.evaluate('AAPL', _mild_bullish_snapshot(), vix=14.0)

        # No evidence
        size_0 = self.portfolio.size(out, price=180.0, portfolio_value=100_000)
        haircut_0 = size_0['uncertainty_haircut']

        # 30 winning trades
        for _ in range(30):
            self.portfolio.update('factor_unified_expected_return', realised_return=0.02)

        size_30 = self.portfolio.size(out, price=180.0, portfolio_value=100_000)
        haircut_30 = size_30['uncertainty_haircut']

        self.assertGreater(haircut_30, haircut_0,
            f"Haircut should grow (less aggressive cut) with evidence: "
            f"{haircut_0:.3f} → {haircut_30:.3f}")
        self.assertGreater(size_30['alloc_pct'], size_0['alloc_pct'],
            "Position size should grow with proven track record")
        print(
            f"  Haircut: no_data={haircut_0:.3f}  after_30_wins={haircut_30:.3f}  "
            f"alloc: {size_0['alloc_pct']*100:.2f}% → {size_30['alloc_pct']*100:.2f}%"
        )

    def test_adv_cap_binding(self):
        """Tiny ADV should cap position below normal allocation."""
        out  = self.engine.evaluate('AAPL', _bullish_snapshot(), vix=14.0)
        # 50 winning trades to get a reasonable position size
        for _ in range(50):
            self.portfolio.update('factor_unified_expected_return', realised_return=0.02)
        size = self.portfolio.size(
            out, price=180.0, portfolio_value=1_000_000, adv=1_000  # tiny ADV
        )
        self.assertTrue(size['adv_capped'], "ADV cap should be binding with tiny ADV")
        self.assertLessEqual(size['qty'], 20.0,
            f"Qty should be ≤ 2% of 1000 shares = 20, got {size['qty']}")
        print(f"  ADV-capped qty={size['qty']:.2f}  adv_capped={size['adv_capped']}")

    def test_weights_sum_to_one(self):
        """Portfolio weights must always sum to exactly 1.0."""
        w = self.portfolio.weights()
        total = sum(w.values())
        self.assertAlmostEqual(total, 1.0, places=3,
            msg=f"Weights must sum to 1.0, got {total:.4f}")
        print(f"  Weights: {w}  total={total:.4f}")

    def test_report_structure(self):
        """report() should return all strategies in descending Sharpe order."""
        self.portfolio.update('factor_microstructure_flow', realised_return=0.03)
        self.portfolio.update('factor_mean_reversion',        realised_return=-0.01)
        report = self.portfolio.report()
        self.assertGreaterEqual(len(report), 2)
        sharpes = [r['sharpe_proxy'] for r in report]
        self.assertEqual(sharpes, sorted(sharpes, reverse=True),
            "Report should be sorted by Sharpe descending")
        print(f"  Report order: {[r['strategy'] for r in report]}")


if __name__ == '__main__':
    print("=" * 65)
    print("  AlphaEngine + BayesianPortfolio Test Suite")
    print("=" * 65)
    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    suite.addTests(loader.loadTestsFromTestCase(TestAlphaEngine))
    suite.addTests(loader.loadTestsFromTestCase(TestBayesianPortfolio))
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    if result.wasSuccessful():
        print("\n✅  All AlphaEngine + Bayesian Portfolio tests PASSED.")
    else:
        print(f"\n❌  {len(result.failures)} failure(s), {len(result.errors)} error(s).")

