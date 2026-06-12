"""
Alpha Adapter - Unified Probabilistic Alpha Engine V10 (Optimized)
==================================================================
Subscribes to MARKET_UPDATE.
Features zero-latency sentiment caching to avoid SQLite read contention on the hot path.
"""
import sqlite3
import time
import os
import threading
import numpy as np
from logger import get_logger

log = get_logger("Alpha Adapter")

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data', 'trading_brain.db'
)
POLL_INTERVAL = 10

# ── Backward-compatible stub kept for other callers ──────────────────────────
class KalmanFilter:
    def __init__(self, transition_covariance=1e-5, observation_covariance=1e-3):
        self.x = 1.0; self.P = 1.0
        self.Q = transition_covariance; self.R = observation_covariance
    def update(self, z):
        P_pred = self.P + self.Q
        K = P_pred / (P_pred + self.R)
        self.x = self.x + K * (z - self.x)
        self.P = (1.0 - K) * P_pred
        return self.x

# ── Delegate to the new AlphaEngine ──────────────────────────────────────────
from alpha_engine import get_engine as _get_alpha_engine, VIX_CAUTION, VIX_PANIC, LONG_THRESHOLD, SHORT_THRESHOLD
from portfolio   import get_portfolio as _get_portfolio

VIX_FEAR = 20.0

# Keep a thin wrapper so other callers still work
class UnifiedMicrostructureAlphaEngine:
    """Backward-compatible shim — delegates to alpha_engine.AlphaEngine."""
    def evaluate_ticker(self, ticker, d):
        out = _get_alpha_engine().evaluate(ticker, d)
        return {
            'expected_return':  out.expected_return,
            'confidence':       out.confidence,
            'decay_probability': out.decay_half_life / 60.0,
            'liquidity_penalty': out.liquidity_cost / 10_000,
            'signal':            out.signal,
        }

alpha_engine = UnifiedMicrostructureAlphaEngine()

STRATEGIES = {
    'factor_latent_alpha':    lambda d: ('BUY', 0.8, 'FACTOR_LATENT_ALPHA')    if d.get('momentum_12_1', 0.0) > 0.05 else (None, 0.0, None),
    'factor_mean_reversion':  lambda d: ('BUY', 0.8, 'FACTOR_MEAN_REVERSION')  if d.get('reversal_5d', 0.0) > 0.01 else (None, 0.0, None),
}
STRATEGY_CATEGORIES = {
    'QuantitativeFactor': ['factor_latent_alpha'],
    'Volatility':         ['factor_mean_reversion'],
}

from event_bus import get_bus, Event, Topics
from models    import AlphaSignal

event_bus = get_bus()

# Zero-Latency Sentiment Cache Cache and Lock (Phase-2 Latency Optimization)
_cached_vix = 15.0
_cached_regime = 'Bull'
_cached_is_halted = False
_cache_lock = threading.Lock()

def _update_sentiments_locally():
    global _cached_vix, _cached_regime, _cached_is_halted
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10.0)
        cursor = conn.cursor()

        # Get VIX
        vix_level = 15.0
        cursor.execute('SELECT vix_level FROM macro_sentiment ORDER BY id DESC LIMIT 1')
        vix_row = cursor.fetchone()
        if vix_row and vix_row[0]:
            vix_level = vix_row[0]

        # Get Regime
        regime = 'Bull'
        cursor.execute('SELECT current_regime FROM regime_state WHERE id = 1')
        regime_row = cursor.fetchone()
        if regime_row:
            regime = regime_row[0]

        # Get Circuit Breaker
        cursor.execute('SELECT is_halted FROM circuit_breaker WHERE id = 1')
        cb_row = cursor.fetchone()
        circuit_halted = bool(cb_row[0]) if cb_row else False
        
        conn.close()
        
        with _cache_lock:
            _cached_vix = vix_level
            _cached_regime = regime
            _cached_is_halted = circuit_halted
            
    except Exception as e:
        log.warning(f"Failed to update sentiments cache: {e}")

def _periodically_poll_sentiments():
    """Background polling thread to keep sentiments updated."""
    log.info("Starting background sentiment cache polling thread (Alpha Adapter)...")
    while True:
        _update_sentiments_locally()
        time.sleep(10)

def on_circuit_breaker_update(event: Event):
    """Immediate circuit breaker cache invalidation (Zero-Latency)."""
    global _cached_is_halted
    halted = event.data.get('is_halted', False)
    with _cache_lock:
        _cached_is_halted = bool(halted)
    log.info(f"⚡ [Cache Invalidation] Circuit breaker status immediately updated to: {halted}")

def on_portfolio_update(event: Event):
    """Immediate regime state cache invalidation (Zero-Latency)."""
    global _cached_regime
    regime = event.data.get('regime', 'Bull')
    with _cache_lock:
        _cached_regime = regime
    log.info(f"⚡ [Cache Invalidation] Regime state immediately updated to: {regime}")


def on_market_update(event: Event):
    ticker = event.data.get('ticker')
    d = event.data.get('features')
    
    if not ticker or not d:
        return

    close = d.get('close_price', 0)
    if close <= 0:
        return

    # Serve VIX, Regime, and Halted status from cache in 0ms (No SQLite queries on hot ticks!)
    with _cache_lock:
        vix_level = _cached_vix
        regime = _cached_regime
        circuit_halted = _cached_is_halted

    if circuit_halted:
        return

    d['regime'] = regime
    d['vix_level'] = vix_level
    d['ticker'] = ticker

    # 1. Run the ONE Unified Probabilistic Alpha Engine
    res = alpha_engine.evaluate_ticker(ticker, d)
    if not res:
        return
        
    expected_return = res['expected_return']
    confidence = res['confidence']
    decay_p = res['decay_probability']
    liq_penalty = res['liquidity_penalty']
    
    # 2. Map probabilistic return back to BUY/SELL signals for event bus compatibility
    # V13 Arch 1: Use imported thresholds from alpha_engine (single source of truth)
    signal_type = res.get('signal')
        
    # Scale confidence with VIX Caution levels
    if signal_type and regime not in ('Fear', 'Panic'):
        if vix_level > VIX_CAUTION:
            penalty = min(0.30, (vix_level - VIX_CAUTION) * 0.06)
            confidence *= (1.0 - penalty)
            
        signal_obj = AlphaSignal(
            ticker=ticker,
            signal_type=signal_type,
            target_strategy='factor_unified_expected_return',
            confidence=round(min(confidence, 1.0), 3),
            supporting_strategies=[
                f"E[r]={expected_return:.4f}",
                f"Decay_P={decay_p:.3f}",
                f"Liq_Penalty={liq_penalty:.4f}"
            ],
            exit_rule='Microstructure_Exit'
        )
        event_bus.publish(Topics.SIGNAL_GENERATED, Event(source="alpha_adapter", data=signal_obj.model_dump()))
        log.debug(f">>> Unified Alpha for {ticker} | E[r]: {expected_return:+.4f} | Conf: {confidence*100:.1f}%")


def run_alpha_adapter():
    log.info("Starting Unified Probabilistic Alpha Adapter (Event-Driven)...")
    
    # Warm up cache locally
    _update_sentiments_locally()
    
    # Spin up cache polling thread
    threading.Thread(target=_periodically_poll_sentiments, name="AlphaAdapter-PollCache", daemon=True).start()
    
    # Subscribe to zero-latency cache invalidations
    event_bus.subscribe(Topics.CIRCUIT_BREAKER, on_circuit_breaker_update)
    event_bus.subscribe(Topics.PORTFOLIO_UPDATE, on_portfolio_update)
    
    event_bus.subscribe(Topics.MARKET_UPDATE, on_market_update)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Alpha Adapter shutting down.")

if __name__ == '__main__':
    run_alpha_adapter()

