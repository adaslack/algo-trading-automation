"""
Cross-Sectional Microstructure Ranker
======================================
Thin adapter. All ranking delegated to alpha_engine.AlphaEngine.rank_universe().
Publishes BUY (long) and SELL (short) signals for top/bottom decile by E[r].
"""
import sqlite3
import time
import os
from logger    import get_logger
from event_bus import get_bus, Event, Topics
from models    import AlphaSignal
from alpha_engine import get_engine as _get_alpha_engine

import numpy as np

log        = get_logger("Cross-Sectional Ranker")
DB_PATH    = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data', 'trading_brain.db'
)
POLL_INTERVAL = 60
event_bus     = get_bus()


def z_score_normalize(values):
    """Normalize a list of values to standard Z-scores (backward compatibility)."""
    arr = np.array(values, dtype=float)
    mean = np.mean(arr)
    std = np.std(arr)
    if std == 0:
        return np.zeros_like(arr)
    return (arr - mean) / std


def get_watchlist() -> list:
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute('PRAGMA journal_mode=WAL')
        c = conn.cursor()
        c.execute('SELECT ticker FROM daily_watchlist')
        tickers = [r[0] for r in c.fetchall()]
        conn.close()
        return tickers
    except Exception as e:
        log.error(f"Failed to read watchlist: {e}")
        return []


def fetch_latest_market_data(tickers: list) -> dict:
    """
    Fetch latest microstructure features from market_data for ranking.
    Returns dict: {ticker: feature_snapshot}
    """
    if not tickers:
        return {}
    data = {}
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30.0)
        conn.execute('PRAGMA journal_mode=WAL')
        c = conn.cursor()
        placeholders = ', '.join(['?'] * len(tickers))
        c.execute(f'''
            SELECT ticker, close_price, sector, vpin, obi, micro_price, dealer_gex,
                   insider_score, garch_volatility, volume_ratio, hurst_exponent, vwap,
                   momentum_12_1, reversal_5d, volume_breakout, vol_regime, trend_strength
            FROM market_data WHERE ticker IN ({placeholders})
        ''', tickers)
        for r in c.fetchall():
            close = r[1] or 0.0
            data[r[0]] = {
                'close_price':      close,
                'sector':           r[2]  or 'Unknown',
                'vpin':             r[3]  or 0.3,
                'obi':              r[4]  or 0.0,
                'micro_price':      r[5]  or close,
                'dealer_gex':       r[6]  or 0.0,
                'insider_score':    r[7]  or 0.0,
                'garch_volatility': r[8]  or 0.02,
                'volume_ratio':     r[9]  or 1.0,
                'hurst_exponent':   r[10] or 0.5,
                'vwap':             r[11] or close,
                'momentum_12_1':    r[12] or 0.0,
                'reversal_5d':      r[13] or 0.0,
                'volume_breakout':  r[14] or 0.0,
                'vol_regime':       r[15] or 0.0,
                'trend_strength':   r[16] or 0.0,
            }
        conn.close()
    except Exception as e:
        log.error(f"Failed to fetch market data: {e}")
    return data


def run_cross_sectional_ranker():
    log.info("Starting Cross-Sectional Microstructure Ranker (V14 Alpha Excellence)...")

    while True:
        try:
            tickers = get_watchlist()
            if len(tickers) < 3:
                log.info("Watchlist < 3 tickers. Waiting...")
                time.sleep(POLL_INTERVAL)
                continue

            universe = fetch_latest_market_data(tickers)
            valid    = {t: d for t, d in universe.items() if d['close_price'] > 0}

            if len(valid) < 3:
                log.info("Insufficient valid data for cross-sectional ranking. Waiting...")
                time.sleep(POLL_INTERVAL)
                continue

            # 1. ── FACTOR RESIDUALIZATION ─────────────────────────────────────
            # Group tickers and run OLS Factor Residualizer
            from feature_store import FactorResidualizer
            sectors_map = {t: d.get('sector', 'Unknown') for t, d in valid.items()}
            # Extract SPY beta or default to 1.0
            spy_betas_map = {t: 1.0 for t in valid}
            
            valid_resid = FactorResidualizer.residualize(valid, sectors_map, spy_betas_map)

            # 2. ── ALPHA ENGINE EVALUATION ────────────────────────────────────
            engine = _get_alpha_engine()
            evaluated = {}
            for t, snapshot in valid_resid.items():
                # Extract macro VIX from valid market data if available (default 15.0)
                vix = 15.0
                evaluated[t] = engine.evaluate(t, snapshot, vix=vix)
                
            # Group by sector to form Sector-Neutral Pairs
            sector_groups = {}
            for t, out in evaluated.items():
                sect = sectors_map.get(t, 'Unknown')
                if sect == 'Market' or sect == 'Unknown':
                    continue
                if sect not in sector_groups:
                    sector_groups[sect] = []
                sector_groups[sect].append(out)
                
            # 3. ── SECTOR-NEUTRAL PAIRS SIGNAL GENERATION ─────────────────────
            pair_count = 0
            for sect, group in sector_groups.items():
                if len(group) < 2:
                    continue # Need at least 2 constituents in sector to pair trade
                
                # Sort descending by expected relative return
                group.sort(key=lambda o: o.expected_return, reverse=True)
                
                long_candidate = group[0]
                short_candidate = group[-1]
                
                if long_candidate.expected_return > short_candidate.expected_return:
                    # Emit LONG signal for strongest sector constituent
                    sig_long = AlphaSignal(
                        ticker=long_candidate.ticker, signal_type='BUY',
                        target_strategy='Microstructure_Ranker',
                        confidence=long_candidate.confidence, exit_rule='Microstructure_Exit',
                    )
                    event_bus.publish(Topics.SIGNAL_GENERATED,
                                      Event(source="cross_sectional_ranker", data=sig_long.model_dump()))
                    log.info(
                        f"PAIR LONG  [{sect.upper()}] {long_candidate.ticker} E[r]={long_candidate.expected_return:+.4f} "
                        f"conf={long_candidate.confidence:.2f}"
                    )
                    
                    # Emit SHORT signal for weakest sector constituent
                    sig_short = AlphaSignal(
                        ticker=short_candidate.ticker, signal_type='SELL',
                        target_strategy='Microstructure_Ranker',
                        confidence=short_candidate.confidence, exit_rule='Microstructure_Exit',
                    )
                    event_bus.publish(Topics.SIGNAL_GENERATED,
                                      Event(source="cross_sectional_ranker", data=sig_short.model_dump()))
                    log.info(
                        f"PAIR SHORT [{sect.upper()}] {short_candidate.ticker} E[r]={short_candidate.expected_return:+.4f} "
                        f"conf={short_candidate.confidence:.2f}"
                    )
                    pair_count += 1
                    
            if pair_count == 0:
                log.info("No active sector pairs matched signal criteria on this cycle.")

        except Exception as e:
            log.error(f"Ranker error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == '__main__':
    run_cross_sectional_ranker()

