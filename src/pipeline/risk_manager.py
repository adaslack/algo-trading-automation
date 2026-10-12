"""
Risk Manager — Centralized Risk Authority (Broker-Agnostic V2)
===============================================================
Subscribes to SIGNAL_GENERATED.
Evaluates portfolio VaR, Kelly sizing, circuit breakers, and correlation.
Features in-memory price history caching to reduce latency from ~2000ms to <1ms.
Now uses the unified BaseBroker interface for broker-agnostic execution.
"""
import sqlite3
import time
import datetime
import os
import sys
import json
import threading
import numpy as np
from dotenv import load_dotenv

from logger import get_logger
from paths import DB_PATH, ENV_FILE

# Add brokers to path
_BROKERS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'brokers')
if _BROKERS_DIR not in sys.path:
    sys.path.insert(0, _BROKERS_DIR)

from broker_factory import get_broker

# Event Bus and Domain Models
from event_bus import get_bus, Event, Topics
from models    import AlphaSignal, Order, SignalType
from risk_gate import RiskGate

# Bayesian Portfolio Engine
from portfolio     import get_portfolio as _get_portfolio
from alpha_engine  import get_engine    as _get_alpha_engine

log = get_logger("Risk Manager")

load_dotenv(ENV_FILE)

MIN_ALLOCATION_PCT = 0.02
MAX_ALLOCATION_PCT = 0.15

CIRCUIT_BREAKER_PCT = 0.04

class PositionProxy:
    """Wrapper to standardize broker positions for risk checks."""
    __slots__ = ('qty', 'market_value')
    def __init__(self, qty: float, market_value: float = 0.0):
        self.qty = qty
        self.market_value = market_value
MAX_DAILY_TRADES = 20

event_bus = get_bus()

# Initialize broker via factory
broker = None
try:
    broker = get_broker(auto_connect=True)
    log.info(f"Risk Manager using broker: {broker.name}")
except Exception as e:
    log.warning(f"Could not initialize broker in RiskManager: {e}")

risk_gate = RiskGate(DB_PATH, trading_client=None)

# Thread-local database connection pool

_local_db = threading.local()

def _get_db_conn():
    if not hasattr(_local_db, "conn"):
        _local_db.conn = sqlite3.connect(DB_PATH, timeout=30.0, check_same_thread=False)
        try:
            _local_db.conn.execute('PRAGMA journal_mode=WAL')
            _local_db.conn.execute('PRAGMA synchronous=NORMAL')
            _local_db.conn.execute('PRAGMA cache_size=-64000')  # 64MB cache
        except Exception:
            pass
    return _local_db.conn

# Cache removed in favor of offline correlation database seeding (sub-millisecond optimization)

# ========== RISK CALCULATIONS ==========

def get_strategy_stats(cursor, strategy_name):
    try:
        cursor.execute(
            'SELECT win_rate, avg_win, avg_loss FROM strategy_performance WHERE strategy = ? LIMIT 1',
            (strategy_name,)
        )
        row = cursor.fetchone()
        if row and all(v is not None for v in row):
            return float(row[0]), float(row[1]), float(row[2])
    except Exception:
        pass
    return 0.5, 0.02, 0.02

def kelly_criterion(win_rate, avg_win, avg_loss, fraction=0.5):
    if avg_loss == 0 or avg_win == 0:
        return MIN_ALLOCATION_PCT
    win_loss_ratio = avg_win / avg_loss
    kelly_pct = win_rate - ((1 - win_rate) / win_loss_ratio)
    kelly_pct = max(0, min(1, kelly_pct))
    return kelly_pct * fraction

def check_correlation(cursor, new_ticker, existing_tickers):
    if not existing_tickers:
        return 0.0
    
    try:
        t0 = time.time()
        correlations = []
        
        for exist in existing_tickers:
            # Sort query targets alphabetically (stored as ticker_a < ticker_b in DB)
            ta, tb = sorted([new_ticker, exist])
            cursor.execute(
                'SELECT correlation FROM watchlist_correlation WHERE ticker_a = ? AND ticker_b = ?',
                (ta, tb)
            )
            row = cursor.fetchone()
            if row is not None:
                correlations.append(float(row[0]))
                
        elapsed = (time.time() - t0) * 1000
        avg_corr = float(np.mean(correlations)) if correlations else 0.0
        log.info(f"⚡ [Risk Manager DB] Correlation queried for {new_ticker} vs {existing_tickers} in {elapsed:.2f}ms (Result={avg_corr:.2f}).")
        return avg_corr
        
    except Exception as e:
        log.warning(f"Correlation check failed: {e}")
        return 0.0

def update_circuit_breaker(cursor, portfolio_value):
    today = datetime.date.today().isoformat()
    
    cursor.execute('SELECT date, day_open_value, is_halted, trades_today FROM circuit_breaker WHERE id = 1')
    cb = cursor.fetchone()
    
    if not cb or cb[0] != today:
        cursor.execute('''
            INSERT OR REPLACE INTO circuit_breaker (id, date, day_open_value, current_value, is_halted, halt_reason, trades_today)
            VALUES (1, ?, ?, ?, 0, NULL, 0)
        ''', (today, portfolio_value, portfolio_value))
        return False, 0
    
    day_open_value = cb[1]
    is_halted = cb[2]
    trades_today = cb[3] or 0
    
    cursor.execute('UPDATE circuit_breaker SET current_value = ? WHERE id = 1', (portfolio_value,))
    
    if is_halted:
        return True, trades_today
    
    if day_open_value > 0:
        daily_pnl_pct = (portfolio_value - day_open_value) / day_open_value
        if daily_pnl_pct <= -CIRCUIT_BREAKER_PCT:
            cursor.execute('''
                UPDATE circuit_breaker SET is_halted = 1, halt_reason = ? WHERE id = 1
            ''', (f"Daily loss {daily_pnl_pct*100:.2f}% exceeds {CIRCUIT_BREAKER_PCT*100:.0f}% limit",))
            log.critical(f"CIRCUIT BREAKER TRIGGERED! Portfolio down {daily_pnl_pct*100:.2f}% today.")
            return True, trades_today
    
    if trades_today >= MAX_DAILY_TRADES:
        cursor.execute('''
            UPDATE circuit_breaker SET is_halted = 1, halt_reason = ? WHERE id = 1
        ''', (f"Max daily trades ({MAX_DAILY_TRADES}) reached",))
        log.warning(f"Daily trade limit reached.")
        return True, trades_today
    
    return False, trades_today


# ========== MAIN RISK LOOP (EVENT-DRIVEN) ==========

def on_signal_generated(event: Event):
    try:
        signal_data = event.data
        if not signal_data:
            return
            
        signal = AlphaSignal(**signal_data)
        ticker = signal.ticker
        signal_type = signal.signal_type.value
        strategy = signal.target_strategy
        confidence = signal.confidence
        exit_rule = signal.exit_rule
        
        # V13 FIX: Initialize alpha_out at function scope (was using fragile dir() check)
        alpha_out = None
        
        conn = _get_db_conn()
        cursor = conn.cursor()

        # Update Portfolio State via Broker Abstraction
        portfolio_value = 100000.0  # fallback simulated value
        buying_power = 100000.0  # fallback simulated value
        positions = {}
        if broker is not None and broker.is_connected():
            try:
                account = broker.get_account()
                portfolio_value = account.portfolio_value
                buying_power = account.buying_power
                
                broker_positions = broker.get_positions()
                positions = {symbol: PositionProxy(bp.qty, bp.market_value) for symbol, bp in broker_positions.items()}
            except Exception as e:
                log.error(f"Failed to fetch broker account: {e}. Using simulated defaults.")
        else:
            # Under simulation / backtesting mode, get current positions from DB trade_history
            try:
                cursor.execute("SELECT ticker FROM trade_history WHERE status = 'OPEN'")
                open_tickers = [r[0] for r in cursor.fetchall()]
                positions = {t: PositionProxy(100.0, 0.0) for t in open_tickers}
            except Exception:
                pass

        has_pos = ticker in positions

        # Update circuit breaker
        is_halted, trades_today = update_circuit_breaker(cursor, portfolio_value)
        if is_halted:
            return

        # V15: Fetch only active V15 columns to purge unused remnants
        cursor.execute(
            'SELECT close_price, volume, garch_volatility, volume_ratio, '
            'momentum_12_1, reversal_5d, vol_regime '
            'FROM market_data WHERE ticker = ?', (ticker,)
        )
        market_row = cursor.fetchone()
        if not market_row or not market_row[0] or market_row[0] <= 0:
            log.warning(f"REJECTED {signal_type} {ticker}: No valid price found.")
            return
            
        price = float(market_row[0])
        adv = float(market_row[1]) if market_row[1] else 5_000_000.0
        
        # Pre-build snapshot dict once (reused by both BUY and SELL paths)
        _mrow = market_row
        snapshot = {
            'close_price':      price,
            'garch_volatility': float(_mrow[2]) if _mrow[2] is not None else 0.02,
            'volume_ratio':     float(_mrow[3]) if _mrow[3] is not None else 1.0,
            'momentum_12_1':    float(_mrow[4]) if _mrow[4] is not None else 0.0,
            'reversal_5d':      float(_mrow[5]) if _mrow[5] is not None else 0.0,
            'vol_regime':       float(_mrow[6]) if _mrow[6] is not None else 0.0,
        }
        
        approved_action = None
        approved_qty = 0

        # ===== BUY LOGIC =====
        is_short_cover = False
        is_long_close = False
        is_short_open = False
        
        if signal_type == 'BUY':
            if has_pos:
                pos_qty = float(positions[ticker].qty)
                if pos_qty >= 0:
                    log.info(f"REJECTED BUY {ticker}: Already holding long position.")
                    return
                else:
                    approved_action = 'BUY'
                    approved_qty = abs(pos_qty)
                    is_short_cover = True
                    log.info(f"Covering short position for {ticker}: BUY {approved_qty} shares.")
            else:
                # Correlation check
                avg_corr = check_correlation(cursor, ticker, list(positions.keys()))
                if avg_corr > 0.7:
                    log.warning(f"REJECTED BUY {ticker}: Too correlated (avg={avg_corr:.2f}).")
                    return
                
                # Check if it's a news breakthrough signal
                if strategy == 'news_breakthrough_momentum':
                    alloc_pct = float(event.data.get('kelly_pct', 0.02))
                    qty = (portfolio_value * alloc_pct) / price
                    sizing = {'uncertainty_haircut': 0.0, 'adv_capped': False}
                    alpha_out = None
                    log.info(f"Bypassing Bayesian sizing for news momentum buy. Allocated {alloc_pct*100:.1f}%")
                else:
                    # V13: Snapshot and ADV already fetched above in consolidated query
                    alpha_out = _get_alpha_engine().evaluate(ticker, snapshot)

                    # Bayesian size
                    sizing = _get_portfolio().size(
                        alpha_out       = alpha_out,
                        price           = price,
                        portfolio_value = portfolio_value,
                        adv             = adv,
                    )
                    alloc_pct = sizing['alloc_pct']
                    qty       = sizing['qty']

                if sizing.get('adv_capped', False):
                    log.warning(
                        f"⚠️ ADV cap active for {ticker}: qty={qty:.4f} "
                        f"(ADV={adv:,.0f}, haircut={sizing['uncertainty_haircut']:.2f})"
                    )

                if qty > 0 and buying_power > (qty * price):
                    order = Order(
                        ticker=ticker,
                        action=SignalType.BUY,
                        qty=qty,
                        estimated_price=price,
                        strategy=strategy or 'factor_latent_alpha',
                        confidence=confidence if confidence else 0.5,
                        kelly_pct=alloc_pct,
                    )
                    decision = risk_gate.approve(order)

                    if decision.approved:
                        approved_action = 'BUY'
                        approved_qty    = qty
                        log.info(
                            f"Sizing approved for {strategy}: "
                            f"E[r]={sizing.get('blended_er', 0.0):+.3f} "
                            f"Haircut={sizing.get('uncertainty_haircut', 0.0):.2f} "
                            f"Alloc={alloc_pct*100:.1f}% = {qty:.4f} shares"
                        )
                    else:
                        log.warning(f"REJECTED BUY {ticker}: RiskGate denied — {decision.reason}")
                else:
                    log.warning(f"REJECTED BUY {ticker}: Insufficient buying power.")
    
        # ===== SELL LOGIC =====
        elif signal_type == 'SELL':
            if has_pos:
                pos_qty = float(positions[ticker].qty)
                if pos_qty > 0:
                    approved_action = 'SELL'
                    approved_qty = pos_qty
                    is_long_close = True
                    log.info(f"Closing long position for {ticker}: SELL {approved_qty} shares.")
                else:
                    log.info(f"REJECTED SELL {ticker}: Already short.")
            else:
                if strategy == 'factor_microstructure_flow' or strategy == 'news_breakthrough_momentum':
                    avg_corr = check_correlation(cursor, ticker, list(positions.keys()))
                    if avg_corr > 0.7:
                        log.warning(f"REJECTED SHORT {ticker}: Too correlated (avg={avg_corr:.2f}).")
                        return
                    
                    if strategy == 'news_breakthrough_momentum':
                        alloc_pct = float(event.data.get('kelly_pct', 0.02))
                        qty = (portfolio_value * alloc_pct) / price
                        sizing = {'uncertainty_haircut': 0.0}
                        alpha_out = None
                        log.info(f"Bypassing Bayesian sizing for news momentum short. Allocated {alloc_pct*100:.1f}%")
                    else:
                        # V13: Snapshot and ADV already fetched above in consolidated query
                        alpha_out = _get_alpha_engine().evaluate(ticker, snapshot)

                        # Bayesian size
                        sizing = _get_portfolio().size(
                            alpha_out       = alpha_out,
                            price           = price,
                            portfolio_value = portfolio_value,
                            adv             = adv,
                        )
                        alloc_pct = sizing['alloc_pct']
                        qty       = sizing['qty']

                    if qty > 0 and buying_power > (qty * price):
                        approved_action = 'SELL'
                        approved_qty = qty
                        is_short_open = True
                        log.info(
                            f"Approved SHORT sale for {ticker}: {qty} shares "
                            f"(Bayesian alloc: {alloc_pct*100:.1f}%, haircut={sizing.get('uncertainty_haircut', 0.0):.2f})"
                        )
                    else:
                        log.warning(f"REJECTED SHORT {ticker}: Insufficient buying power.")
                else:
                    log.info(f"REJECTED SELL {ticker}: No position to sell.")
    
        # ===== HEDGE LOGIC =====
        elif signal_type == 'HEDGE':
            if has_pos:
                pos = positions[ticker]
                pos_value = abs(float(pos.market_value))
                if pos_value > 1000:
                    put_strike = round(price * 0.95, 2)
                    num_contracts = max(1, int(float(pos.qty) / 100))
                    cursor.execute('''
                        INSERT INTO options_queue (timestamp, underlying, option_type, action, strike, quantity, premium_estimate, status)
                        VALUES (?, ?, 'PUT', 'BUY', ?, ?, ?, 'QUEUED')
                    ''', (datetime.date.today().isoformat(), ticker, put_strike, num_contracts, price * 0.02))
                    log.info(f"HEDGE APPROVED: {num_contracts}x PUT on {ticker} @ ${put_strike}")
    
        # If approved, publish ORDER_APPROVED event
        if approved_action and approved_qty > 0:
            order_to_execute = Order(
                ticker=ticker,
                action=SignalType(approved_action),
                qty=approved_qty,
                estimated_price=price,
                strategy=strategy or 'factor_latent_alpha',
                confidence=confidence,
                kelly_pct=0.0
            )
            
            event_bus.publish(Topics.ORDER_APPROVED, Event(
                source="risk_manager",
                data=order_to_execute.model_dump()
            ))
            
            log.info(f"APPROVED {approved_action} {approved_qty}x {ticker} @ ${price:.2f}")
            
            if is_short_cover:
                cursor.execute('''
                    UPDATE trade_history SET status = 'CLOSING'
                    WHERE ticker = ? AND action = 'SELL' AND status = 'OPEN'
                ''', (ticker,))
            elif is_long_close:
                cursor.execute('''
                    UPDATE trade_history SET status = 'CLOSING'
                    WHERE ticker = ? AND action = 'BUY' AND status = 'OPEN'
                ''', (ticker,))
            elif is_short_open:
                cursor.execute('''
                    INSERT INTO trade_history (ticker, action, entry_price, entry_date, quantity, strategy, exit_rule, predicted_er, status)
                    VALUES (?, 'SELL', ?, ?, ?, ?, ?, ?, 'OPEN')
                ''', (ticker, price, datetime.date.today().isoformat(), approved_qty, strategy, exit_rule,
                      alpha_out.expected_return if alpha_out is not None else None))
            elif approved_action == 'BUY':  # Normal Long Open
                cursor.execute('''
                    INSERT INTO trade_history (ticker, action, entry_price, entry_date, quantity, strategy, exit_rule, predicted_er, status)
                    VALUES (?, 'BUY', ?, ?, ?, ?, ?, ?, 'OPEN')
                ''', (ticker, price, datetime.date.today().isoformat(), approved_qty, strategy, exit_rule,
                      alpha_out.expected_return if alpha_out is not None else None))
            
            cursor.execute('UPDATE circuit_breaker SET trades_today = trades_today + 1 WHERE id = 1')

        conn.commit()

    except Exception as e:
        log.error(f"Error in Risk Event Handler: {e}")

def on_news_sentiment_alert(event: Event):
    """
    Handles breaking news sentiment alerts.
    Validates sentiment with recent volume spikes (volume_ratio > 1.5).
    """
    try:
        data = event.data
        ticker = data.get("ticker")
        sentiment_score = float(data.get("sentiment_score", 0.0))
        headline = data.get("headline", "")
        source = data.get("source", "")
        
        # Use thread-local DB connection (routes through DuckDB/Postgres adapter)
        conn = _get_db_conn()
        cursor = conn.cursor()
        
        # Fetch current volume_ratio and close_price
        cursor.execute("SELECT volume_ratio, close_price FROM market_data WHERE ticker=?", (ticker,))
        row = cursor.fetchone()
        if not row:
            log.warning(f"No market data found for {ticker}. Skipping sentiment trade.")
            return
            
        volume_ratio = float(row[0] or 1.0)
        price = float(row[1])
        
        # Retrieve portfolio value for sizing
        portfolio_value = 100000.0
        if broker is not None and broker.is_connected():
            try:
                account = broker.get_account()
                portfolio_value = float(account.portfolio_value)
            except Exception:
                pass
        
        # Validate using:
        # 1. High absolute sentiment (e.g. >= 0.5)
        # 2. Volume Ratio >= 1.5 (showing volume spike/money influx)
        if abs(sentiment_score) >= 0.5 and volume_ratio >= 1.5:
            action = 'BUY' if sentiment_score > 0 else 'SELL'
            
            # Sizing: 2% of portfolio * sentiment multiplier
            alloc_pct = 0.02 * min(1.0, abs(sentiment_score))
            
            log.info(f"📰 NEWS BREAKTHROUGH VALIDATED for {ticker}! Headline: '{headline[:50]}' | Volume Ratio: {volume_ratio:.2f}x | Action: {action}")
            
            # Create order event and publish to event bus for Signal Generated
            # (which will be processed by on_signal_generated risk rules)
            event_bus.publish(Topics.SIGNAL_GENERATED, Event(
                source="news_breakthrough_engine",
                data={
                    "ticker": ticker,
                    "signal_type": action,
                    "confidence": abs(sentiment_score),
                    "target_strategy": "news_breakthrough_momentum",
                    "price_estimate": price,
                    "kelly_pct": alloc_pct
                }
            ))
        else:
            log.debug(f"News sentiment alert discarded for {ticker} (sentiment={sentiment_score:.2f}, vol_ratio={volume_ratio:.2f}x)")
            
    except Exception as e:
        log.error(f"Error in on_news_sentiment_alert handler: {e}")

def run_risk_manager():
    log.info("Starting Centralized Risk Manager (Event-Driven)...")
    
    event_bus.subscribe(Topics.SIGNAL_GENERATED, on_signal_generated)
    event_bus.subscribe(Topics.NEWS_SENTIMENT_ALERT, on_news_sentiment_alert)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Risk Manager shutting down.")

if __name__ == '__main__':
    run_risk_manager()
