"""
Risk Manager — Centralized Risk Authority (Optimized)
=====================================================
Subscribes to SIGNAL_GENERATED.
Evaluates portfolio VaR, Kelly sizing, circuit breakers, and correlation.
Features in-memory price history caching to reduce latency from ~2000ms to <1ms.
"""
import sqlite3
import time
import datetime
import os
import json
import threading
import numpy as np
from dotenv import load_dotenv

from logger import get_logger

# Event Bus and Domain Models
from event_bus import get_bus, Event, Topics
from models    import AlphaSignal, Order, SignalType
from risk_gate import RiskGate

# Bayesian Portfolio Engine
from portfolio     import get_portfolio as _get_portfolio
from alpha_engine  import get_engine    as _get_alpha_engine

log = get_logger("Risk Manager")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'trading_brain.db')

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env'))
API_KEY = os.getenv('ALPACA_API_KEY')
SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')

MIN_ALLOCATION_PCT = 0.02
MAX_ALLOCATION_PCT = 0.15

CIRCUIT_BREAKER_PCT = 0.04
MAX_DAILY_TRADES = 20

from alpaca.trading.client import TradingClient

event_bus = get_bus()
try:
    trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)
except Exception as e:
    trading_client = None
    log.warning(f"Could not connect to Alpaca in RiskManager: {e}")

risk_gate = RiskGate(DB_PATH, trading_client=trading_client)

# In-memory daily price history cache & lock for thread safety (latency optimization)
_PRICE_CACHE = {}
_CACHE_LOCK = threading.Lock()

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

def _initialize_price_cache():
    """Asynchronously bulk pre-fetches price history to warm correlation cache."""
    log.info("Prefetching close prices for watchlist to warm correlation cache...")
    try:
        conn = _get_db_conn()
        c = conn.cursor()
        c.execute('SELECT ticker FROM daily_watchlist')
        watchlist = [r[0] for r in c.fetchall()]
        
        if not watchlist:
            log.info("No daily watchlist found. Cache prefetch skipped.")
            return
            
        import yfinance as yf
        import pandas as pd
        
        df = yf.download(watchlist, period='30d', interval='1d', progress=False)
        
        if isinstance(df.columns, pd.MultiIndex) and 'Close' in df.columns:
            close_df = df['Close']
        elif 'Close' in df.columns:
            close_df = df['Close']
        else:
            close_df = df
            
        with _CACHE_LOCK:
            for ticker in watchlist:
                if len(watchlist) == 1 and isinstance(close_df, pd.Series):
                    _PRICE_CACHE[ticker] = close_df.dropna()
                elif ticker in close_df.columns:
                    _PRICE_CACHE[ticker] = close_df[ticker].dropna()
                    
        log.info(f"Price cache warmed successfully with {len(_PRICE_CACHE)} tickers.")
    except Exception as e:
        log.warning(f"Failed to prefetch price history on boot: {e}")

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
        import yfinance as yf
        import pandas as pd
        
        t0 = time.time()
        
        # 1. Identify missing tickers
        missing = []
        with _CACHE_LOCK:
            for t in [new_ticker] + existing_tickers:
                if t not in _PRICE_CACHE or _PRICE_CACHE[t].empty:
                    missing.append(t)
        
        # 2. Fetch missing tickers asynchronously as fallback to avoid blocking the hot path
        if missing:
            log.info(f"Cache miss for {missing}. Spawning background fetch to warm correlation cache...")
            def bg_fetch():
                try:
                    df = yf.download(missing, period='30d', interval='1d', progress=False)
                    if isinstance(df.columns, pd.MultiIndex) and 'Close' in df.columns:
                        close_df = df['Close']
                    elif 'Close' in df.columns:
                        close_df = df['Close']
                    else:
                        close_df = df
                        
                    with _CACHE_LOCK:
                        for t in missing:
                            if len(missing) == 1 and isinstance(close_df, pd.Series):
                                _PRICE_CACHE[t] = close_df.dropna()
                            elif t in close_df.columns:
                                _PRICE_CACHE[t] = close_df[t].dropna()
                    log.info(f"Background correlation cache warmed for {missing}.")
                except Exception as fe:
                    log.warning(f"Background correlation cache warm failed: {fe}")
            
            threading.Thread(target=bg_fetch, daemon=True, name="RiskManager-BgFetch").start()
                
        # 3. Calculate correlation locally using price cache
        prices = {}
        with _CACHE_LOCK:
            for t in [new_ticker] + existing_tickers:
                if t in _PRICE_CACHE and not _PRICE_CACHE[t].empty:
                    prices[t] = _PRICE_CACHE[t]
                    
        if new_ticker not in prices or len(prices) < 2:
            return 0.0
            
        df = pd.DataFrame(prices)
        returns = df.pct_change().dropna()
        corr_matrix = returns.corr()
        
        correlations = []
        for exist in existing_tickers:
            if exist in corr_matrix.columns:
                corr = corr_matrix.loc[new_ticker, exist]
                if pd.notna(corr):
                    correlations.append(corr)
                    
        elapsed = (time.time() - t0) * 1000
        log.info(f"⚡ [Risk Manager Cache] Correlation computed locally for {new_ticker} vs {existing_tickers} in {elapsed:.2f}ms.")
        return np.mean(correlations) if correlations else 0.0
        
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

        # Update Portfolio State via Alpaca
        portfolio_value = 100000.0  # fallback simulated value
        buying_power = 100000.0  # fallback simulated value
        positions = {}
        if trading_client is not None:
            try:
                account = trading_client.get_account()
                portfolio_value = float(account.portfolio_value)
                buying_power = float(account.buying_power)
                
                positions_list = trading_client.get_all_positions()
                positions = {p.symbol: p for p in positions_list}
            except Exception as e:
                log.error(f"Failed to fetch Alpaca account: {e}. Using simulated defaults.")
        else:
            # Under simulation / backtesting mode, get current positions from DB trade_history
            try:
                cursor.execute("SELECT ticker FROM trade_history WHERE status = 'OPEN'")
                open_tickers = [r[0] for r in cursor.fetchall()]
                # Represent open positions minimally
                class SimulatedPos:
                    def __init__(self, qty):
                        self.qty = qty
                positions = {t: SimulatedPos(100.0) for t in open_tickers}
            except Exception:
                pass

        has_pos = ticker in positions

        # Update circuit breaker
        is_halted, trades_today = update_circuit_breaker(cursor, portfolio_value)
        if is_halted:
            return

        # V13 OPT: Single consolidated query for ALL market_data fields (was 3 separate queries)
        cursor.execute(
            'SELECT close_price, volume, vpin, obi, micro_price, dealer_gex, insider_score, '
            'garch_volatility, volume_ratio, hurst_exponent, vwap, '
            'momentum_12_1, reversal_5d, volume_breakout, vol_regime, trend_strength '
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
            'vpin':             float(_mrow[2]) if _mrow[2] is not None else 0.3,
            'obi':              float(_mrow[3]) if _mrow[3] is not None else 0.0,
            'micro_price':      float(_mrow[4]) if _mrow[4] is not None else price,
            'dealer_gex':       float(_mrow[5]) if _mrow[5] is not None else 0.0,
            'insider_score':    float(_mrow[6]) if _mrow[6] is not None else 0.0,
            'garch_volatility': float(_mrow[7]) if _mrow[7] is not None else 0.02,
            'volume_ratio':     float(_mrow[8]) if _mrow[8] is not None else 1.0,
            'hurst_exponent':   float(_mrow[9]) if _mrow[9] is not None else 0.5,
            'vwap':             float(_mrow[10]) if _mrow[10] is not None else price,
            'momentum_12_1':    float(_mrow[11]) if _mrow[11] is not None else 0.0,
            'reversal_5d':      float(_mrow[12]) if _mrow[12] is not None else 0.0,
            'volume_breakout':  float(_mrow[13]) if _mrow[13] is not None else 0.0,
            'vol_regime':       float(_mrow[14]) if _mrow[14] is not None else 0.0,
            'trend_strength':   float(_mrow[15]) if _mrow[15] is not None else 0.0,
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

                if sizing['adv_capped']:
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
                            f"Bayesian sizing: {strategy} "
                            f"E[r]={sizing['blended_er']:+.3f} "
                            f"Haircut={sizing['uncertainty_haircut']:.2f} "
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
                if strategy == 'factor_microstructure_flow':
                    avg_corr = check_correlation(cursor, ticker, list(positions.keys()))
                    if avg_corr > 0.7:
                        log.warning(f"REJECTED SHORT {ticker}: Too correlated (avg={avg_corr:.2f}).")
                        return
                    
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
                            f"(Bayesian alloc: {alloc_pct*100:.1f}%, haircut={sizing['uncertainty_haircut']:.2f})"
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

def run_risk_manager():
    log.info("Starting Centralized Risk Manager (Event-Driven)...")
    
    # Asynchronously initialize/warm the price history cache for correlations
    threading.Thread(target=_initialize_price_cache, name="RiskManager-WarmCache", daemon=True).start()
    
    event_bus.subscribe(Topics.SIGNAL_GENERATED, on_signal_generated)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Risk Manager shutting down.")

if __name__ == '__main__':
    run_risk_manager()

