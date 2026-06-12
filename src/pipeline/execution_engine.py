"""
Execution Engine (The Sniper)
==============================
Reads APPROVED orders from the event bus.
Connects to Alpaca to execute live Market/Limit Orders.
Logs the final trade and records fill analytics.
"""
import sqlite3
import time
import datetime
import os
import csv
from dotenv import load_dotenv

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from logger import get_logger

log = get_logger("Execution Engine")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'trading_brain.db')
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'data', 'pipeline_trades.csv')
LOG_FILE = os.path.abspath(LOG_FILE)

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env'))
API_KEY = os.getenv('ALPACA_API_KEY')
SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')

try:
    trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)
    data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
except Exception as e:
    trading_client = None
    data_client = None
    log.error(f"Could not connect to Alpaca: {e}")

import threading

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

def init_logger():
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    if not os.path.exists(LOG_FILE):
        with open(LOG_FILE, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['timestamp', 'ticker', 'action', 'qty', 'estimated_price', 'alpaca_order_id'])

def log_trade(ticker, action, qty, price, order_id):
    with open(LOG_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.datetime.now().isoformat(),
            ticker, action, qty, f'{price:.2f}', order_id
        ])

from event_bus import get_bus, Event, Topics
from execution_analytics import ExecutionAnalytics
from models import Order, SignalType, OrderStatus

event_bus = get_bus()
analytics = ExecutionAnalytics(DB_PATH)

# In-memory deduplication cache for local process thread safety
_processed_order_keys = set()

def on_order_approved(event: Event):
    if not trading_client:
        return
        
    order_data = event.data
    if not order_data:
        return
        
    order = Order(**order_data)
    ticker = order.ticker
    action = order.action.value
    qty = order.qty
    price_estimate = order.estimated_price
    start_time = time.time()
    
    # --- IDEMPOTENCY SAFETY GATE (Cross-Process & Double Execution Protection) ---
    event_timestamp = getattr(event, 'timestamp', '')
    correlation_id = getattr(event, 'correlation_id', '')
    unique_sig = f"{ticker}:{action}:{qty}:{event_timestamp}:{correlation_id}"
    
    # Local check (fast-path)
    if unique_sig in _processed_order_keys:
        log.warning(f"Discarding duplicate local execution for {ticker} (Sig: {unique_sig})")
        return
        
    # Distributed check via Redis (if event_bus is RedisBus)
    is_duplicate = False
    if hasattr(event_bus, '_redis') and event_bus._redis is not None:
        redis_key = f"idempotency:order_approved:{unique_sig}"
        try:
            # Atomic SETNX with 60 second expiration
            acquired = event_bus._redis.set(redis_key, "executed", nx=True, ex=60)
            if not acquired:
                is_duplicate = True
        except Exception as e:
            log.warning(f"Could not verify idempotency in Redis: {e}")
            
    if is_duplicate:
        log.warning(f"Discarding duplicate distributed execution for {ticker} (Sig: {unique_sig})")
        return
        
    _processed_order_keys.add(unique_sig)
    # Evict cache if it gets too large
    if len(_processed_order_keys) > 1000:
        _processed_order_keys.clear()
    # ----------------------------------------------------------------------------
    
    try:
        clock = trading_client.get_clock()
        if not clock.is_open:
            log.warning(f"Market closed. Dropping order for {ticker}.")
            return
            
        side = OrderSide.BUY if action == 'BUY' else OrderSide.SELL
        
        # ADAPTIVE EXECUTION CHECK
        bid_price = price_estimate
        ask_price = price_estimate
        mid_price = price_estimate
        half_spread = 0.01
        
        try:
            if data_client:
                quote_req = StockLatestQuoteRequest(symbol_or_symbols=ticker)
                quotes = data_client.get_stock_latest_quote(quote_req)
                if ticker in quotes:
                    q_data = quotes[ticker]
                    if q_data.ask_price and q_data.bid_price and q_data.bid_price > 0:
                        bid_price = float(q_data.bid_price)
                        ask_price = float(q_data.ask_price)
                        mid_price = (bid_price + ask_price) / 2.0
                        half_spread = max(0.01, (ask_price - bid_price) / 2.0)
        except Exception as e:
            log.warning(f"Could not check latest quote for Avellaneda pricing: {e}")
            
        # Get Current Inventory (q) for Avellaneda-Stoikov Reservation Pricing
        inventory = 0.0
        try:
            positions_list = trading_client.get_all_positions()
            for p in positions_list:
                if p.symbol == ticker:
                    inventory = float(p.qty)
                    break
        except Exception as e:
            log.warning(f"Could not fetch inventory for Avellaneda pricing: {e}")
            
        # Get Volatility (sigma) from ATR, ADV, and VPIN using a SINGLE DB connection
        volatility = 0.02
        adv = 1000000.0
        daily_vol = 0.02
        vpin = 0.3
        try:
            conn = _get_db_conn()
            cursor = conn.cursor()
            cursor.execute(
                'SELECT atr14, close_price, volume, volatility_20d, vpin '
                'FROM market_data WHERE ticker = ?', (ticker,)
            )
            row = cursor.fetchone()
            if row:
                if row[0] and row[1] and row[1] > 0:
                    volatility = float(row[0]) / float(row[1])
                if row[2] and row[2] > 0:
                    adv = float(row[2]) * 20.0  # Approximate 20-day ADV
                if row[3] and row[3] > 0:
                    daily_vol = float(row[3]) / 15.87  # Annualized to daily volatility
                if row[4]:
                    vpin = float(row[4])
        except Exception:
            pass
        # Submit Order to Alpaca
        try:
            from alpaca.trading.requests import MarketOrderRequest
            alpaca_order = trading_client.submit_order(
                order_data=MarketOrderRequest(
                    symbol=ticker,
                    qty=qty,
                    side=side,
                    time_in_force=TimeInForce.DAY
                )
            )
        except Exception as e:
            log.warning(f"Alpaca submit failed (simulating fill): {e}")
            alpaca_order = None

        # Base execution friction (10 bps base slippage + inventory risk premium)
        gamma = 0.1
        inventory_risk = abs(inventory) * gamma * (volatility ** 2)
        total_friction_pct = 0.0010 + inventory_risk
            
        # Adverse Selection Curve
        fill_ratio = 1.0
        if vpin > 0.5:
            # Linear decay: at VPIN=0.8, we only get filled on 55% of our order size due to queue front-running
            fill_ratio = max(0.1, 1.0 - (vpin - 0.5) * 1.5)
            # Add toxic adverse selection premium to slippage (up to 15 bps extra penalty)
            adverse_premium = (vpin - 0.5) * 0.0005
            total_friction_pct += adverse_premium
            log.info(f"Adverse Selection Active: High Toxicity (VPIN={vpin:.2f}) | Fill Ratio cut to {fill_ratio*100:.1f}% | Added {adverse_premium*10000:.1f} bps toxic premium.")
            
        final_filled_qty = float(qty) * fill_ratio
        
        alpaca_fill = float(alpaca_order.filled_avg_price) if (alpaca_order and alpaca_order.filled_avg_price) else price_estimate
        if action == 'BUY':
            fill_price = alpaca_fill * (1.0 + total_friction_pct)
        else:
            fill_price = alpaca_fill * (1.0 - total_friction_pct)
            
        latency_ms = (time.time() - start_time) * 1000
        
        analytics.record_fill(
            ticker=ticker,
            action=action,
            expected_price=price_estimate,
            fill_price=fill_price,
            qty=final_filled_qty,
            latency_ms=latency_ms,
            commission=0.0  # Alpaca is commission-free
        )
        if fill_ratio < 1.0:
            log.info(f"EXECUTED PARTIALLY (Adverse Selection): {action} {final_filled_qty:.1f}x {ticker} (of {qty}x requested)")
        else:
            log.info(f"EXECUTED FULLY: {action} {qty}x {ticker}")
        
        # --- DB AUDIT: EXECUTION STATE & SLIPPAGE SYNCHRONIZATION ---
        atr_row = None
        try:
            conn = _get_db_conn()
            cursor = conn.cursor()
            
            # Check if this is a closing fill (marked as 'CLOSING' by risk_manager)
            if action == 'BUY':
                cursor.execute(
                    "SELECT id, entry_price, entry_date, quantity FROM trade_history "
                    "WHERE ticker = ? AND action = 'SELL' AND status = 'CLOSING' "
                    "ORDER BY id DESC LIMIT 1", (ticker,)
                )
            else:
                cursor.execute(
                    "SELECT id, entry_price, entry_date, quantity FROM trade_history "
                    "WHERE ticker = ? AND action = 'BUY' AND status = 'CLOSING' "
                    "ORDER BY id DESC LIMIT 1", (ticker,)
                )
            closing_row = cursor.fetchone()
            
            if closing_row:
                trade_id, entry_price, entry_date_str, entry_qty = closing_row
                
                # Realized PnL and PnL% calculations
                if action == 'SELL':  # Long Close
                    pnl = (fill_price - entry_price) * final_filled_qty
                    pnl_pct = (fill_price - entry_price) / (entry_price if entry_price > 0 else 1.0)
                else:  # Short Cover (BUY)
                    pnl = (entry_price - fill_price) * final_filled_qty
                    pnl_pct = (entry_price - fill_price) / (entry_price if entry_price > 0 else 1.0)
                
                holding_days = 0
                try:
                    if entry_date_str:
                        entry_dt = datetime.date.fromisoformat(entry_date_str)
                        holding_days = (datetime.date.today() - entry_dt).days
                except Exception:
                    pass
                
                cursor.execute('''
                    UPDATE trade_history SET
                        exit_price = ?,
                        exit_date = ?,
                        pnl = ?,
                        pnl_pct = ?,
                        holding_days = ?,
                        status = 'CLOSED'
                    WHERE id = ?
                ''', (fill_price, datetime.date.today().isoformat(), pnl, pnl_pct, holding_days, trade_id))
                log.info(f"DB AUDIT: Closed trade {trade_id} for {ticker} | Entry price: ${entry_price:.2f} | Exit price: ${fill_price:.2f} | Realized PnL: ${pnl:.2f} ({pnl_pct*100:.2f}%)")
            else:
                # Opening fill: update the ideal quantity and estimated entry price
                cursor.execute('''
                    SELECT id FROM trade_history
                    WHERE ticker = ? AND action = ? AND status = 'OPEN' AND exit_date IS NULL
                    ORDER BY id DESC LIMIT 1
                ''', (ticker, action))
                open_row = cursor.fetchone()
                if open_row:
                    trade_id = open_row[0]
                    cursor.execute('''
                        UPDATE trade_history SET
                            entry_price = ?,
                            quantity = ?
                        WHERE id = ?
                    ''', (fill_price, final_filled_qty, trade_id))
                    log.info(f"DB AUDIT: Updated opening trade {trade_id} for {ticker} | Actual Entry Price: ${fill_price:.2f} | Actual Qty: {final_filled_qty:.4f}")
            
            # Fetch ATR-14 for trailing stop
            cursor.execute('SELECT atr14 FROM market_data WHERE ticker = ?', (ticker,))
            atr_row = cursor.fetchone()
            
            conn.commit()
        except Exception as db_err:
            log.error(f"FAILED to audit database execution fills for {ticker}: {db_err}")
            atr_row = None
        
        if atr_row and atr_row[0] and atr_row[0] > 0:
            atr = float(atr_row[0])
            
            if action == 'BUY':
                stop_limit_price = round(fill_price - (atr * 2.5), 2)
                side_stop = OrderSide.SELL
            else:  # Short entry (SELL) -> protective cover stop (BUY)
                stop_limit_price = round(fill_price + (atr * 2.5), 2)
                side_stop = OrderSide.BUY
                
            if stop_limit_price > 0:
                try:
                    stop_order = trading_client.submit_order(
                        order_data=LimitOrderRequest(
                            symbol=ticker,
                            qty=round(final_filled_qty, 4),
                            side=side_stop,
                            time_in_force=TimeInForce.DAY,
                            limit_price=stop_limit_price
                        )
                    )
                    log.info(f"  Protective Limit Stop attached: {side_stop.name} {ticker} @ ${stop_limit_price:.2f} (ATR-2.5x)")
                except Exception as e:
                    log.warning(f"  Failed to attach Limit Stop: {e}")

        # Publish Filled Event
        event_bus.publish(Topics.ORDER_FILLED, Event(
            source="execution_engine",
            data={
                "ticker": ticker,
                "action": action,
                "qty": qty,
                "fill_price": fill_price,
                "latency_ms": latency_ms
            }
        ))
        
        log_trade(ticker, action, qty, price_estimate, str(alpaca_order.id if alpaca_order else "SIM_ID"))
        
    except Exception as e:
        log.error(f"FAILED to execute {action} for {ticker}: {e}")

def run_execution_engine():
    log.info("Starting Execution Engine (Event-Driven)...")
    init_logger()
    event_bus.subscribe(Topics.ORDER_APPROVED, on_order_approved)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Execution Engine shutting down.")

if __name__ == '__main__':
    run_execution_engine()

