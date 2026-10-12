"""
Execution Engine (The Sniper) — Broker-Agnostic V2
====================================================
Reads APPROVED orders from the event bus.
Connects to the active broker (IBKR) via the unified BaseBroker interface.
Executes live Market/Limit Orders. Logs the final trade and records fill analytics.
"""
import sqlite3
import time
import datetime
import os
import sys
import csv
from dotenv import load_dotenv

# Add brokers to path
_BROKERS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'brokers')
if _BROKERS_DIR not in sys.path:
    sys.path.insert(0, _BROKERS_DIR)

from broker_factory import get_broker
from base_broker import BrokerOrder, OrderSideEnum, OrderTypeEnum, TimeInForceEnum

from logger import get_logger
from paths import DB_PATH, TRADES_CSV, ENV_FILE

log = get_logger("Execution Engine")

LOG_FILE = TRADES_CSV

load_dotenv(ENV_FILE)

# Initialize broker via factory (lazy — connects on first use)
broker = None
try:
    broker = get_broker(auto_connect=True)
    log.info(f"Execution Engine using broker: {broker.name}")
except Exception as e:
    log.error(f"Could not initialize broker: {e}")

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
            writer.writerow(['timestamp', 'ticker', 'action', 'qty', 'estimated_price', 'order_id', 'broker', 'commission'])

def log_trade(ticker, action, qty, price, order_id, broker_name, commission):
    with open(LOG_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.datetime.now().isoformat(),
            ticker, action, qty, f'{price:.2f}', order_id, broker_name, f'{commission:.4f}'
        ])

from event_bus import get_bus, Event, Topics
from execution_analytics import ExecutionAnalytics
from models import Order, SignalType, OrderStatus

event_bus = get_bus()
analytics = ExecutionAnalytics(DB_PATH)

# In-memory deduplication cache for local process thread safety
_processed_order_keys = set()

def on_order_approved(event: Event):
    if not broker or not broker.is_connected():
        log.warning("Broker not connected — cannot execute orders")
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
            acquired = event_bus._redis.set(redis_key, "executed", nx=True, ex=60)
            if not acquired:
                is_duplicate = True
        except Exception as e:
            log.warning(f"Could not verify idempotency in Redis: {e}")
            
    if is_duplicate:
        log.warning(f"Discarding duplicate distributed execution for {ticker} (Sig: {unique_sig})")
        return
        
    _processed_order_keys.add(unique_sig)
    if len(_processed_order_keys) > 1000:
        _processed_order_keys.clear()
    # ----------------------------------------------------------------------------
    
    try:
        # Check market hours via broker
        if not broker.is_market_open():
            log.warning(f"Market closed. Dropping order for {ticker}.")
            return
            
        side = OrderSideEnum.BUY if action == 'BUY' else OrderSideEnum.SELL
        
        # ADAPTIVE EXECUTION: Get latest quote from broker
        bid_price = price_estimate
        ask_price = price_estimate
        mid_price = price_estimate
        half_spread = 0.01
        
        try:
            quote = broker.get_latest_quote(ticker)
            if quote.bid_price > 0 and quote.ask_price > 0:
                bid_price = quote.bid_price
                ask_price = quote.ask_price
                mid_price = quote.mid_price
                half_spread = max(0.01, (ask_price - bid_price) / 2.0)
        except Exception as e:
            log.warning(f"Could not get latest quote for {ticker}: {e}")
            
        # Get Current Inventory for Avellaneda-Stoikov Reservation Pricing
        inventory = 0.0
        try:
            positions = broker.get_positions()
            if ticker in positions:
                inventory = positions[ticker].qty
        except Exception as e:
            log.warning(f"Could not fetch inventory for {ticker}: {e}")
            
        # Get Volatility data from DB
        volatility = 0.02
        adv = 1000000.0
        daily_vol = 0.02
        try:
            conn = _get_db_conn()
            cursor = conn.cursor()
            cursor.execute(
                'SELECT close_price, volume, garch_volatility, volume_ratio '
                'FROM market_data WHERE ticker = ?', (ticker,)
            )
            row = cursor.fetchone()
            if row:
                if row[0] and row[0] > 0:
                    pass  # price already from broker quote
                if row[1] and row[1] > 0:
                    adv = float(row[1]) * 20.0
                if row[2] and row[2] > 0:
                    volatility = float(row[2])
                if row[3] and row[3] > 0:
                    daily_vol = float(row[3]) / 15.87
        except Exception:
            pass

        # Handle fractional shares for IBKR (integers only)
        order_qty = qty
        if not broker.supports_fractional:
            order_qty = max(1, int(round(qty)))
            if order_qty != qty:
                log.info(f"Rounded qty from {qty} to {order_qty} (broker={broker.name} requires integers)")

        # Submit Order via broker abstraction
        broker_order = BrokerOrder(
            symbol=ticker,
            qty=order_qty,
            side=side,
            order_type=OrderTypeEnum.MARKET,
            time_in_force=TimeInForceEnum.DAY
        )
        
        result = broker.place_order(broker_order)
        
        if result.status in ('error', 'rejected'):
            log.warning(f"Broker rejected order for {ticker}: status={result.status}")
            return

        # Base execution friction (10 bps base slippage + inventory risk premium)
        gamma = 0.1
        inventory_risk = abs(inventory) * gamma * (volatility ** 2)
        total_friction_pct = 0.0010 + inventory_risk
            
        # Adverse Selection Curve (simplified — removed dead VPIN dependency)
        fill_ratio = 1.0
        final_filled_qty = float(order_qty) * fill_ratio
        
        broker_fill = result.filled_avg_price if result.filled_avg_price > 0 else price_estimate
        if action == 'BUY':
            fill_price = broker_fill * (1.0 + total_friction_pct)
        else:
            fill_price = broker_fill * (1.0 - total_friction_pct)
            
        latency_ms = (time.time() - start_time) * 1000
        
        # Include broker commission
        commission = result.commission
        
        analytics.record_fill(
            ticker=ticker,
            action=action,
            expected_price=price_estimate,
            fill_price=fill_price,
            qty=final_filled_qty,
            latency_ms=latency_ms,
            commission=commission
        )
        
        log.info(f"EXECUTED: {action} {order_qty}x {ticker} via {broker.name} "
                 f"(fill=${fill_price:.2f}, commission=${commission:.2f})")
        
        # --- DB AUDIT: EXECUTION STATE & SLIPPAGE SYNCHRONIZATION ---
        atr_row = None
        try:
            conn = _get_db_conn()
            cursor = conn.cursor()
            
            # Check if this is a closing fill
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
                log.info(f"DB AUDIT: Closed trade {trade_id} for {ticker} | PnL: ${pnl:.2f} ({pnl_pct*100:.2f}%)")
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
                    log.info(f"DB AUDIT: Updated opening trade {trade_id} for {ticker} | Entry: ${fill_price:.2f}")
            
            conn.commit()
        except Exception as db_err:
            log.error(f"FAILED to audit database execution fills for {ticker}: {db_err}")
            atr_row = None

        # Publish Filled Event
        event_bus.publish(Topics.ORDER_FILLED, Event(
            source="execution_engine",
            data={
                "ticker": ticker,
                "action": action,
                "qty": order_qty,
                "fill_price": fill_price,
                "latency_ms": latency_ms,
                "broker": broker.name,
                "commission": commission
            }
        ))
        
        log_trade(ticker, action, order_qty, price_estimate,
                  result.order_id or "SIM_ID", broker.name, commission)
        
    except Exception as e:
        log.error(f"FAILED to execute {action} for {ticker}: {e}")

def run_execution_engine():
    log.info(f"Starting Execution Engine (Broker: {broker.name if broker else 'NONE'})...")
    init_logger()
    event_bus.subscribe(Topics.ORDER_APPROVED, on_order_approved)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Execution Engine shutting down.")

if __name__ == '__main__':
    run_execution_engine()
