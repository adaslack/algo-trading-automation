"""
Websocket Data Engine (Intraday - Broker-Agnostic V2)
=======================================================
Replaces the old Yahoo Finance 5-minute polling with real institutional
websockets via the unified broker interface. Subscribes to real-time
1-minute bars for the watchlist using whatever broker is configured.

Supports: IBKR reqRealTimeBars, yfinance fallback.
"""
import sqlite3
import datetime
import pandas as pd
import numpy as np
import os
import sys
import asyncio
from dotenv import load_dotenv

# Add brokers to path
_BROKERS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'brokers')
if _BROKERS_DIR not in sys.path:
    sys.path.insert(0, _BROKERS_DIR)

from broker_factory import get_broker

from logger import get_logger

# Import feature store & Event Bus
from feature_store import FeatureStore
from event_bus import get_bus, Event, Topics

log = get_logger("Websocket Ingest")

feature_store = FeatureStore()
event_bus = get_bus()

from paths import DB_PATH

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env'))

# Initialize broker (lazy)
broker = None
try:
    broker = get_broker(auto_connect=True)
    log.info(f"Websocket Ingest using broker: {broker.name}")
except Exception as e:
    log.error(f"Could not initialize broker for Websocket Ingest: {e}")

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

# In-memory dataframe storage for fast indicator calculation
data_cache = {}

def get_watchlist():
    try:
        conn = _get_db_conn()
        c = conn.cursor()
        c.execute('SELECT ticker FROM daily_watchlist')
        tickers = [r[0] for r in c.fetchall()]
        return tickers
    except Exception as e:
        log.error(f"Could not read watchlist: {e}")
        return []

def preload_history(tickers):
    """Preloads the last 200 minute bars via the broker or yfinance fallback."""
    log.info(f"Preloading historical minute bars for {len(tickers)} tickers...")
    
    end_dt = datetime.datetime.now(datetime.timezone.utc)
    start_dt = end_dt - datetime.timedelta(days=2)  # 2 days of minute bars > 200 bars
    
    # Try broker API first
    if broker and broker.is_connected():
        try:
            bars_data = broker.get_historical_bars(tickers, '1Min', start_dt, end_dt)
            for ticker in tickers:
                if ticker in bars_data and bars_data[ticker]:
                    df = pd.DataFrame([
                        {'timestamp': b.timestamp, 'open': b.open, 'high': b.high,
                         'low': b.low, 'close': b.close, 'volume': b.volume}
                        for b in bars_data[ticker]
                    ])
                    if not df.empty:
                        for col in ['open', 'high', 'low', 'close', 'volume']:
                            df[col] = df[col].astype(np.float32)
                        df.set_index('timestamp', inplace=True)
                        data_cache[ticker] = df.tail(200)
                        log.info(f"Preloaded {len(data_cache[ticker])} bars for {ticker} via {broker.name}")
            return  # Success via broker
        except Exception as e:
            log.error(f"Failed to preload history via broker: {e}")
    
    # Fallback to yfinance bulk download
    log.info("Attempting Yahoo Finance bulk download fallback for preloading 1-minute bars...")
    try:
        import yfinance as yf
        df = yf.download(tickers, period='5d', interval='1m', group_by='ticker', progress=False)
        
        for ticker in tickers:
            ticker_df = None
            if len(tickers) == 1:
                ticker_df = df.copy()
            elif ticker in df.columns.get_level_values(0):
                ticker_df = df[ticker].copy()
                
            if ticker_df is not None and not ticker_df.empty:
                ticker_df.rename(columns={
                    'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'
                }, inplace=True)
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    ticker_df[col] = ticker_df[col].astype(np.float32)
                ticker_df.index = pd.to_datetime(ticker_df.index)
                if ticker_df.index.tz is None:
                    ticker_df = ticker_df.tz_localize('UTC')
                else:
                    ticker_df = ticker_df.tz_convert('UTC')
                
                data_cache[ticker] = ticker_df.tail(200)
                log.info(f"  [yfinance bulk] Preloaded {len(data_cache[ticker])} bars for {ticker}")
    except Exception as yf_err:
        log.error(f"  [yfinance bulk] Bulk preloading failed: {yf_err}")

# Alternative Data Cache
alt_data_cache = {}

def on_alt_data_update(event: Event):
    ticker = event.data.get('ticker')
    if ticker:
        if ticker not in alt_data_cache:
            alt_data_cache[ticker] = {}
        alt_data_cache[ticker].update(event.data)

# Register listener
event_bus.subscribe(Topics.ALT_DATA_UPDATE, on_alt_data_update)

def handle_bar_callback(symbol, broker_bar):
    """Callback for broker when a new bar arrives (works for active broker)."""
    ticker = symbol
    if ticker not in data_cache:
        data_cache[ticker] = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        
    new_data = pd.DataFrame([{
        'open': broker_bar.open, 'high': broker_bar.high, 'low': broker_bar.low,
        'close': broker_bar.close, 'volume': broker_bar.volume
    }], index=[broker_bar.timestamp])
    
    data_cache[ticker] = pd.concat([data_cache[ticker], new_data]).tail(200)
    df = data_cache[ticker].copy().astype(np.float32)
    
    if len(df) < 50:
        return
        
    try:
        alt_data = alt_data_cache.get(ticker, {})
        features = feature_store.compute_features(ticker, df, alt_data=alt_data)
        
        if features:
            event_bus.publish_async(Topics.MARKET_UPDATE, Event(
                source="websocket_ingest",
                data={
                    "ticker": ticker,
                    "features": features
                }
            ))
    except Exception as e:
        log.error(f"Error computing features for {ticker}: {e}")

    # Save only institutional features to DB
    try:
        features = features or {}
        close_p = float(df['close'].iloc[-1])
        open_p = float(df['open'].iloc[-1])
        high_p = float(df['high'].iloc[-1])
        low_p = float(df['low'].iloc[-1])
        vol_v = float(df['volume'].iloc[-1])
        
        conn = _get_db_conn()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE market_data SET
                timestamp=?, close_price=?, open_price=?, high_price=?, low_price=?, volume=?,
                garch_volatility=?, volume_ratio=?, momentum_12_1=?, reversal_5d=?, vol_regime=?
            WHERE ticker=?
        ''', (
            datetime.datetime.now().isoformat(),
            close_p, open_p, high_p, low_p, vol_v,
            features.get('garch_volatility', 0.0), features.get('volume_ratio', 1.0),
            features.get('momentum_12_1', 0.0), features.get('reversal_5d', 0.0),
            features.get('vol_regime', 0.0), ticker
        ))
        
        if cursor.rowcount == 0:
            cursor.execute('''
                INSERT INTO market_data (
                    ticker, timestamp, close_price, open_price, high_price, low_price, volume,
                    garch_volatility, volume_ratio, momentum_12_1, reversal_5d, vol_regime
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (
                ticker, datetime.datetime.now().isoformat(),
                close_p, open_p, high_p, low_p, vol_v,
                features.get('garch_volatility', 0.0), features.get('volume_ratio', 1.0),
                features.get('momentum_12_1', 0.0), features.get('reversal_5d', 0.0),
                features.get('vol_regime', 0.0)
            ))
            
        conn.commit()
        
        log.debug(f"[STREAM] {ticker} | Price: {close_p:.2f} | Momentum: {features.get('momentum_12_1', 0.0):.2f}")
        
    except Exception as e:
        log.error(f"Error saving features for {ticker}: {e}")

def run_websocket_ingest():
    log.info(f"Starting Websocket Ingest Engine (Broker: {broker.name if broker else 'yfinance fallback'})...")
    tickers = get_watchlist()
    
    if not tickers:
        log.warning("Watchlist empty. Exiting.")
        return
        
    # Preload data
    preload_history(tickers)
    
    # Real-time minute bars are handled via the unified broker interface below
    
    # Subscribe to real-time bars via broker
    if broker and broker.is_connected():
        log.info(f"Subscribing to minute bars for {len(tickers)} tickers via {broker.name}...")
        broker.subscribe_bars(tickers, handle_bar_callback)
        broker.run_stream()
    else:
        log.warning("No broker connected. Running in yfinance polling fallback mode.")
        import yfinance as yf
        while True:
            try:
                for ticker in tickers:
                    tk = yf.Ticker(ticker)
                    hist = tk.history(period='1d', interval='1m')
                    if not hist.empty:
                        latest = hist.iloc[-1]
                        from base_broker import BrokerBar
                        bar = BrokerBar(
                            timestamp=hist.index[-1],
                            open=float(latest['Open']),
                            high=float(latest['High']),
                            low=float(latest['Low']),
                            close=float(latest['Close']),
                            volume=float(latest['Volume'])
                        )
                        handle_bar_callback(ticker, bar)
                import time
                time.sleep(60)  # Poll every minute
            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"yfinance polling error: {e}")
                import time
                time.sleep(60)

if __name__ == '__main__':
    run_websocket_ingest()
