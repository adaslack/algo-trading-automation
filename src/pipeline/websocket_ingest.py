"""
Websocket Data Engine (Intraday - Optimized)
===========================================
Replaces the old Yahoo Finance 5-minute polling with true institutional
websockets via Alpaca. Subscribes to real-time 1-minute bars for the watchlist.

Optimized with yfinance bulk download pre-loading fallbacks.
"""
import sqlite3
import datetime
import pandas as pd
import numpy as np
import os
import asyncio
from dotenv import load_dotenv
from alpaca.data.live import StockDataStream
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from logger import get_logger

# Import feature store & Event Bus
from feature_store import FeatureStore
from event_bus import get_bus, Event, Topics

log = get_logger("Websocket Ingest")

feature_store = FeatureStore()
event_bus = get_bus()

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'trading_brain.db')

load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env'))
API_KEY = os.getenv('ALPACA_API_KEY')
SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')

if not API_KEY or not SECRET_KEY:
    log.error("ALPACA_API_KEY or ALPACA_SECRET_KEY is missing. Websocket cannot start.")
    exit(1)

# Initialize clients
stream = StockDataStream(API_KEY, SECRET_KEY)
hist_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

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
    """Preloads the last 200 minute bars to initialize indicators."""
    log.info(f"Preloading historical minute bars for {len(tickers)} tickers...")
    
    end_dt = datetime.datetime.now(datetime.timezone.utc)
    start_dt = end_dt - datetime.timedelta(days=2) # 2 days of minute bars should be > 200 bars
    
    req = StockBarsRequest(
        symbol_or_symbols=tickers,
        timeframe=TimeFrame.Minute,
        start=start_dt,
        end=end_dt
    )
    
    try:
        bars = hist_client.get_stock_bars(req)
        
        for ticker in tickers:
            if ticker in bars.data:
                df = pd.DataFrame([
                    {'timestamp': b.timestamp, 'open': b.open, 'high': b.high, 'low': b.low, 'close': b.close, 'volume': b.volume}
                    for b in bars.data[ticker]
                ])
                if not df.empty:
                    for col in ['open', 'high', 'low', 'close', 'volume']:
                        df[col] = df[col].astype(np.float32)
                    df.set_index('timestamp', inplace=True)
                    data_cache[ticker] = df.tail(200) # Keep last 200 bars in memory
                    log.info(f"Preloaded {len(data_cache[ticker])} bars for {ticker}")
    except Exception as e:
        log.error(f"Failed to preload history via Alpaca: {e}")
        log.info("Attempting Yahoo Finance bulk download fallback for preloading 1-minute bars...")
        try:
            import yfinance as yf
            # Single bulk download instead of sequential loops (Phase-2 Startup Optimization)
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

async def handle_bar(bar):
    """Callback for Alpaca websocket when a new minute bar closes."""
    ticker = bar.symbol
    if ticker not in data_cache:
        data_cache[ticker] = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
        
    new_data = pd.DataFrame([{
        'open': bar.open, 'high': bar.high, 'low': bar.low, 'close': bar.close, 'volume': bar.volume
    }], index=[bar.timestamp])
    
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
                garch_volatility=?, volume_ratio=?, vwap=?, hurst_exponent=?,
                vpin=?, obi=?, micro_price=?, dealer_gex=?, insider_score=?,
                momentum_12_1=?, reversal_5d=?, volume_breakout=?, vol_regime=?, trend_strength=?
            WHERE ticker=?
        ''', (
            datetime.datetime.now().isoformat(),
            close_p, open_p, high_p, low_p, vol_v,
            features.get('garch_volatility', 0.0), features.get('volume_ratio', 1.0),
            features.get('vwap', close_p), features.get('hurst_exponent', 0.5),
            features.get('vpin', 0.0), features.get('obi', 0.0),
            features.get('micro_price', close_p), features.get('dealer_gex', 0.0),
            features.get('insider_score', 0.0),
            features.get('momentum_12_1', 0.0), features.get('reversal_5d', 0.0),
            features.get('volume_breakout', 0.0), features.get('vol_regime', 0.0),
            features.get('trend_strength', 0.0), ticker
        ))
        
        if cursor.rowcount == 0:
            cursor.execute('''
                INSERT INTO market_data (
                    ticker, timestamp, close_price, open_price, high_price, low_price, volume,
                    garch_volatility, volume_ratio, vwap, hurst_exponent,
                    vpin, obi, micro_price, dealer_gex, insider_score,
                    momentum_12_1, reversal_5d, volume_breakout, vol_regime, trend_strength
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ''', (
                ticker, datetime.datetime.now().isoformat(),
                close_p, open_p, high_p, low_p, vol_v,
                features.get('garch_volatility', 0.0), features.get('volume_ratio', 1.0),
                features.get('vwap', close_p), features.get('hurst_exponent', 0.5),
                features.get('vpin', 0.0), features.get('obi', 0.0),
                features.get('micro_price', close_p), features.get('dealer_gex', 0.0),
                features.get('insider_score', 0.0),
                features.get('momentum_12_1', 0.0), features.get('reversal_5d', 0.0),
                features.get('volume_breakout', 0.0), features.get('vol_regime', 0.0),
                features.get('trend_strength', 0.0)
            ))
            
        conn.commit()
        
        log.debug(f"[STREAM] {ticker} | Price: {close_p:.2f} | VPIN: {features.get('vpin', 0.0):.2f} | OBI: {features.get('obi', 0.0):.2f}")
        
    except Exception as e:
        log.error(f"Error saving features for {ticker}: {e}")

def run_websocket_ingest():
    log.info("Starting Websocket Ingest Engine (Intraday)...")
    tickers = get_watchlist()
    
    if not tickers:
        log.warning("Watchlist empty. Exiting.")
        return
        
    # Preload data
    preload_history(tickers)
    
    try:
        import sys
        scripts_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'scripts')
        sys.path.append(scripts_path)
        from l2_feed_connector import L2FeedConnector
        l2_conn = L2FeedConnector(tickers)
        l2_conn.start()
        log.info("🚀 Institutional L2 Tick Pipeline triggered concurrently.")
    except Exception as e:
        log.warning(f"Could not load concurrent L2 Feed Connector: {e}")
    
    log.info(f"Subscribing to minute bars for {len(tickers)} tickers...")
    stream.subscribe_bars(handle_bar, *tickers)
    
    stream.run()

if __name__ == '__main__':
    run_websocket_ingest()

