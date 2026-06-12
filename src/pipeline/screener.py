"""
Institutional Multi-Cap Diversified Screener (V11.5 Upgrade)
============================================================
Runs once per day before market open to identify candidate equities.
Dynamically scans the US stock market and classifies assets into 6 distinct
market capitalization tiers to guarantee deep statistical coverage.

Market Cap Brackets:
  1. Mega Cap  : $200B and more
  2. Large Cap : $10B to $200B
  3. Mid Cap   : $2B to $10B
  4. Small Cap : $300M to $2B
  5. Micro Cap : $50M to $300M
  6. Nano Cap  : under $50M

Optimized for Stage 1 (16GB RAM + 4 Cores) systems:
- Universe constrained to exactly 5 picks per tier = 30 assets + anchor (SPY).
- Implements active sector-diversification constraints to prevent correlation traps.
- Leverages concurrent ThreadPoolExecutor for high-frequency yfinance queries.
"""

import os
import sqlite3
import yfinance as yf
import concurrent.futures
from dotenv import load_dotenv
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass, AssetStatus
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest
from logger import get_logger

log = get_logger("Screener")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'trading_brain.db')

# Market Cap Brackets (in USD)
MEGA_CAP = 200_000_000_000
LARGE_CAP = 10_000_000_000
MID_CAP = 2_000_000_000
SMALL_CAP = 300_000_000
MICRO_CAP = 50_000_000
# Nano Cap = < $50M

PICKS_PER_TIER = 2  # 2 picks per tier * 6 tiers = 12 assets total (Ultra-focused gold ratio for 16GB RAM L2/HMM processing)

def fetch_ticker_data(ticker):
    """Worker function for concurrent yfinance fetching."""
    try:
        info = yf.Ticker(ticker).info
        market_cap = info.get('marketCap', 0) or 0
        sector = info.get('sector', 'Unknown')
        return ticker, market_cap, sector
    except Exception:
        return ticker, 0, 'Unknown'

def select_sector_diversified_picks(candidates, max_picks=5):
    """
    Selects the top liquid candidates from a cap tier while enforcing
    active sector-diversification to prevent portfolio correlation traps.
    """
    picks = []
    seen_sectors = {}
    
    # Sort candidates by dollar volume descending
    sorted_candidates = sorted(candidates, key=lambda x: x[2], reverse=True)
    
    for ticker, sector, dollar_vol in sorted_candidates:
        if len(picks) >= max_picks:
            break
            
        # Allow maximum 2 stocks from the same sector in each tier to enforce diversification
        sector_count = seen_sectors.get(sector, 0)
        if sector_count < 2:
            picks.append((ticker, sector, dollar_vol))
            seen_sectors[sector] = sector_count + 1
            
    # Fallback to volume sorting if we have empty slots due to strict sector filters
    if len(picks) < max_picks:
        for ticker, sector, dollar_vol in sorted_candidates:
            if len(picks) >= max_picks:
                break
            if (ticker, sector, dollar_vol) not in picks:
                picks.append((ticker, sector, dollar_vol))
                
    return picks

def run_screener():
    log.info("=" * 60)
    log.info("  MULTI-CAP DIVERSIFIED SECTOR SCREENER (V11.5) ONLINE")
    log.info("=" * 60)
    
    load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env'))
    API_KEY = os.getenv('ALPACA_API_KEY')
    SECRET_KEY = os.getenv('ALPACA_SECRET_KEY')
    
    if not API_KEY or not SECRET_KEY:
        log.error("Alpaca keys missing. Cannot run screener.")
        return
    
    trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)
    data_client = StockHistoricalDataClient(API_KEY, SECRET_KEY)
    
    log.info("Fetching all active US equities from Alpaca...")
    req = GetAssetsRequest(asset_class=AssetClass.US_EQUITY, status=AssetStatus.ACTIVE)
    assets = trading_client.get_all_assets(req)
    
    # Filter for marginable and fractionable stocks
    valid_symbols = [a.symbol for a in assets if a.tradable and a.marginable and a.fractionable]
    log.info(f"Found {len(valid_symbols)} tradable/fractionable US stocks.")
    
    log.info("Querying volume data for liquidity filtering...")
    
    # Chunk requests to avoid Alpaca URL length limits (Chunk by 500)
    chunk_size = 500
    volumes = {}
    
    for i in range(0, len(valid_symbols), chunk_size):
        chunk = valid_symbols[i:i+chunk_size]
        try:
            snap_req = StockSnapshotRequest(symbol_or_symbols=chunk)
            snapshots = data_client.get_stock_snapshot(snap_req)
            for symbol, snap in snapshots.items():
                if snap.previous_daily_bar:
                    volumes[symbol] = snap.previous_daily_bar.volume * snap.previous_daily_bar.close
        except Exception:
            pass
            
    # Sort by dollar volume descending, take top 250 candidates
    sorted_tickers = sorted(volumes.keys(), key=lambda x: volumes[x], reverse=True)
    candidates = sorted_tickers[:250]
    
    log.info(f"Pre-filtered to top {len(candidates)} candidates by dollar volume.")
    log.info("Fetching Market Cap & Sector data concurrently from Yahoo Finance...")
    
    # Initialize cap bracket queues
    mega_cap = []
    large_cap = []
    mid_cap = []
    small_cap = []
    micro_cap = []
    nano_cap = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(fetch_ticker_data, candidates))
        
    for ticker, market_cap, sector in results:
        dollar_vol = volumes.get(ticker, 0)
        entry = (ticker, sector, dollar_vol)
        
        if market_cap >= MEGA_CAP:
            mega_cap.append(entry)
        elif market_cap >= LARGE_CAP:
            large_cap.append(entry)
        elif market_cap >= MID_CAP:
            mid_cap.append(entry)
        elif market_cap >= SMALL_CAP:
            small_cap.append(entry)
        elif market_cap >= MICRO_CAP:
            micro_cap.append(entry)
        elif market_cap > 0:
            nano_cap.append(entry)
            
    # Select sector-diversified picks from each tier
    selected_mega = select_sector_diversified_picks(mega_cap, PICKS_PER_TIER)
    selected_large = select_sector_diversified_picks(large_cap, PICKS_PER_TIER)
    selected_mid = select_sector_diversified_picks(mid_cap, PICKS_PER_TIER)
    selected_small = select_sector_diversified_picks(small_cap, PICKS_PER_TIER)
    selected_micro = select_sector_diversified_picks(micro_cap, PICKS_PER_TIER)
    selected_nano = select_sector_diversified_picks(nano_cap, PICKS_PER_TIER)
    
    log.info(f"")
    log.info(f"  Mega Cap  ({len(selected_mega)}): {[t[0] for t in selected_mega]}")
    log.info(f"  Large Cap ({len(selected_large)}): {[t[0] for t in selected_large]}")
    log.info(f"  Mid Cap   ({len(selected_mid)}): {[t[0] for t in selected_mid]}")
    log.info(f"  Small Cap ({len(selected_small)}): {[t[0] for t in selected_small]}")
    log.info(f"  Micro Cap ({len(selected_micro)}): {[t[0] for t in selected_micro]}")
    log.info(f"  Nano Cap  ({len(selected_nano)}): {[t[0] for t in selected_nano]}")
    
    # Build final watchlist
    watchlist = []
    
    # Always include SPY as our market regime tracking anchor
    watchlist.append(('SPY', 'Market'))
    
    for tier in [selected_mega, selected_large, selected_mid, selected_small, selected_micro, selected_nano]:
        for ticker, sector, _ in tier:
            watchlist.append((ticker, sector))
            
    total_stocks = len(watchlist)
    log.info(f"Final Watched Universe: {total_stocks} assets (1 anchor + {total_stocks - 1} diversified equities)")
    
    # Save to database (using dynamic db_adapter)
    import db_adapter
    conn = db_adapter.get_connection()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM daily_watchlist')
    
    for ticker, sector in watchlist:
        # DuckDB and Postgres both support standard ON CONFLICT clauses
        cursor.execute('INSERT INTO daily_watchlist (ticker, sector) VALUES (?, ?) ON CONFLICT DO NOTHING', (ticker, sector))
        
    try:
        conn.commit()
    except Exception:
        pass
    conn.close()
    
    log.info("Watchlist updated successfully in high-performance columnar DuckDB.")

if __name__ == '__main__':
    run_screener()

