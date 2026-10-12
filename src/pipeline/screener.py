"""
Institutional Multi-Cap Diversified Screener (V11.5 - Broker-Agnostic V2)
=========================================================================
Runs once per day before market open to identify candidate equities.
Dynamically scans the US stock market and classifies assets into 6 distinct
market capitalization tiers to guarantee deep statistical coverage.

Now supports IBKR via the unified broker interface,
with yfinance fallback for market cap and sector data.
"""

import os
import sys
import sqlite3
import yfinance as yf
import concurrent.futures
from dotenv import load_dotenv

# Add brokers to path
_BROKERS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'brokers')
if _BROKERS_DIR not in sys.path:
    sys.path.insert(0, _BROKERS_DIR)

from broker_factory import get_broker

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
        avg_vol = info.get('averageVolume', 1000000) or 1000000
        prev_close = info.get('previousClose', 10.0) or 10.0
        dollar_vol = avg_vol * prev_close
        company_name = info.get('longName') or info.get('shortName') or ticker
        return ticker, market_cap, sector, dollar_vol, company_name
    except Exception:
        return ticker, 0, 'Unknown', 0, ticker

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
    
    # Get broker and fetch tradable assets
    broker = None
    try:
        broker = get_broker(auto_connect=True)
        log.info(f"Screener using broker: {broker.name}")
    except Exception as e:
        log.warning(f"Could not connect broker for screener: {e}")
    
    candidates = []
    
    if broker and broker.is_connected():
        log.info(f"Fetching tradable US equities from {broker.name}...")
        try:
            assets = broker.get_tradable_assets()
            valid_symbols = [a.symbol for a in assets if a.tradable]
            log.info(f"Found {len(valid_symbols)} tradable US stocks via {broker.name}.")
            # Take top 250 by name (we'll sort by volume via yfinance)
            candidates = valid_symbols[:250]
        except Exception as e:
            log.warning(f"Failed to get assets from broker: {e}")
    
    if not candidates:
        # Fallback: use a curated list of liquid US equities
        log.info("Using curated fallback universe for screening...")
        import yfinance as yf
        # Get S&P 500 + popular liquid stocks
        try:
            import pandas as pd
            sp500_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
            sp500_table = pd.read_html(sp500_url)
            candidates = sp500_table[0]['Symbol'].str.replace('.', '-').tolist()[:250]
        except Exception:
            candidates = [
                'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK-B',
                'JPM', 'V', 'UNH', 'MA', 'HD', 'PG', 'XOM', 'JNJ', 'COST', 'ABBV',
                'MRK', 'CVX', 'KO', 'PEP', 'AVGO', 'LLY', 'TMO', 'WMT', 'MCD',
                'CSCO', 'ACN', 'ABT', 'DHR', 'TXN', 'NEE', 'PM', 'VZ', 'UPS',
                'RTX', 'HON', 'INTC', 'QCOM', 'LOW', 'INTU', 'SPGI', 'BA', 'GE',
                'AMD', 'CAT', 'BLK', 'DE', 'AMAT', 'ISRG', 'ADP', 'GILD', 'MDLZ',
                'SYK', 'ADI', 'TJX', 'MMC', 'BKNG', 'CI', 'CVS', 'LRCX', 'ZTS',
            ]
    
    log.info(f"Pre-filtered to {len(candidates)} candidates.")
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
        
    company_names = {}
    for ticker, market_cap, sector, dollar_vol, company_name in results:
        company_names[ticker] = company_name
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
    
    use_postgres = bool(os.getenv("DB_HOST"))
    def run_sql(sql, params=None):
        if use_postgres:
            sql = sql.replace('?', '%s')
        cursor.execute(sql, params or ())
        
    run_sql('DELETE FROM daily_watchlist')
    
    # Anchor name
    company_names['SPY'] = 'SPDR S&P 500 ETF Trust'
    
    for ticker, sector in watchlist:
        name = company_names.get(ticker, ticker)
        # DuckDB and Postgres both support standard ON CONFLICT clauses
        run_sql('INSERT INTO daily_watchlist (ticker, sector, company_name) VALUES (?, ?, ?) ON CONFLICT DO NOTHING', (ticker, sector, name))
        
    # Calculate pairwise correlation matrix offline
    log.info("Calculating pairwise correlation matrix offline for all watchlist assets...")
    try:
        import yfinance as yf
        import pandas as pd
        
        tickers_list = [t[0] for t in watchlist]
        df = yf.download(tickers_list, period='30d', interval='1d', progress=False)
        
        if isinstance(df.columns, pd.MultiIndex) and 'Close' in df.columns:
            close_df = df['Close']
        elif 'Close' in df.columns:
            close_df = df['Close']
        else:
            close_df = df
            
        corr_matrix = close_df.corr().fillna(0.0)
        
        run_sql('DELETE FROM watchlist_correlation')
        
        inserted_count = 0
        for col_a in corr_matrix.columns:
            for col_b in corr_matrix.columns:
                if col_a < col_b:
                    coef = float(corr_matrix.loc[col_a, col_b])
                    run_sql(
                        'INSERT INTO watchlist_correlation (ticker_a, ticker_b, correlation) VALUES (?, ?, ?)',
                        (col_a, col_b, coef)
                    )
                    inserted_count += 1
                    
        log.info(f"Calculated and saved {inserted_count} correlation coefficients in database.")
    except Exception as corr_err:
        log.error(f"Failed to calculate and store correlation matrix offline: {corr_err}")

    try:
        conn.commit()
    except Exception:
        pass
    conn.close()
    
    log.info("Watchlist updated successfully in high-performance columnar DuckDB.")

if __name__ == '__main__':
    run_screener()
