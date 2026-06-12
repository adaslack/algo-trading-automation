"""
Institutional Multi-Year Walk-Forward Backtesting Engine (V13 — Optimized)
==========================================================================
V13 Fixes:
  - Eliminated lookahead bias in feature computation
  - Pre-built date→index maps for O(1) lookups (was O(N²))
  - Feature caching from screening step (eliminated 2x computation)
  - HMM regime state reset per day (eliminated cross-ticker leakage)
  - Net-of-cost signal threshold gating

Reports annual metrics (returns, max drawdowns, Sharpe ratios, and active fills)
to evaluate multi-year quantitative performance.
"""

import sys
import os
import numpy as np
import pandas as pd
import yfinance as yf
import concurrent.futures
from datetime import datetime

# Adjust paths to import src/pipeline modules
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'src', 'pipeline'))

from alpha_engine import AlphaEngine, AlphaOutput
from portfolio    import BayesianPortfolio
from feature_store import FeatureStore

# Market Cap Brackets (in USD)
MEGA_CAP = 200_000_000_000
LARGE_CAP = 10_000_000_000
MID_CAP = 2_000_000_000
SMALL_CAP = 300_000_000
MICRO_CAP = 50_000_000

# 60 Candidate Pool covering all 6 cap brackets (10 per bracket)
CANDIDATE_POOL = [
    # Mega Caps
    'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AVGO', 'LLY', 'V',
    # Large Caps
    'AMD', 'INTC', 'PYPL', 'QCOM', 'TXN', 'DIS', 'SBUX', 'NKE', 'SCHW', 'CVS',
    # Mid Caps
    'DBX', 'GPRO', 'RBLX', 'RUN', 'HOOD', 'CHWY', 'PLUG', 'UPST', 'AFRM', 'SOFI',
    # Small Caps
    'BLNK', 'RIOT', 'MARA', 'HUT', 'LAZR', 'NKLA', 'APPS', 'CLSK', 'WKHS', 'SPWR',
    # Micro Caps
    'EVGO', 'HYLN', 'IVR', 'CEI', 'MMAT', 'MULN', 'XELA', 'AEON', 'WTI', 'SCPL',
    # Nano Caps
    'HUSA', 'METX', 'SINT', 'PHUN', 'OPTT', 'BIOL', 'TOPS', 'SHIP', 'GLBS', 'EDSA'
]

# Hardcoded Metadata Fallback Table (bulletproof recovery from Yahoo Finance api limits)
METADATA_FALLBACK = {
    'AAPL': {'marketCap': 3_000_000_000_000, 'sector': 'Technology'},
    'MSFT': {'marketCap': 3_100_000_000_000, 'sector': 'Technology'},
    'NVDA': {'marketCap': 2_200_000_000_000, 'sector': 'Technology'},
    'AMZN': {'marketCap': 1_800_000_000_000, 'sector': 'Consumer Cyclical'},
    'GOOGL': {'marketCap': 1_700_000_000_000, 'sector': 'Technology'},
    'META': {'marketCap': 1_200_000_000_000, 'sector': 'Technology'},
    'TSLA': {'marketCap': 550_000_000_000, 'sector': 'Consumer Cyclical'},
    'AVGO': {'marketCap': 600_000_000_000, 'sector': 'Technology'},
    'LLY': {'marketCap': 700_000_000_000, 'sector': 'Healthcare'},
    'V': {'marketCap': 500_000_000_000, 'sector': 'Financial Services'},
    'AMD': {'marketCap': 180_000_000_000, 'sector': 'Technology'},
    'INTC': {'marketCap': 110_000_000_000, 'sector': 'Technology'},
    'PYPL': {'marketCap': 65_000_000_000, 'sector': 'Financial Services'},
    'QCOM': {'marketCap': 185_000_000_000, 'sector': 'Technology'},
    'TXN': {'marketCap': 150_000_000_000, 'sector': 'Technology'},
    'DIS': {'marketCap': 170_000_000_000, 'sector': 'Consumer Cyclical'},
    'SBUX': {'marketCap': 85_000_000_000, 'sector': 'Consumer Cyclical'},
    'NKE': {'marketCap': 115_000_000_000, 'sector': 'Consumer Cyclical'},
    'SCHW': {'marketCap': 130_000_000_000, 'sector': 'Financial Services'},
    'CVS': {'marketCap': 70_000_000_000, 'sector': 'Healthcare'},
    'DBX': {'marketCap': 8_500_000_000, 'sector': 'Technology'},
    'GPRO': {'marketCap': 2_100_000_000, 'sector': 'Technology'},
    'RBLX': {'marketCap': 9_200_000_000, 'sector': 'Technology'},
    'RUN': {'marketCap': 3_500_000_000, 'sector': 'Technology'},
    'HOOD': {'marketCap': 9_500_000_000, 'sector': 'Financial Services'},
    'CHWY': {'marketCap': 7_800_000_000, 'sector': 'Consumer Cyclical'},
    'PLUG': {'marketCap': 2_200_000_000, 'sector': 'Industrials'},
    'UPST': {'marketCap': 2_500_000_000, 'sector': 'Financial Services'},
    'AFRM': {'marketCap': 8_200_000_000, 'sector': 'Financial Services'},
    'SOFI': {'marketCap': 6_800_000_000, 'sector': 'Financial Services'},
    'BLNK': {'marketCap': 450_000_000, 'sector': 'Consumer Cyclical'},
    'RIOT': {'marketCap': 1_800_000_000, 'sector': 'Technology'},
    'MARA': {'marketCap': 1_900_000_000, 'sector': 'Technology'},
    'HUT': {'marketCap': 650_000_000, 'sector': 'Technology'},
    'LAZR': {'marketCap': 550_000_000, 'sector': 'Technology'},
    'NKLA': {'marketCap': 400_000_000, 'sector': 'Consumer Cyclical'},
    'APPS': {'marketCap': 350_000_000, 'sector': 'Technology'},
    'CLSK': {'marketCap': 1_200_000_000, 'sector': 'Technology'},
    'WKHS': {'marketCap': 320_000_000, 'sector': 'Industrials'},
    'SPWR': {'marketCap': 310_000_000, 'sector': 'Technology'},
    'EVGO': {'marketCap': 280_000_000, 'sector': 'Consumer Cyclical'},
    'HYLN': {'marketCap': 150_000_000, 'sector': 'Industrials'},
    'IVR': {'marketCap': 220_000_000, 'sector': 'Financial Services'},
    'CEI': {'marketCap': 80_000_000, 'sector': 'Energy'},
    'MMAT': {'marketCap': 95_000_000, 'sector': 'Technology'},
    'MULN': {'marketCap': 55_000_000, 'sector': 'Consumer Cyclical'},
    'XELA': {'marketCap': 60_000_000, 'sector': 'Technology'},
    'AEON': {'marketCap': 120_000_000, 'sector': 'Healthcare'},
    'WTI': {'marketCap': 290_000_000, 'sector': 'Energy'},
    'SCPL': {'marketCap': 210_000_000, 'sector': 'Technology'},
    'HUSA': {'marketCap': 15_000_000, 'sector': 'Energy'},
    'METX': {'marketCap': 12_000_000, 'sector': 'Education'},
    'SINT': {'marketCap': 8_000_000, 'sector': 'Industrials'},
    'PHUN': {'marketCap': 35_000_000, 'sector': 'Technology'},
    'OPTT': {'marketCap': 18_000_000, 'sector': 'Industrials'},
    'BIOL': {'marketCap': 9_000_000, 'sector': 'Healthcare'},
    'TOPS': {'marketCap': 5_000_000, 'sector': 'Industrials'},
    'SHIP': {'marketCap': 22_000_000, 'sector': 'Industrials'},
    'GLBS': {'marketCap': 14_000_000, 'sector': 'Industrials'},
    'EDSA': {'marketCap': 7_000_000, 'sector': 'Healthcare'}
}

def fetch_ticker_metadata(ticker):
    """Fetch market cap and sector concurrently with a robust fallback."""
    try:
        info = yf.Ticker(ticker).info
        mc = info.get('marketCap', 0) or 0
        sect = info.get('sector', 'Unknown')
        if mc > 0 and sect != 'Unknown':
            return ticker, mc, sect
    except Exception:
        pass
    fallback = METADATA_FALLBACK.get(ticker, {'marketCap': 0, 'sector': 'Unknown'})
    return ticker, fallback['marketCap'], fallback['sector']

def select_sector_diversified_picks(candidates, max_picks=2):
    """Selects the top liquid candidates from a cap tier enforcing sector-diversification."""
    picks = []
    seen_sectors = {}
    
    # Sort candidates by dollar volume descending
    sorted_candidates = sorted(candidates, key=lambda x: x[2], reverse=True)
    
    for ticker, sector, dollar_vol in sorted_candidates:
        if len(picks) >= max_picks:
            break
            
        # Allow max 1 stock per sector per tier
        sector_count = seen_sectors.get(sector, 0)
        if sector_count < 1:
            picks.append((ticker, sector, dollar_vol))
            seen_sectors[sector] = sector_count + 1
            
    # Fallback to volume sorting if we have empty slots
    if len(picks) < max_picks:
        for ticker, sector, dollar_vol in sorted_candidates:
            if len(picks) >= max_picks:
                break
            if (ticker, sector, dollar_vol) not in picks:
                picks.append((ticker, sector, dollar_vol))
                
    return picks

def get_slippage_rate(market_cap):
    """Institutional cap-bracketed slippage mapping (5 bps to 60 bps)."""
    if market_cap >= 200_000_000_000:
        return 0.0005  # Mega Cap: 5 bps
    elif market_cap >= 10_000_000_000:
        return 0.0010  # Large Cap: 10 bps
    elif market_cap >= 2_000_000_000:
        return 0.0015  # Mid Cap: 15 bps
    elif market_cap >= 300_000_000:
        return 0.0025  # Small Cap: 25 bps
    elif market_cap >= 50_000_000:
        return 0.0040  # Micro Cap: 40 bps
    else:
        return 0.0060  # Nano Cap: 60 bps


def compute_real_features(close_prices, volumes, day_idx):
    return FeatureStore.compute_real_features(close_prices, volumes, day_idx)


def main():
    print("=" * 80)
    print("  INSTITUTIONAL MULTI-YEAR DYNAMIC HISTORICAL BACKTESTING ENGINE (V13)")
    print("  Period: January 1, 2016 to May 22, 2026 (10+ Years Walk-Forward)")
    print("  Alpha: Real Predictive Factors (Momentum, Reversal, Volume, Vol Regime, Trend)")
    print("=" * 80)
    
    # 1. Fetch Ticker Metadata Concurrently
    print("\n[STEP 1] Fetching metadata for 60 universe assets...")
    ticker_metadata = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        results = list(executor.map(fetch_ticker_metadata, CANDIDATE_POOL))
    for ticker, mc, sect in results:
        ticker_metadata[ticker] = {'marketCap': mc, 'sector': sect}
    print(f"Metadata loading complete. Tickers registered: {len(ticker_metadata)} assets.")

    # 2. Bulk Download Historical Price Data
    # Fetching from 2014-12-01 to fully warm up 252-day momentum lookback before Jan 1, 2016
    start_date = "2014-12-01"
    end_date = "2026-05-24"
    print(f"\n[STEP 2] Downloading historical market data via Yahoo Finance...")
    all_tickers = CANDIDATE_POOL + ['SPY', '^VIX']
    
    try:
        df_all = yf.download(all_tickers, start=start_date, end=end_date, group_by='ticker', progress=False)
    except Exception as e:
        print(f"Bulk download error: {e}. Attempting recovery.")
        df_all = None
        
    data = {}
    if df_all is not None:
        for ticker in all_tickers:
            try:
                if ticker in df_all.columns.levels[0]:
                    df = df_all[ticker].dropna(how='all')
                    if not df.empty:
                        data[ticker] = df
            except Exception:
                pass
                
    # Direct individual fallback download for failed tickers
    for ticker in all_tickers:
        if ticker not in data or data[ticker].empty:
            try:
                df = yf.download(ticker, start=start_date, end=end_date, progress=False)
                if not df.empty:
                    data[ticker] = df
            except Exception:
                pass
                
    print(f"Data ingest completed. Tickers loaded: {len(data)} / {len(all_tickers)}")
    
    # 3. Precompute price/volume arrays for each ticker
    print("\n[STEP 3] Pre-computing price and volume arrays...")
    ticker_arrays = {}
    
    for ticker, df in data.items():
        if ticker in ['SPY', '^VIX']:
            continue
        try:
            # V14 Corporate Action Handling: Use Adjusted Close to account for dividends and splits
            col = 'Adj Close' if 'Adj Close' in df.columns else 'Close'
            close_prices = df[col].values.flatten()
            volumes = df['Volume'].values.flatten()
            
            if len(close_prices) > 252:
                ticker_arrays[ticker] = {
                    'close': close_prices,
                    'volume': volumes,
                    'index': df.index,
                    'index_str': list(df.index.strftime('%Y-%m-%d'))
                }
        except Exception as e:
            print(f"  ⚠️ Pre-processing failed for {ticker}: {e}")
            
    print(f"Pre-processing complete. Assets with sufficient history: {len(ticker_arrays)}")

    # V13 Opt 1: Pre-build date→index maps for O(1) lookups (was O(N) per ticker per day)
    print("\n[STEP 3b] Building date→index maps for O(1) lookups...")
    date_to_idx = {}  # {ticker: {date_str: index}}
    for ticker, arrays in ticker_arrays.items():
        date_to_idx[ticker] = {d: i for i, d in enumerate(arrays['index_str'])}
    print(f"Date index maps built for {len(date_to_idx)} tickers.")

    # 4. Set Up Backtest Loop
    engine = AlphaEngine()
    engine.calib_alpha = 0.0
    engine.calib_beta = 1.0  # No downscaling
    portfolio = BayesianPortfolio()
    
    # V14 Alpha Excellence: Train Cross-Sectional ML Ranker & Meta-Label Classifier
    print("\n[STEP 3.5] Training Cross-Sectional ML Ranker & Meta-Label Classifier...")
    try:
        from ml_ranker import build_and_train_models
        ranker, meta_model = build_and_train_models(
            ticker_arrays=ticker_arrays,
            ticker_metadata=ticker_metadata,
            end_date_str="2016-01-01"
        )
        engine.ml_ranker = ranker
        engine.meta_labeler = meta_model
        print("✅ V14 ML Models successfully trained and registered in AlphaEngine!")
    except Exception as e:
        print(f"⚠️ Failed to train ML Models: {e}. Reverting to baseline HMM model.")

    # V13: Store default HMM regime probs for daily reset
    _default_regime_probs = engine._regime_probs.copy()
    
    # Generate all backtesting trading days dynamically from SPY series
    spy_df = data['SPY'].sort_index()
    all_backtest_days = list(spy_df.loc["2016-01-01":"2026-05-22"].index.strftime('%Y-%m-%d'))
    print(f"\n[STEP 4] Found {len(all_backtest_days)} total trading days across the 10+ years window.")
    
    # Group backtesting days by year to report chain-linked performance
    days_by_year = {}
    for day in all_backtest_days:
        year = day[:4]
        if year not in days_by_year:
            days_by_year[year] = []
        days_by_year[year].append(day)
        
    portfolio_value = 100000.0
    global_trades = []
    annual_reports = []
    prev_weights = {}
    
    print("\n--- Starting Vectorized Multi-Year Simulation Loop ---")
    
    for year in sorted(days_by_year.keys()):
        print(f"\n⏳ Running Walk-Forward Simulation for Year {year}...")
        year_days = days_by_year[year]
        year_start_value = portfolio_value
        year_trades = []
        year_values = [portfolio_value]
        
        for idx, day in enumerate(year_days):
            # V13 Bug 6 FIX: Reset HMM regime state at start of each day
            # to prevent cross-ticker state leakage
            engine._regime_probs = _default_regime_probs.copy()
            
            # A. Daily Watchlist Screening (using 20-day rolling dollar volume)
            mega_candidates = []
            large_candidates = []
            mid_candidates = []
            
            # V13 Bug 2 FIX: Cache features during screening to avoid recomputation
            day_feature_cache = {}  # {ticker: features_dict}
            
            for ticker, arrays in ticker_arrays.items():
                # V13 Opt 1: O(1) lookup instead of O(N) linear scan
                dtm = date_to_idx.get(ticker, {})
                
                # Find the last position strictly before this day
                # We need to find the largest index where date < day
                # Use the pre-built map: check if day exists, use day_idx-1; else find prior
                if day in dtm:
                    # V13 Bug 3 FIX: Use day_idx-1 to avoid using today's data in features
                    last_pos = dtm[day] - 1
                else:
                    # Find the closest prior day
                    last_pos = -1
                    for d_str, d_idx in dtm.items():
                        if d_str < day and d_idx > last_pos:
                            last_pos = d_idx
                
                if last_pos < 252:  # Need 252 days for momentum
                    continue
                
                features = compute_real_features(arrays['close'], arrays['volume'], last_pos)
                if features is None:
                    continue
                
                # Cache features for reuse in alpha evaluation step
                day_feature_cache[ticker] = (features, last_pos)
                    
                dollar_vol = features['dollar_vol_20d']
                
                meta = ticker_metadata[ticker]
                mc = meta['marketCap']
                sect = meta['sector']
                entry = (ticker, sect, dollar_vol)
                
                if mc >= MEGA_CAP:
                    mega_candidates.append(entry)
                elif mc >= LARGE_CAP:
                    large_candidates.append(entry)
                elif mc >= MID_CAP:
                    mid_candidates.append(entry)
                    
            # Select exactly 4 sector-diversified picks per tier across Mega, Large, and Mid Cap tiers (highly liquid)
            selected_mega = select_sector_diversified_picks(mega_candidates, 4)
            selected_large = select_sector_diversified_picks(large_candidates, 4)
            selected_mid = select_sector_diversified_picks(mid_candidates, 4)
            
            active_watchlist = []
            for tier in [selected_mega, selected_large, selected_mid]:
                for ticker, _, _ in tier:
                    active_watchlist.append(ticker)
                    
            # B. Ingest Real VIX close price
            try:
                vix = float(data['^VIX'].loc[day]['Close'])
            except Exception:
                vix = 14.0
                
            # C. Evaluate Alpha Scores using REAL predictive features
            alpha_outputs = []
            for ticker in active_watchlist:
                # V13 Bug 2 FIX: Reuse cached features from screening step
                if ticker in day_feature_cache:
                    features, cached_day_idx = day_feature_cache[ticker]
                else:
                    # Fallback: compute if not cached (shouldn't happen normally)
                    arrays = ticker_arrays[ticker]
                    dtm = date_to_idx.get(ticker, {})
                    if day in dtm:
                        day_idx = dtm[day] - 1
                    else:
                        day_idx = -1
                        for d_str, d_idx in dtm.items():
                            if d_str <= day and d_idx > day_idx:
                                day_idx = d_idx
                    if day_idx < 252:
                        continue
                    features = compute_real_features(arrays['close'], arrays['volume'], day_idx)
                    if features is None:
                        continue
                    
                snapshot = features  # Already in the right format
                out = engine.evaluate(ticker, snapshot, vix=vix)
                alpha_outputs.append(out)
                    
            if not alpha_outputs:
                continue
                
            # D. Portfolio Sizing
            price_history = {}
            for t in active_watchlist:
                if t in ticker_arrays:
                    arrays = ticker_arrays[t]
                    dtm = date_to_idx.get(t, {})
                    if day in dtm:
                        last_pos = dtm[day]
                    else:
                        last_pos = -1
                        for d_str, d_idx in dtm.items():
                            if d_str < day and d_idx > last_pos:
                                last_pos = d_idx
                    if last_pos >= 9:
                        start_pos = max(0, last_pos - 9)
                        price_history[t] = list(arrays['close'][start_pos:last_pos+1])
                    
            allocations = portfolio.size_portfolio(alpha_outputs, price_history, portfolio_value)
            
            # E. Process Fills on next-day realized returns
            active_allocations = {t: w for t, w in allocations.items() if abs(w) > 0.0001}
            
            # Combine all tickers that were active yesterday or are active today
            all_active_tickers = set(active_allocations.keys()) | set(prev_weights.keys())
            
            next_day_pnl = 0.0
            for t in all_active_tickers:
                w_new = active_allocations.get(t, 0.0)
                w_prev = prev_weights.get(t, 0.0)
                
                # Fetch ticker market cap for slippage determination
                meta = ticker_metadata.get(t, {'marketCap': 1e9, 'sector': 'Unknown'})
                mc = meta['marketCap']
                slippage_rate = get_slippage_rate(mc)
                
                # 1. Transaction cost based on trade turnover
                turnover = abs(w_new - w_prev)
                tc = turnover * slippage_rate * portfolio_value
                
                # 2. Next-day realized returns (if position is open today)
                realized_ret = 0.0
                if abs(w_new) > 0.0 and t in ticker_arrays:
                    arrays = ticker_arrays[t]
                    dtm = date_to_idx.get(t, {})
                    try:
                        if day in dtm:
                            curr_idx = dtm[day]
                            if curr_idx + 1 < len(arrays['close']):
                                next_close = float(arrays['close'][curr_idx + 1])
                                curr_close = float(arrays['close'][curr_idx])
                                realized_ret = (next_close - curr_close) / curr_close
                    except Exception:
                        realized_ret = 0.0
                
                # Raw PnL (natively handles both long and short signs cleanly)
                raw_pnl = portfolio_value * w_new * realized_ret
                
                # Net PnL (raw PnL minus turnover execution cost)
                trade_pnl = raw_pnl - tc
                next_day_pnl += trade_pnl
                
                # 3. Calculate net return for analytics/reporting
                if abs(w_new) > 0.0:
                    raw_direction_ret = realized_ret if w_new > 0 else -realized_ret
                    prop_tc_ret = (turnover * slippage_rate) / abs(w_new) if abs(w_new) > 0.001 else 0.0
                    net_ret = raw_direction_ret - prop_tc_ret
                else:
                    net_ret = -slippage_rate
                
                if abs(w_new) > 0.0001 or abs(w_prev) > 0.0001:
                    trade_record = {
                        'date': day,
                        'ticker': t,
                        'weight': w_new,
                        'raw_ret': realized_ret,
                        'net_ret': net_ret,
                        'pnl': trade_pnl,
                        'year': year
                    }
                    year_trades.append(trade_record)
                    global_trades.append(trade_record)
                    
            portfolio_value += next_day_pnl
            year_values.append(portfolio_value)
            
            # Save target weights for next day's turnover calculation
            prev_weights = active_allocations.copy()
            
        # Calculate Annual Performance Metrics
        year_return_pct = (portfolio_value - year_start_value) / year_start_value * 100
        year_daily_rets = np.diff(year_values) / year_values[:-1]
        
        avg_ret = np.mean(year_daily_rets) if len(year_daily_rets) > 0 else 0.0
        std_ret = np.std(year_daily_rets) if len(year_daily_rets) > 1 else 1e-6
        year_sharpe = (avg_ret / max(std_ret, 1e-8)) * np.sqrt(252) if len(year_daily_rets) > 0 else 0.0
        
        trades_df = pd.DataFrame(year_trades)
        if not trades_df.empty:
            wins = trades_df[trades_df['net_ret'] > 0]
            year_win_rate = len(wins) / len(trades_df) * 100
        else:
            year_win_rate = 0.0
            
        # Max Drawdown
        values_arr = np.array(year_values)
        peaks = np.maximum.accumulate(values_arr)
        drawdowns = (values_arr - peaks) / peaks
        year_max_dd = np.min(drawdowns) * 100 if len(drawdowns) > 0 else 0.0
        
        print(f"  📊 Year {year} Performance Audit:")
        print(f"    Return       : {year_return_pct:+.2f}%")
        print(f"    Sharpe Ratio : {year_sharpe:.2f}")
        print(f"    Max Drawdown : {year_max_dd:.2f}%")
        print(f"    Win Rate     : {year_win_rate:.1f}%")
        print(f"    Total Fills  : {len(year_trades)}")
        
        annual_reports.append({
            'year': year,
            'return': year_return_pct,
            'sharpe': year_sharpe,
            'max_dd': year_max_dd,
            'win_rate': year_win_rate,
            'fills': len(year_trades),
            'end_value': portfolio_value
        })
        
    # 5. Final Performance Audit Report
    print("\n" + "=" * 80)
    print("  INSTITUTIONAL 5-YEAR PERFORMANCE COMPILATION SUMMARY (V13 REAL FACTORS)")
    print("=" * 80)
    
    global_trades_df = pd.DataFrame(global_trades)
    total_net_return = (portfolio_value - 100000.0) / 100000.0 * 100
    
    print(f"\n{'YEAR'.ljust(8)} | {'NET RETURN'.ljust(12)} | {'SHARPE'.ljust(8)} | {'MAX DD'.ljust(8)} | {'WIN RATE'.ljust(10)} | {'FILLS'.ljust(6)} | {'CAPITAL'.ljust(12)}")
    print("-" * 80)
    for rep in annual_reports:
        print(f"{rep['year'].ljust(8)} | {f'{rep['return']:+.2f}%'.ljust(12)} | {f'{rep['sharpe']:.2f}'.ljust(8)} | {f'{rep['max_dd']:.2f}%'.ljust(8)} | {f'{rep['win_rate']:.1f}%'.ljust(10)} | {str(rep['fills']).ljust(6)} | {f'${rep['end_value']:,.2f}'.ljust(12)}")
    print("-" * 80)
    
    if not global_trades_df.empty:
        total_wins = global_trades_df[global_trades_df['net_ret'] > 0]
        global_win_rate = len(total_wins) / len(global_trades_df) * 100
    else:
        global_win_rate = 0.0
        
    print(f"\n🏆  Total Cumulative Return : {total_net_return:+.2f}% (Jan 2016 - May 2026)")
    print(f"🎯  Global Trade Win Rate    : {global_win_rate:.1f}%")
    print(f"📈  Total Recorded Fills    : {len(global_trades_df)} trades")
    print("=" * 80)
    
    # Save cumulative fills locally in CSV format
    os.makedirs("data", exist_ok=True)
    global_trades_df.to_csv("data/paper_trades.csv", index=False)
    print("Historical fills successfully exported to: data/paper_trades.csv")

if __name__ == '__main__':
    main()

