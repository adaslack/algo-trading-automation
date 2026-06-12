"""
Cross-Sectional ML Ranker & Meta-Labeling Model (V14 Alpha Excellence)
======================================================================
Implements:
1. HistGradientBoostingRegressor for cross-sectional relative return percentile ranking.
2. HistGradientBoostingClassifier for secondary Meta-Label filtering.
"""
import numpy as np
import pandas as pd
from typing import Optional, Dict, List, Tuple
from sklearn.ensemble import HistGradientBoostingRegressor, HistGradientBoostingClassifier
from logger import get_logger

log = get_logger("MLRanker")

# Core features used for cross-sectional ranking
FEATURE_NAMES = [
    'momentum_12_1',
    'reversal_5d',
    'realized_volatility',
    'volume_breakout',
    'rolling_correlation',
    'rolling_beta',
    'volatility_expansion',
    'drawdown_depth',
    'sector_relative_strength'
]

# Secondary macro/micro features used for meta-labeling
META_FEATURE_NAMES = [
    'spread',
    'VIX',
    'liquidity',
    'signal_crowding',
    'volatility',
    'sector_regime'
]


class CrossSectionalMLRanker:
    """
    Predicts the future 5-day relative return percentile using HistGradientBoostingRegressor.
    Ranks stocks from best expected relative return (percentile close to 100) to worst.
    """
    def __init__(self):
        self.model = HistGradientBoostingRegressor(
            max_iter=100,
            learning_rate=0.05,
            max_leaf_nodes=31,
            min_samples_leaf=20,
            random_state=42
        )
        self.is_fitted = False

    def fit(self, X: np.ndarray, y: np.ndarray):
        """Fit the gradient boosting ranker."""
        try:
            if len(X) < 50:
                log.warning("Insufficient samples to train ML Ranker. Model not trained.")
                return
            log.info(f"Training CrossSectionalMLRanker on {len(X)} samples with {X.shape[1]} features...")
            self.model.fit(X, y)
            self.is_fitted = True
            log.info("CrossSectionalMLRanker successfully fitted.")
        except Exception as e:
            log.error(f"Failed to fit CrossSectionalMLRanker: {e}")

    def predict(self, X: np.ndarray) -> np.ndarray:
        """Predict expected percentile ranks."""
        if not self.is_fitted:
            # Fallback: uniform output
            return np.ones(len(X)) * 50.0
        return self.model.predict(X)


class MetaLabelClassifier:
    """
    Secondary binary classifier using HistGradientBoostingClassifier to filter trades.
    Predicts the probability that a generated signal succeeds (positive relative edge).
    """
    def __init__(self):
        self.model = HistGradientBoostingClassifier(
            max_iter=100,
            learning_rate=0.05,
            max_leaf_nodes=15,
            min_samples_leaf=20,
            random_state=42
        )
        self.is_fitted = False

    def fit(self, X: np.ndarray, y: np.ndarray):
        """Fit the binary classifier."""
        try:
            if len(X) < 50:
                log.warning("Insufficient samples to train MetaLabelClassifier. Model not trained.")
                return
            log.info(f"Training MetaLabelClassifier on {len(X)} samples with {X.shape[1]} features...")
            self.model.fit(X, y)
            self.is_fitted = True
            log.info("MetaLabelClassifier successfully fitted.")
        except Exception as e:
            log.error(f"Failed to fit MetaLabelClassifier: {e}")

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Predict success probability [0, 1]."""
        if not self.is_fitted:
            # Fallback: high confidence to bypass
            return np.ones(len(X)) * 0.60
        return self.model.predict_proba(X)[:, 1]


def compute_ticker_rolling_metrics(
    ticker: str,
    close: np.ndarray,
    volume: np.ndarray,
    spy_close: np.ndarray,
    sector_closes: Dict[str, np.ndarray],
    sector: str
) -> Dict[str, np.ndarray]:
    """
    Vectorized rolling feature calculation for a single ticker.
    Returns dictionary of arrays of the same length as the input series.
    """
    n = len(close)
    metrics = {}

    # Returns
    rets = np.zeros(n)
    rets[1:] = np.diff(close) / (close[:-1] + 1e-9)
    spy_rets = np.zeros(n)
    spy_rets[1:] = np.diff(spy_close) / (spy_close[:-1] + 1e-9)

    # 12-1 Momentum
    mom = np.zeros(n)
    for i in range(252, n):
        mom[i] = (close[i] / (close[i-252] + 1e-9) - 1.0) - (close[i] / (close[i-21] + 1e-9) - 1.0)
    metrics['momentum_12_1'] = np.clip(mom, -1.0, 1.0)

    # 5d Reversal
    rev = np.zeros(n)
    for i in range(5, n):
        rev[i] = - (close[i] / (close[i-5] + 1e-9) - 1.0)
    metrics['reversal_5d'] = np.clip(rev, -0.2, 0.2)

    # Realized Volatility (20d)
    vol = np.zeros(n)
    for i in range(20, n):
        vol[i] = np.std(rets[i-20:i]) * np.sqrt(252)
    metrics['realized_volatility'] = vol

    # Volume Breakout
    v_break = np.zeros(n)
    for i in range(20, n):
        v_5 = np.mean(volume[i-5:i])
        v_20 = np.mean(volume[i-20:i])
        v_break[i] = (v_5 / (v_20 + 1e-9)) - 1.0
        # directional adjustment
        v_break[i] = v_break[i] * np.sign(rets[i]) * 0.5
    metrics['volume_breakout'] = np.clip(v_break, -0.5, 0.5)

    # Rolling Correlation & Beta (20d)
    corr = np.zeros(n)
    beta = np.zeros(n)
    for i in range(20, n):
        sub_r = rets[i-20:i]
        sub_spy = spy_rets[i-20:i]
        cov = np.cov(sub_r, sub_spy)
        std_r = np.std(sub_r)
        std_spy = np.std(sub_spy)
        
        if std_r > 1e-6 and std_spy > 1e-6:
            corr[i] = cov[0, 1] / (std_r * std_spy)
            beta[i] = cov[0, 1] / (std_spy ** 2 + 1e-9)
    metrics['rolling_correlation'] = np.clip(corr, -1.0, 1.0)
    metrics['rolling_beta'] = np.clip(beta, -3.0, 3.0)

    # Volatility Expansion
    vol_exp = np.zeros(n)
    for i in range(20, n):
        v_5 = np.std(rets[i-5:i])
        v_20 = np.std(rets[i-20:i])
        vol_exp[i] = v_5 / (v_20 + 1e-9)
    metrics['volatility_expansion'] = np.clip(vol_exp, 0.1, 5.0)

    # Drawdown Depth
    dd = np.zeros(n)
    for i in range(20, n):
        max_p = np.max(close[i-20:i+1])
        dd[i] = (close[i] - max_p) / (max_p + 1e-9)
    metrics['drawdown_depth'] = np.clip(dd, -1.0, 0.0)

    # Sector Relative Strength
    sec_rel = np.zeros(n)
    if sector in sector_closes:
        sec_close = sector_closes[sector]
        sec_rets = np.zeros(n)
        sec_rets[1:] = np.diff(sec_close) / (sec_close[:-1] + 1e-9)
        for i in range(5, n):
            stock_5d = close[i] / (close[i-5] + 1e-9) - 1.0
            sec_5d = sec_close[i] / (sec_close[i-5] + 1e-9) - 1.0
            sec_rel[i] = stock_5d - sec_5d
    metrics['sector_relative_strength'] = np.clip(sec_rel, -0.5, 0.5)

    return metrics


def build_and_train_models(
    ticker_arrays: dict,
    ticker_metadata: dict,
    end_date_str: str
) -> Tuple[CrossSectionalMLRanker, MetaLabelClassifier]:
    """
    Builds training datasets from historical data strictly before `end_date_str` and fits the models.
    Uses pandas reindexing to perfectly align all tickers to SPY index, avoiding broadcasting mismatches.
    """
    log.info(f"Extracting historical training features prior to {end_date_str}...")
    
    # 1. Align SPY and calculate sector close averages
    spy = ticker_arrays.get('SPY')
    if spy is None:
        # fallback
        for t in ticker_arrays:
            spy = ticker_arrays[t]
            break
            
    spy_dates = spy['index_str']
    n_days = len(spy_dates)
    spy_close = spy['close'][:n_days]
    
    # Calculate sector averages using perfectly aligned series
    sectors = {}
    for ticker, arrays in ticker_arrays.items():
        meta = ticker_metadata.get(ticker, {'sector': 'Unknown'})
        sect = meta['sector']
        if sect not in sectors:
            sectors[sect] = []
        # Reindex ticker's close price to SPY dates
        s_close = pd.Series(arrays['close'], index=arrays['index_str']).reindex(spy_dates).ffill().bfill().values
        sectors[sect].append(s_close)
        
    sector_closes = {}
    for sect, series_list in sectors.items():
        stacked = np.stack(series_list, axis=0)
        sector_closes[sect] = np.mean(stacked, axis=0)

    # Compute rolling metrics for all tickers (perfectly aligned)
    all_metrics = {}
    for ticker, arrays in ticker_arrays.items():
        if ticker in ['SPY', '^VIX']:
            continue
        meta = ticker_metadata.get(ticker, {'sector': 'Unknown'})
        # Reindex constituent to match spy_dates exactly
        close = pd.Series(arrays['close'], index=arrays['index_str']).reindex(spy_dates).ffill().bfill().values
        volume = pd.Series(arrays['volume'], index=arrays['index_str']).reindex(spy_dates).fillna(0.0).values
        
        all_metrics[ticker] = compute_ticker_rolling_metrics(
            ticker, close, volume, spy_close, sector_closes, meta['sector']
        )

    # 2. Construct tabular datasets for Regressor and Classifier
    X_reg_list = []
    y_reg_list = []
    X_meta_list = []
    y_meta_list = []

    # Find the day index limit for training
    train_limit = n_days - 10  # leave 10 days for future returns validation
    if end_date_str in spy_dates:
        train_limit = min(train_limit, spy_dates.index(end_date_str))

    log.info(f"Building dataset over {train_limit - 300} training days...")
    
    # Pre-calculate future 5-day relative returns for training targets
    future_5d_rets = {}
    for ticker, arrays in ticker_arrays.items():
        if ticker in ['SPY', '^VIX']:
            continue
        close = pd.Series(arrays['close'], index=arrays['index_str']).reindex(spy_dates).ffill().bfill().values
        meta = ticker_metadata.get(ticker, {'sector': 'Unknown'})
        sect = meta['sector']
        sec_close = sector_closes.get(sect, spy_close)
        
        rets_5d = np.zeros(n_days)
        for i in range(n_days - 5):
            stock_fut = (close[i+5] / (close[i] + 1e-9)) - 1.0
            sec_fut = (sec_close[i+5] / (sec_close[i] + 1e-9)) - 1.0
            rets_5d[i] = stock_fut - sec_fut
            
        future_5d_rets[ticker] = rets_5d

    for t_idx in range(300, train_limit):
        # We need cross-sectional data for percentiles
        day_rel_rets = {}
        for ticker in all_metrics:
            day_rel_rets[ticker] = future_5d_rets[ticker][t_idx]
            
        # Sort and convert to percentiles [0, 100]
        sorted_tickers = sorted(day_rel_rets.keys(), key=lambda tk: day_rel_rets[tk])
        n_tickers = len(sorted_tickers)
        
        percentiles = {}
        for rank_idx, ticker in enumerate(sorted_tickers):
            percentiles[ticker] = (rank_idx / max(1, n_tickers - 1)) * 100.0

        for ticker in all_metrics:
            metrics = all_metrics[ticker]
            # Core Features
            feat_row = [metrics[name][t_idx] for name in FEATURE_NAMES]
            X_reg_list.append(feat_row)
            y_reg_list.append(percentiles[ticker])

            # Meta-Labeling Generation
            signal_direction = np.sign(metrics['momentum_12_1'][t_idx] + metrics['reversal_5d'][t_idx])
            if signal_direction != 0:
                future_outcome = future_5d_rets[ticker][t_idx]
                # Succeeds if future relative return matches signal direction
                success = 1 if (future_outcome * signal_direction > 0) else 0
                
                # Meta features
                meta_row = [
                    2.0,  # approximate base spread (bps)
                    15.0, # base VIX
                    metrics['volume_breakout'][t_idx] + 1.0, # liquidity proxy
                    float(signal_direction), # crowding proxy
                    metrics['realized_volatility'][t_idx],
                    0.0  # base sector regime
                ]
                X_meta_list.append(meta_row)
                y_meta_list.append(success)

    X_reg = np.array(X_reg_list)
    y_reg = np.array(y_reg_list)
    X_meta = np.array(X_meta_list)
    y_meta = np.array(y_meta_list)

    ranker = CrossSectionalMLRanker()
    ranker.fit(X_reg, y_reg)

    meta_model = MetaLabelClassifier()
    meta_model.fit(X_meta, y_meta)

    return ranker, meta_model

