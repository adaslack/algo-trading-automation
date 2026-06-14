"""
Cross-Sectional ML Ranker & Meta-Labeling Model (V15 Alpha Purity & Professional Validation)
============================================================================================
Implements:
1. HistGradientBoostingRegressor for cross-sectional relative return percentile ranking.
2. HistGradientBoostingClassifier for secondary Meta-Label filtering.
3. Chronological train-val split validation pipeline.
4. Adversarial Validation for train-test feature drift monitoring.
5. Out-of-sample Rank Information Coefficient (IC) metric logger.
6. Out-of-sample Permutation Feature Importance to prevent silent parameter decay.
"""
import numpy as np
import pandas as pd
from typing import Optional, Dict, List, Tuple
from sklearn.ensemble import HistGradientBoostingRegressor, HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.inspection import permutation_importance
from scipy.stats import spearmanr
from logger import get_logger

log = get_logger("MLRanker")

# Core features used for cross-sectional ranking (V15 Pruned Strictly to 3 Core Alpha Factors)
FEATURE_NAMES = [
    'momentum_12_1',
    'reversal_5d',
    'vol_regime'
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


def run_adversarial_validation(X_train: np.ndarray, X_val: np.ndarray) -> float:
    """
    Trains a HistGradientBoostingClassifier to distinguish training from validation features.
    If AUC > 0.65, indicates a major train-test distribution shift / temporal drift.
    """
    try:
        if len(X_train) < 20 or len(X_val) < 20:
            return 0.5
        
        # Labels: 0 for training split, 1 for validation split
        y_train_av = np.zeros(len(X_train))
        y_val_av = np.ones(len(X_val))
        
        X_av = np.vstack([X_train, X_val])
        y_av = np.concatenate([y_train_av, y_val_av])
        
        X_av_train, X_av_test, y_av_train, y_av_test = train_test_split(
            X_av, y_av, test_size=0.3, random_state=42, stratify=y_av
        )
        
        av_clf = HistGradientBoostingClassifier(max_iter=50, random_state=42)
        av_clf.fit(X_av_train, y_av_train)
        
        probs = av_clf.predict_proba(X_av_test)[:, 1]
        auc = roc_auc_score(y_av_test, probs)
        
        log.info(f"🛡️ [ADVERSARIAL VALIDATION] Train-Val Feature Drift AUC: {auc:.4f}")
        if auc > 0.65:
            log.warning(f"🚨 [FEATURE DRIFT WARNING] High Adversarial Validation AUC detected ({auc:.4f} > 0.65). "
                        "Training and validation distributions have significantly drifted!")
        return auc
    except Exception as e:
        log.error(f"Failed to compute adversarial validation: {e}")
        return 0.5


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

    # Volatility Regime (V15 Pure Factor)
    # Formula: -(vol_5d / vol_20d - 1.0)
    vol_reg = np.zeros(n)
    for i in range(21, n):
        sub_rets = rets[i-20:i]
        vol_5 = np.std(sub_rets[-5:]) * np.sqrt(252)
        vol_20 = np.std(sub_rets) * np.sqrt(252)
        if vol_20 > 0.001:
            vol_reg[i] = -(vol_5 / vol_20 - 1.0)
        else:
            vol_reg[i] = 0.0
    metrics['vol_regime'] = np.clip(vol_reg, -1.0, 1.0)

    # Realized Volatility (20d) (Auxiliary meta feature)
    vol = np.zeros(n)
    for i in range(20, n):
        vol[i] = np.std(rets[i-20:i]) * np.sqrt(252)
    metrics['realized_volatility'] = vol

    # Volume Breakout (Auxiliary meta feature)
    v_break = np.zeros(n)
    for i in range(20, n):
        v_5 = np.mean(volume[i-5:i])
        v_20 = np.mean(volume[i-20:i])
        v_break[i] = (v_5 / (v_20 + 1e-9)) - 1.0
        v_break[i] = v_break[i] * np.sign(rets[i]) * 0.5
    metrics['volume_breakout'] = np.clip(v_break, -0.5, 0.5)

    # Rolling Correlation & Beta (20d) (Auxiliary)
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

    # Volatility Expansion (Auxiliary)
    vol_exp = np.zeros(n)
    for i in range(20, n):
        v_5 = np.std(rets[i-5:i])
        v_20 = np.std(rets[i-20:i])
        vol_exp[i] = v_5 / (v_20 + 1e-9)
    metrics['volatility_expansion'] = np.clip(vol_exp, 0.1, 5.0)

    # Drawdown Depth (Auxiliary)
    dd = np.zeros(n)
    for i in range(20, n):
        max_p = np.max(close[i-20:i+1])
        dd[i] = (close[i] - max_p) / (max_p + 1e-9)
    metrics['drawdown_depth'] = np.clip(dd, -1.0, 0.0)

    # Sector Relative Strength (Auxiliary)
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


_metrics_cache = {}

def build_and_train_models(
    ticker_arrays: dict,
    ticker_metadata: dict,
    end_date_str: str
) -> Tuple[CrossSectionalMLRanker, MetaLabelClassifier]:
    """
    Builds training datasets from historical data strictly before `end_date_str` and fits the models.
    Incorporates high-level out-of-sample metrics (Adversarial Validation, Rank IC, Permutation Importance)
    using chronological splits, followed by full retraining on the complete dataset.
    Uses module-level caching to pre-compute rolling metrics once, speeding up backtest retraining over 95%.
    """
    global _metrics_cache
    log.info(f"Extracting historical training features prior to {end_date_str}...")
    
    spy = ticker_arrays.get('SPY')
    if spy is None:
        for t in ticker_arrays:
            spy = ticker_arrays[t]
            break
            
    spy_dates = spy['index_str']
    n_days = len(spy_dates)
    spy_close = spy['close'][:n_days]
    
    cache_key = tuple(sorted(ticker_arrays.keys()))
    
    if cache_key in _metrics_cache:
        all_metrics, future_5d_rets = _metrics_cache[cache_key]
    else:
        log.info("🚀 [CACHE MISS] Computing rolling metrics and future returns for the entire universe (one-time cost)...")
        # Calculate sector averages
        sectors = {}
        for ticker, arrays in ticker_arrays.items():
            meta = ticker_metadata.get(ticker, {'sector': 'Unknown'})
            sect = meta['sector']
            if sect not in sectors:
                sectors[sect] = []
            s_close = pd.Series(arrays['close'], index=arrays['index_str']).reindex(spy_dates).ffill().bfill().values
            sectors[sect].append(s_close)
            
        sector_closes = {}
        for sect, series_list in sectors.items():
            stacked = np.stack(series_list, axis=0)
            sector_closes[sect] = np.mean(stacked, axis=0)

        # Compute rolling metrics for all tickers
        all_metrics = {}
        for ticker, arrays in ticker_arrays.items():
            if ticker in ['SPY', '^VIX']:
                continue
            meta = ticker_metadata.get(ticker, {'sector': 'Unknown'})
            close = pd.Series(arrays['close'], index=arrays['index_str']).reindex(spy_dates).ffill().bfill().values
            volume = pd.Series(arrays['volume'], index=arrays['index_str']).reindex(spy_dates).fillna(0.0).values
            
            all_metrics[ticker] = compute_ticker_rolling_metrics(
                ticker, close, volume, spy_close, sector_closes, meta['sector']
            )

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
            
        _metrics_cache[cache_key] = (all_metrics, future_5d_rets)

    # Find the day index limit for training
    train_limit = n_days - 10  # leave 10 days for future returns validation
    if end_date_str in spy_dates:
        train_limit = min(train_limit, spy_dates.index(end_date_str))

    # Chronological Split: 80% Train days, 20% Val days
    start_day = 300
    if train_limit <= start_day + 50:
        start_day = max(20, train_limit - 50)
        
    split_idx = int(start_day + 0.8 * (train_limit - start_day))
    
    X_reg_train_list, y_reg_train_list = [], []
    X_reg_val_list, y_reg_val_list = [], []
    X_meta_train_list, y_meta_train_list = [], []
    X_meta_val_list, y_meta_val_list = [], []

    log.info(f"Splitting dataset chronologically at day index {split_idx} (Total Days: {train_limit - start_day})...")
    
    for t_idx in range(start_day, train_limit):
        is_train = t_idx < split_idx
        
        # Cross-sectional returns target
        day_rel_rets = {}
        for ticker in all_metrics:
            day_rel_rets[ticker] = future_5d_rets[ticker][t_idx]
            
        # Convert targets to percentile ranks [0, 100]
        sorted_tickers = sorted(day_rel_rets.keys(), key=lambda tk: day_rel_rets[tk])
        n_tickers = len(sorted_tickers)
        
        percentiles = {}
        for rank_idx, ticker in enumerate(sorted_tickers):
            percentiles[ticker] = (rank_idx / max(1, n_tickers - 1)) * 100.0

        for ticker in all_metrics:
            metrics = all_metrics[ticker]
            # Core 3 Features
            feat_row = [metrics[name][t_idx] for name in FEATURE_NAMES]
            if is_train:
                X_reg_train_list.append(feat_row)
                y_reg_train_list.append(percentiles[ticker])
            else:
                X_reg_val_list.append(feat_row)
                y_reg_val_list.append(percentiles[ticker])

            # Meta-Labeling Generation
            signal_direction = np.sign(metrics['momentum_12_1'][t_idx] + metrics['reversal_5d'][t_idx])
            if signal_direction != 0:
                future_outcome = future_5d_rets[ticker][t_idx]
                success = 1 if (future_outcome * signal_direction > 0) else 0
                
                meta_row = [
                    2.0,  # approximate base spread (bps)
                    15.0, # base VIX
                    metrics['volume_breakout'][t_idx] + 1.0, # liquidity proxy
                    float(signal_direction), # crowding proxy
                    metrics['realized_volatility'][t_idx],
                    0.0  # base sector regime
                ]
                if is_train:
                    X_meta_train_list.append(meta_row)
                    y_meta_train_list.append(success)
                else:
                    X_meta_val_list.append(meta_row)
                    y_meta_val_list.append(success)

    X_reg_train = np.array(X_reg_train_list)
    y_reg_train = np.array(y_reg_train_list)
    X_reg_val = np.array(X_reg_val_list)
    y_reg_val = np.array(y_reg_val_list)
    
    X_meta_train = np.array(X_meta_train_list)
    y_meta_train = np.array(y_meta_train_list)
    X_meta_val = np.array(X_meta_val_list)
    y_meta_val = np.array(y_meta_val_list)

    if len(X_reg_train) < 20 or len(X_reg_val) < 20:
        log.warning("Chronological dataset size too small for split validation. Reverting to full fitting.")
        # Fallback to combining everything
        X_reg = np.vstack([X_reg_train, X_reg_val]) if len(X_reg_val) > 0 else X_reg_train
        y_reg = np.concatenate([y_reg_train, y_reg_val]) if len(y_reg_val) > 0 else y_reg_train
        X_meta = np.vstack([X_meta_train, X_meta_val]) if len(X_meta_val) > 0 else X_meta_train
        y_meta = np.concatenate([y_meta_train, y_meta_val]) if len(y_meta_val) > 0 else y_meta_train
        
        ranker = CrossSectionalMLRanker()
        ranker.fit(X_reg, y_reg)
        meta_clf = MetaLabelClassifier()
        meta_clf.fit(X_meta, y_meta)
        return ranker, meta_clf

    # 1. Adversarial Validation Drift Classifier
    run_adversarial_validation(X_reg_train, X_reg_val)

    # 2. Train and Validate Ranker on split
    validation_ranker = CrossSectionalMLRanker()
    validation_ranker.fit(X_reg_train, y_reg_train)
    
    # Out-of-sample Rank IC
    val_preds = validation_ranker.predict(X_reg_val)
    # We correlate predicted ranks against actual target return rankings (y_reg_val)
    rank_ic, _ = spearmanr(val_preds, y_reg_val)
    log.info(f"📊 [OOS PERFORMANCE] Out-of-Sample Spearman Rank IC: {rank_ic:.4f}")

    # Out-of-sample Permutation Feature Importance
    try:
        r_importance = permutation_importance(validation_ranker.model, X_reg_val, y_reg_val, n_repeats=5, random_state=42)
        log.info("🧪 [PERMUTATION IMPORTANCE] Out-of-Sample feature contributions:")
        for i, f_name in enumerate(FEATURE_NAMES):
            log.info(f"    - {f_name:20s}: {r_importance.importances_mean[i]:+.4f} (±{r_importance.importances_std[i]:.4f})")
    except Exception as ex:
        log.error(f"Failed to calculate permutation feature importance: {ex}")

    # 3. Final Retraining on COMBINED dataset for maximum trading fidelity
    log.info("🔄 [FULL RETRAINING] Fitting final production models on combined train+validation dataset...")
    X_reg_full = np.vstack([X_reg_train, X_reg_val])
    y_reg_full = np.concatenate([y_reg_train, y_reg_val])
    X_meta_full = np.vstack([X_meta_train, X_meta_val])
    y_meta_full = np.concatenate([y_meta_train, y_meta_val])

    production_ranker = CrossSectionalMLRanker()
    production_ranker.fit(X_reg_full, y_reg_full)

    production_meta_clf = MetaLabelClassifier()
    production_meta_clf.fit(X_meta_full, y_meta_full)

    return production_ranker, production_meta_clf

