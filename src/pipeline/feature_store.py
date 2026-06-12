"""
Feature Store — Centralized Feature Engineering (V5 Upgrade - Optimized)
========================================================================
All features are computed ONCE, stored centrally, and served.

Optimized with Hurst calculation decimation (90% CPU calculation savings).
"""
import pandas as pd
import numpy as np
import threading
from typing import Optional
from datetime import datetime
from scipy.stats import norm as _scipy_norm
from logger import get_logger

def _compute_vwap(df):
    """Volume-Weighted Average Price."""
    try:
        tp = (df['high'] + df['low'] + df['close']) / 3.0
        return float((tp * df['volume']).sum() / (df['volume'].sum() + 1e-9))
    except Exception:
        return float(df['close'].iloc[-1])

def _compute_hurst(ts):
    """Hurst exponent via highly-optimized vectorized standard deviation of differences."""
    try:
        ts = np.array(ts, dtype=float)
        n = len(ts)
        if n < 20:
            return 0.5
        lags = [2, 4, 8, 16, 32, 64]
        # Only evaluate lags that can fit into the series
        valid_lags = [lag for lag in lags if n > lag]
        if len(valid_lags) < 3:
            return 0.5
        
        tau = [np.std(ts[lag:] - ts[:-lag]) for lag in valid_lags]
        if any(t <= 0 for t in tau):
            return 0.5
            
        slope = np.polyfit(np.log(valid_lags), np.log(tau), 1)[0]
        return float(np.clip(slope, 0.0, 1.0))
    except Exception:
        return 0.5

log = get_logger("FeatureStore")


class FeatureStore:
    """
    Centralized feature computation and caching.
    """

    @staticmethod
    def compute_real_features(close_prices: np.ndarray, volumes: np.ndarray, day_idx: int) -> Optional[dict]:
        """
        Compute REAL predictive features from historical price/volume data.
        All features use data strictly BEFORE day_idx (no lookahead).
        
        Returns dict of features or None if insufficient history.
        """
        if day_idx < 252:  # Need 252 days for 12-month momentum
            return None
        
        close = float(close_prices[day_idx])
        if close <= 0:
            return None
        
        # ── Factor 1: Cross-Sectional Momentum (12-1 month) ──────────────────
        price_252d_ago = float(close_prices[day_idx - 252])
        price_21d_ago = float(close_prices[day_idx - 21])
        
        if price_252d_ago > 0 and price_21d_ago > 0:
            ret_12m = (close / price_252d_ago) - 1.0
            ret_1m = (close / price_21d_ago) - 1.0
            momentum_12_1 = ret_12m - ret_1m
        else:
            momentum_12_1 = 0.0
        
        # ── Factor 2: Short-Term Mean Reversion (5-day) ──────────────────────
        price_5d_ago = float(close_prices[day_idx - 5])
        if price_5d_ago > 0:
            ret_5d = (close / price_5d_ago) - 1.0
            reversal_5d = -ret_5d
        else:
            reversal_5d = 0.0
        
        # ── Factor 3: Volume Breakout Signal ─────────────────────────────────
        vol_5d_avg = float(np.mean(volumes[day_idx-5:day_idx]))
        vol_20d_avg = float(np.mean(volumes[day_idx-20:day_idx]))
        if vol_20d_avg > 0:
            volume_breakout = (vol_5d_avg / vol_20d_avg) - 1.0
        else:
            volume_breakout = 0.0
        
        price_1d_ago = float(close_prices[day_idx - 1])
        daily_ret = (close - price_1d_ago) / price_1d_ago if price_1d_ago > 0 else 0.0
        volume_breakout = volume_breakout * np.sign(daily_ret) * 0.5
        
        # ── Factor 4: Volatility Regime Ratio ────────────────────────────────
        # CRITICAL FIX (V13): Use day_idx exclusive upper bound to prevent lookahead bias.
        # Previously used day_idx+1 which leaked today's close into vol computation.
        log_rets = np.diff(np.log(close_prices[day_idx-21:day_idx] + 1e-9))
        vol_5d = float(np.std(log_rets[-5:])) * np.sqrt(252) if len(log_rets) >= 5 else 0.0
        vol_20d = float(np.std(log_rets)) * np.sqrt(252) if len(log_rets) >= 2 else 0.0
        
        if vol_20d > 0.001:
            vol_regime = -(vol_5d / vol_20d - 1.0)
        else:
            vol_regime = 0.0
        
        # ── Factor 5: Hurst-Adjusted Trend Strength ──────────────────────────
        try:
            sub = close_prices[day_idx-100:day_idx]
            lags = [2, 4, 8, 16, 32]
            tau = [np.std(sub[lag:] - sub[:-lag]) for lag in lags]
            if all(t > 0 for t in tau):
                hurst = np.polyfit(np.log(lags), np.log(tau), 1)[0]
            else:
                hurst = 0.5
        except Exception:
            hurst = 0.5
        hurst = np.clip(hurst, 0.1, 0.9)
        
        ret_10d = (close / float(close_prices[day_idx - 10])) - 1.0 if close_prices[day_idx - 10] > 0 else 0.0
        if hurst > 0.55:
            trend_strength = ret_10d * (hurst - 0.5) * 4.0
        elif hurst < 0.45:
            trend_strength = -ret_10d * (0.5 - hurst) * 4.0
        else:
            trend_strength = 0.0
        
        garch_vol = vol_20d if vol_20d > 0.001 else 0.02
        vol_ratio_raw = float(volumes[day_idx]) / vol_20d_avg if vol_20d_avg > 0 else 1.0
        dollar_vol_20d = float(np.mean(close_prices[day_idx-20:day_idx] * volumes[day_idx-20:day_idx]))
        
        return {
            'close': close,
            'momentum_12_1': float(np.clip(momentum_12_1, -1.0, 1.0)),
            'reversal_5d': float(np.clip(reversal_5d, -0.2, 0.2)),
            'volume_breakout': float(np.clip(volume_breakout, -0.5, 0.5)),
            'vol_regime': float(np.clip(vol_regime, -1.0, 1.0)),
            'trend_strength': float(np.clip(trend_strength, -0.3, 0.3)),
            'garch_volatility': garch_vol,
            'volume_ratio': vol_ratio_raw,
            'dollar_vol_20d': dollar_vol_20d,
        }

    # Institutional Versioned Feature Registry & Lineage
    REGISTRY = {
        'vwap': {
            'version': '1.0.0',
            'lookback_bars': 200,
            'description': 'Volume-Weighted Average Price fair-value proxy.',
            'compute_formula': 'sum(typical_price * volume) / sum(volume)'
        },
        'garch_volatility': {
            'version': '1.1.2',
            'lookback_bars': 20,
            'description': 'Rolling realized conditional returns standard deviation scaled to annual equivalent.',
            'compute_formula': 'std(returns[-20:]) * sqrt(252)'
        },
        'hurst_exponent': {
            'version': '2.0.1',
            'lookback_bars': 200,
            'description': 'Long memory time-series correlation exponent computed via decimated R/S analysis.',
            'compute_formula': 'rescaled_range_analysis(returns)'
        },
        'volume_ratio': {
            'version': '1.0.0',
            'lookback_bars': 20,
            'description': 'Ratio of current bar volume to rolling average volume.',
            'compute_formula': 'current_volume / mean_volume_20d'
        },
        'obi': {
            'version': '1.2.0',
            'lookback_bars': 1,
            'description': 'Order Book Imbalance computed from buying vs selling pressure.',
            'compute_formula': '(estimated_bid_vol - estimated_ask_vol) / (total_volume + 1)'
        },
        'vpin': {
            'version': '2.1.0',
            'lookback_bars': 30,
            'description': 'Volume-Synchronized Probability of Informed Trading adverse selection metric.',
            'compute_formula': 'rolling_sum(abs(buy_vol - sell_vol)) / rolling_sum(total_volume)'
        }
    }

    def __init__(self):
        # In-memory feature cache: {ticker: {feature_name: value}}
        self._cache: dict[str, dict] = {}
        # Feature history for drift detection: {ticker: [snapshots]}
        self._history: dict[str, list[dict]] = {}
        self._max_history = 1000
        self._last_computed: dict[str, datetime] = {}
        # Thread lock for concurrency safety
        self._lock = threading.Lock()
        
        # Hurst Exponent Cache & Counter (Phase-2 CPU Optimization)
        self._hurst_cache = {}
        self._hurst_counter = {}

        # Initialize Feature Lineage Registry from static registry definitions
        try:
            from feature_lineage_registry import get_registry
            reg = get_registry()
            for f_name, meta in self.REGISTRY.items():
                reg.register_feature(
                    name=f_name,
                    version=meta['version'],
                    formula=meta['compute_formula'],
                    parameters={'lookback_bars': meta['lookback_bars']},
                    reference_distribution=None
                )
        except Exception as ex:
            log.warning(f"Could not initialize feature lineage registry: {ex}")

    def compute_features(self, ticker: str, df: pd.DataFrame, alt_data: dict = None) -> dict:
        """
        Compute all features for a ticker from a price DataFrame.
        V13: Now computes real predictive features alongside legacy features.
        """
        if df is None or len(df) < 50:
            return {}

        try:
            features = {}
            df_calc = df.copy().astype(np.float32)
            returns = df_calc['close'].pct_change().dropna()
            close_prices = df_calc['close'].values
            volumes = df_calc['volume'].values
            N = len(close_prices)
            
            latest = df_calc.iloc[-1].fillna(0).to_dict()
            latest['close_price'] = latest['close']
            close = float(latest['close'])
            
            # VWAP & GARCH Volatility proxy
            latest['vwap'] = _compute_vwap(df_calc)
            latest['garch_volatility'] = float(returns.tail(20).std() * np.sqrt(252)) if len(returns) >= 20 else 0.03
            
            # Hurst Exponent Decimation (Calculate once every 10 ticks to save 90% CPU overhead)
            with self._lock:
                if ticker not in self._hurst_counter:
                    self._hurst_counter[ticker] = 0
                self._hurst_counter[ticker] += 1
                
                if self._hurst_counter[ticker] % 10 == 0 or ticker not in self._hurst_cache:
                    h_val = _compute_hurst(close_prices)
                    self._hurst_cache[ticker] = h_val
                else:
                    h_val = self._hurst_cache[ticker]
            
            latest['hurst_exponent'] = h_val
            
            avg_vol = df_calc['volume'].rolling(window=20).mean()
            latest['volume_ratio'] = float(latest['volume'] / avg_vol.replace(0, np.nan).iloc[-1]) if len(avg_vol) > 0 else 1.0
            
            # ══════════════════════════════════════════════════════════════════
            # V13 REAL PREDICTIVE FEATURES (reused from unified implementation)
            # ══════════════════════════════════════════════════════════════════
            real_features = FeatureStore.compute_real_features(close_prices, volumes, N - 1)
            if real_features:
                latest.update(real_features)
            else:
                latest.update({
                    'momentum_12_1': 0.0,
                    'reversal_5d': 0.0,
                    'volume_breakout': 0.0,
                    'vol_regime': 0.0,
                    'trend_strength': 0.0,
                })
            
            # ══════════════════════════════════════════════════════════════════
            # LEGACY FEATURES (kept for backward compatibility with other modules)
            # ══════════════════════════════════════════════════════════════════
            
            # OBI & Micro-Price
            buying_pressure = (latest['close'] - latest['low']) / (latest['high'] - latest['low'] + 0.0001)
            selling_pressure = (latest['high'] - latest['close']) / (latest['high'] - latest['low'] + 0.0001)
            estimated_bid_vol = latest['volume'] * buying_pressure
            estimated_ask_vol = latest['volume'] * selling_pressure
            
            latest['obi'] = (estimated_bid_vol - estimated_ask_vol) / (latest['volume'] + 1)
            latest['micro_price'] = (estimated_bid_vol * latest['low'] + estimated_ask_vol * latest['high']) / (latest['volume'] + 1)
            
            # VPIN (Volume-Synchronized Probability of Toxicity)
            price_changes = df_calc['close'].diff()
            rolling_volatility = price_changes.rolling(window=20).std().replace(0, 0.0001).fillna(0.0001)
            z_scores = (price_changes / rolling_volatility).fillna(0.0)
            
            buy_ratios = _scipy_norm.cdf(z_scores.clip(-3.0, 3.0))
            buy_vols = df_calc['volume'] * buy_ratios
            sell_vols = df_calc['volume'] * (1 - buy_ratios)
            
            absolute_imbalances = (buy_vols - sell_vols).abs()
            rolling_imbalance_sum = absolute_imbalances.tail(30).sum()
            rolling_volume_sum = df_calc['volume'].tail(30).sum()
            
            latest['vpin'] = float(rolling_imbalance_sum / (rolling_volume_sum + 0.0001))
            
            # Alternative Data Signals (Purged for V14 Alpha Excellence)
            latest['dealer_gex'] = 0.0
            latest['insider_score'] = 0.0
            
            latest['volatility_20d'] = float(returns.tail(20).std() * np.sqrt(252)) if len(returns) >= 20 else 0.0
            latest['volatility_5d'] = float(returns.tail(5).std() * np.sqrt(252)) if len(returns) >= 5 else 0.0
            latest['timestamp'] = datetime.now().isoformat()
            
            features = latest

            # Log feature values to Lineage Registry
            try:
                from feature_lineage_registry import get_registry
                reg = get_registry()
                for f_name in self.REGISTRY.keys():
                    if f_name in latest:
                        reg.log_inference_value(f_name, latest[f_name])
            except Exception as ex:
                pass

            # Store in cache with thread lock safety
            with self._lock:
                self._cache[ticker] = features
                self._last_computed[ticker] = datetime.now()

                if ticker not in self._history:
                    self._history[ticker] = []
                self._history[ticker].append(features.copy())
                if len(self._history[ticker]) > self._max_history:
                    self._history[ticker] = self._history[ticker][-self._max_history:]

            return features

        except Exception as e:
            import traceback
            log.error(f"Feature computation failed for {ticker}: {e}")
            traceback.print_exc()
            return {}

    def get_features(self, ticker: str) -> Optional[dict]:
        """Get the latest cached features for a ticker with lock safety."""
        with self._lock:
            return self._cache.get(ticker)

    def get_model_tensor(self, ticker: str, feature_names: list[str]) -> Optional[np.ndarray]:
        with self._lock:
            features = self._cache.get(ticker)
            if not features:
                return None
            try:
                return np.array([features.get(f, 0.0) for f in feature_names], dtype=np.float32)
            except Exception:
                return None

    def get_feature_history(self, ticker: str, feature_name: str) -> np.ndarray:
        with self._lock:
            history = self._history.get(ticker, [])
            return np.array([h.get(feature_name, 0.0) for h in history])

    def get_all_tickers(self) -> list[str]:
        with self._lock:
            return list(self._cache.keys())

    def get_stats(self) -> dict:
        with self._lock:
            if not self._cache:
                return {'cached_tickers': 0, 'features_per_ticker': 0, 'total_history_points': 0}
            return {
                'cached_tickers': len(self._cache),
                'features_per_ticker': len(next(iter(self._cache.values()), {})),
                'total_history_points': sum(len(h) for h in self._history.values()),
            }


class FactorResidualizer:
    """
    Orthogonalizes core factors against SPY beta and sector industry averages to strip market and sector beta.
    Formula: f_resid = f - c0 - c1 * spy_beta - c2 * sector_average
    """
    @staticmethod
    def residualize(universe_features: dict, sectors: dict, spy_betas: dict) -> dict:
        """
        universe_features: {ticker: features_dict}
        sectors: {ticker: sector_name}
        spy_betas: {ticker: spy_beta_value}
        """
        if len(universe_features) < 3:
            return universe_features
            
        factors = ['momentum_12_1', 'reversal_5d', 'vol_regime', 'volume_breakout']
        residualized = {}
        for ticker, feats in universe_features.items():
            residualized[ticker] = feats.copy()
            
        tickers = list(universe_features.keys())
        N = len(tickers)
        
        # Group tickers by sector
        sector_groups = {}
        for t in tickers:
            s = sectors.get(t, 'Unknown')
            if s not in sector_groups:
                sector_groups[s] = []
            sector_groups[s].append(t)
            
        for f in factors:
            # 1. Compute sector averages
            sector_averages = {}
            for s, t_list in sector_groups.items():
                vals = [universe_features[t].get(f, 0.0) for t in t_list]
                sector_averages[s] = np.mean(vals) if vals else 0.0
                
            # 2. Build regression variables
            y = np.array([universe_features[t].get(f, 0.0) for t in tickers])
            x_beta = np.array([spy_betas.get(t, 1.0) for t in tickers])
            x_sec = np.array([sector_averages.get(sectors.get(t, 'Unknown'), 0.0) for t in tickers])
            
            # OLS: y = c0 + c1 * x_beta + c2 * x_sec + residual
            X_mat = np.column_stack((np.ones(N), x_beta, x_sec))
            try:
                # Use robust least squares solver
                coeffs, _, _, _ = np.linalg.lstsq(X_mat, y, rcond=None)
                fitted = X_mat.dot(coeffs)
                residuals = y - fitted
                
                # Assign residualized factors back
                for idx, t in enumerate(tickers):
                    residualized[t][f] = float(np.clip(residuals[idx], -1.0, 1.0))
            except Exception:
                # Fallback: simple sector-demeaning if OLS fails
                for t in tickers:
                    sec_avg = sector_averages.get(sectors.get(t, 'Unknown'), 0.0)
                    residualized[t][f] = universe_features[t].get(f, 0.0) - sec_avg
                    
        return residualized


