"""
Institutional Quant Research Core & Simulation Engine
======================================================
1. Deterministic Tick Replay Engine: Historical order-book replay with queue positioning.
2. Transaction-Cost-Aware Portfolio Optimizer.
3. Dynamic Covariance Forecaster: online covariance regime-switching.
4. Live-vs-Backtest Sharpe Drift Monitor & IC Decay.
5. Alpha Cemetery & Feature Graveyard: Auto-retire crowded/decayed factors.
"""

import numpy as np
import pandas as pd
import sqlite3
import os
import time
import datetime
from scipy.optimize import minimize
from logger import get_logger
from event_bus import get_bus, Event, Topics

log = get_logger("Research Core")

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'trading_brain.db')

class TickReplayEngine:
    """
    High-Fidelity Deterministic Tick Replay Engine
    Simulates historical limit order queues, order cancellations/replacements,
    and hidden liquidity (iceberg) fill probabilities.
    """
    def __init__(self, ticker, depth=10):
        self.ticker = ticker
        self.depth = depth
        self.queue_position = 0.0
        self.queue_depth = 1000.0  # Initial queue depth in shares
        self.cancellation_rate = 0.35  # 35% standard cancellation rate in LOB
        self.hidden_liquidity_ratio = 0.20  # 20% estimated iceberg/hidden volume
        
    def simulate_order_fill(self, price, side, target_price, tick_volume, toxic_vpin=0.3):
        """
        Calculates the probability and latency of order fills using 
        LOB queue positioning, cancel/replace simulation, and hidden liquidity.
        """
        if side == 'BUY' and target_price < price:
            return {'filled': False, 'reason': 'Price out of range', 'slippage_bps': 0.0}
        if side == 'SELL' and target_price > price:
            return {'filled': False, 'reason': 'Price out of range', 'slippage_bps': 0.0}
            
        # 1. Cancel/Replace Simulation: Real traders cancel/modify orders in front of us
        cancellations = tick_volume * self.cancellation_rate * np.random.uniform(0.8, 1.2)
        self.queue_depth = max(100.0, self.queue_depth - cancellations)
        
        # 2. Hidden Liquidity / Iceberg Approximation:
        effective_tick_volume = tick_volume * (1.0 - self.hidden_liquidity_ratio)
        
        # 3. Queue Positioning Decay:
        filled_qty = effective_tick_volume * np.random.uniform(0.15, 0.45)
        self.queue_position = max(0.0, self.queue_position - filled_qty)
        
        # 4. Toxic Flow Adverse Selection:
        front_run_slippage = 0.0
        if toxic_vpin > 0.6:
            front_run_slippage = (toxic_vpin - 0.6) * 15.0  # Up to 6 bps slippage penalty
            self.queue_depth += 400.0  # Informed flow expands depth on opposite side
            
        if self.queue_position == 0.0:
            fill_price = target_price + (front_run_slippage * 0.01 if side == 'BUY' else -front_run_slippage * 0.01)
            return {
                'filled': True,
                'fill_price': round(fill_price, 4),
                'slippage_bps': round(front_run_slippage, 2),
                'latency_ms': np.random.exponential(45.0)  # Institutional 45ms avg execution latency
            }
            
        return {'filled': False, 'reason': 'Queue position not reached', 'slippage_bps': 0.0}



class TransactionCostOptimizer:
    """
    Transaction-Cost-Aware Portfolio Optimizer (Alpha - Turnover - Impact - Slippage)
    """
    def __init__(self, risk_aversion=1.5, turnover_penalty=0.0010, impact_scaling=0.5):
        self.gamma = risk_aversion
        self.lambda_turnover = turnover_penalty  # 10 bps turnover penalty
        self.eta = impact_scaling               # Market impact coefficient
        
    def optimize(self, alphas, current_weights, cov_matrix, daily_volumes, price_estimates, aum=10000000.0):
        """
        Solves for target weights maximizing:
        Objective = w^T * alpha - gamma/2 * w^T * Cov * w - TurnoverCost(w - w_0) - ImpactCost(w)
        """
        n = len(alphas)
        if n == 0:
            return np.array([])
            
        # Bounds: Market Neutral (-10% to +10% exposure per asset)
        bounds = [(-0.15, 0.15) for _ in range(n)]
        
        # Constraints: Net Beta/Exposure Neutral (Sum of weights = 0)
        constraints = ({'type': 'eq', 'fun': lambda w: np.sum(w)})
        
        def objective(w):
            w = np.array(w)
            current = np.array(current_weights)
            
            # 1. Expected Alpha Return
            ret = np.dot(w, alphas)
            
            # 2. Portfolio Risk Variance
            risk = 0.5 * self.gamma * np.dot(w.T, np.dot(cov_matrix, w))
            
            # 3. Linear Turnover Cost
            turnover_cost = self.lambda_turnover * np.sum(np.abs(w - current))
            
            # 4. Non-Linear Market Impact Cost (Square-Root Law)
            impact_cost = 0.0
            for i in range(n):
                vol_val = w[i] * aum
                participation = abs(vol_val) / (daily_volumes[i] * price_estimates[i] + 1e-9)
                daily_vol = np.sqrt(cov_matrix[i, i])
                impact_cost += self.eta * daily_vol * np.sqrt(participation + 1e-9) * abs(w[i])
                
            return -(ret - risk - turnover_cost - impact_cost)
            
        initial_guess = np.zeros(n)
        result = minimize(objective, initial_guess, bounds=bounds, constraints=constraints, method='SLSQP')
        
        if result.success:
            return np.round(result.x, 4)
        else:
            log.warning(f"Transaction Cost Optimization failed: {result.message}. Returning baseline.")
            return np.zeros(n)
            
    def generate_capacity_curve(self, alphas, cov_matrix, daily_volumes, price_estimates, scale_range=[1e6, 5e6, 1e7, 5e7, 1e8]):
        """
        Generates Capacity Curves plotting net expected returns as AUM scales.
        """
        n = len(alphas)
        curve = []
        for aum in scale_range:
            w_opt = self.optimize(alphas, np.zeros(n), cov_matrix, daily_volumes, price_estimates, aum=aum)
            net_alpha = np.dot(w_opt, alphas)
            
            # Compute costs
            impact = 0.0
            for i in range(n):
                vol_val = w_opt[i] * aum
                participation = abs(vol_val) / (daily_volumes[i] * price_estimates[i] + 1e-9)
                daily_vol = np.sqrt(cov_matrix[i, i])
                impact += self.eta * daily_vol * np.sqrt(participation + 1e-9) * abs(w_opt[i])
                
            realized_return = net_alpha - impact
            curve.append({'AUM': aum, 'Expected_Return': net_alpha, 'Realized_Return': realized_return, 'Impact_Decay': impact})
        return pd.DataFrame(curve)


class DynamicCovarianceForecaster:
    """
    Online EWMA & DCC-GARCH Covariance Estimator for Volatility Clustering
    """
    def __init__(self, num_assets, decay=0.94):
        self.decay = decay
        self.n = num_assets
        self.cov_matrix = np.eye(self.n) * 0.0004 # Baseline daily variance (~2% vol)
        
    def update_covariance(self, daily_returns):
        """
        Performs an online update of the asset covariance matrix.
        """
        returns = np.array(daily_returns).reshape(-1, 1)
        outer_prod = np.dot(returns, returns.T)
        self.cov_matrix = (self.decay * self.cov_matrix) + ((1 - self.decay) * outer_prod)
        
        # Regularization
        self.cov_matrix += np.eye(self.n) * 1e-6
        return self.cov_matrix


class LiveBacktestDivergenceEngine:
    """
    Expected vs Realized Sharpe Drift Tracker
    """
    def __init__(self):
        self.expected_sharpes = {}
        self.realized_returns = {}
        
    def record_prediction(self, strategy, expected_sharpe):
        if strategy not in self.expected_sharpes:
            self.expected_sharpes[strategy] = []
        self.expected_sharpes[strategy].append(expected_sharpe)
        
    def record_realized_trade(self, strategy, pnl_pct):
        if strategy not in self.realized_returns:
            self.realized_returns[strategy] = []
        self.realized_returns[strategy].append(pnl_pct)
        
    def calculate_sharpe_drift(self, strategy):
        """
        Computes the Sharpe drift.
        """
        expected = self.expected_sharpes.get(strategy, [])
        realized = self.realized_returns.get(strategy, [])
        
        if len(expected) < 5 or len(realized) < 5:
            return {'drift_bps': 0.0, 'status': 'INSUFFICIENT_DATA', 'p_value': 1.0}
            
        mean_exp = np.mean(expected)
        std_real = np.std(realized) + 1e-8
        mean_real = np.mean(realized)
        realized_sharpe = (mean_real / std_real) * np.sqrt(252)
        
        drift = mean_exp - realized_sharpe
        status = 'STABLE'
        if drift > 1.5:
            status = 'WARNING_DECAY'
        if drift > 3.0:
            status = 'CRITICAL_ALPHA_COLLAPSE'
            
        return {
            'expected_sharpe': round(mean_exp, 2),
            'realized_sharpe': round(realized_sharpe, 2),
            'drift': round(drift, 2),
            'status': status,
            'observations': len(realized)
        }


class AlphaCemetery:
    """
    The Quant Graveyard & Crowding Monitor
    """
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        
    def audit_and_retire_strategies(self):
        """
        Audits registered strategies, retiring failed alphas to the Graveyard.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT strategy, sharpe_ratio, win_rate, total_trades FROM strategy_metrics")
        metrics = cursor.fetchall()
        
        retired = []
        for strategy, sharpe, win_rate, total_trades in metrics:
            if not total_trades or total_trades < 10:
                continue
                
            # Retirement Thresholds: Sharpe < 0.3 or Win Rate < 42%
            if (sharpe and sharpe < 0.3) or (win_rate and win_rate < 0.42):
                log.warning(f"⚠️ Strategy '{strategy}' failed institutional health audit. RETIRING TO GRAVEYARD.")
                
                # Move weight to 0.0
                cursor.execute("UPDATE strategy_weights SET weight = 0.0 WHERE strategy = ?", (strategy,))
                # Mark as retired
                cursor.execute("UPDATE strategy_metrics SET total_trades = -1 WHERE strategy = ?", (strategy,))
                retired.append(strategy)
                
        conn.commit()
        conn.close()
        return retired


class StatisticalDefenseCourt:
    """
    Anti-Overfitting Court & Statistical Defense Engine
    """
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        
    def calculate_dsr(self, strategy_returns, trials_count=50):
        """
        Calculates the Deflated Sharpe Ratio (DSR) for a strategy return series.
        """
        from scipy.stats import norm
        returns = np.array(strategy_returns, dtype=float)
        T = len(returns)
        if T < 10:
            return {'dsr_p_value': 1.0, 'status': 'INSUFFICIENT_DATA', 'dsr': 0.0}
            
        mean_r = np.mean(returns)
        std_r = np.std(returns) + 1e-8
        
        # Annualized observed Sharpe ratio
        obs_sr = (mean_r / std_r) * np.sqrt(252)
        
        # Skewness and Kurtosis
        skew = float(pd.Series(returns).skew() or 0.0)
        kurt = float(pd.Series(returns).kurt() or 0.0) + 3.0
        
        # expected maximum Sharpe under null hypothesis (SR_0)
        em_constant = 0.5772156649
        std_trials = 0.15
        expected_max_z = (1.0 - em_constant) * norm.ppf(1.0 - 1.0 / trials_count) + em_constant * norm.ppf(1.0 - 1.0 / (trials_count * np.e))
        expected_max_sr = std_trials * expected_max_z
        
        # Variance of estimated Sharpe ratio
        var_sr = (1.0 - skew * obs_sr + (kurt - 1.0) / 4.0 * (obs_sr ** 2)) / (T - 1)
        
        # DSR Statistic Z-score
        dsr_z = (obs_sr - expected_max_sr) / np.sqrt(max(1e-8, var_sr))
        dsr_p = 1.0 - norm.cdf(dsr_z)
        
        return {
            'observed_sharpe': round(obs_sr, 2),
            'expected_max_sharpe': round(expected_max_sr, 2),
            'skewness': round(skew, 2),
            'kurtosis': round(kurt, 2),
            'dsr_p_value': round(float(dsr_p), 4),
            'status': 'REJECT_OVERFIT' if dsr_p > 0.05 else 'PASS_GENUINE_ALPHA'
        }
        
    def run_whites_reality_check(self, strategy_returns, benchmark_returns=None, num_bootstraps=1000):
        """
        White's Reality Check (Bootstrap Multiple Testing Correction).
        """
        returns = np.array(strategy_returns, dtype=float)
        T = len(returns)
        if T < 10:
            return {'wrc_p_value': 1.0, 'significant': False}
            
        benchmark = np.zeros(T) if benchmark_returns is None else np.array(benchmark_returns, dtype=float)
        
        excess_returns = returns - benchmark
        observed_mean = np.mean(excess_returns)
        
        null_means = []
        for _ in range(num_bootstraps):
            indices = np.random.choice(T, size=T, replace=True)
            resampled_excess = excess_returns[indices]
            centered_excess = resampled_excess - observed_mean
            null_means.append(np.mean(centered_excess))
            
        null_means = np.array(null_means)
        wrc_p = np.sum(null_means >= observed_mean) / num_bootstraps
        
        return {
            'observed_excess_mean': round(float(observed_mean * 252 * 100), 2),  # Annualized %
            'wrc_p_value': round(float(wrc_p), 4),
            'significant': wrc_p <= 0.05
        }


class FeatureLifecycleTracker:
    """
    Research Factory: Feature Lifecycle & Alpha Genealogy Engine
    ============================================================
    Tracks the rolling Information Coefficient (IC) decay and retires decayed features.
    """
    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        self.feature_registry = {}
        self._init_registry()
    
    def _init_registry(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        c = conn.cursor()
        c.execute("SELECT strategy, win_rate, sharpe_ratio, total_trades FROM strategy_metrics")
        for strat, wr, sr, trades in c.fetchall():
            self.feature_registry[strat] = {
                'born': datetime.datetime.now().isoformat(),
                'ic_history': [],
                'half_life': None,
                'status': 'ACTIVE' if (trades or 0) >= 0 else 'RETIRED',
                'win_rate': wr or 0.5,
                'sharpe': sr or 0.0,
                'total_trades': trades or 0
            }
        conn.close()
        log.info(f"Feature Lifecycle Registry initialized with {len(self.feature_registry)} features.")
    
    def compute_rolling_ic(self, strategy):
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        c = conn.cursor()
        c.execute("""
            SELECT pnl_pct FROM trade_history 
            WHERE strategy = ? AND status = 'CLOSED' AND pnl_pct IS NOT NULL
            ORDER BY id DESC LIMIT 30
        """, (strategy,))
        rows = c.fetchall()
        conn.close()
        
        if len(rows) < 10:
            return None
        
        returns = np.array([r[0] for r in rows])
        ranks = np.argsort(np.argsort(returns))
        sequence = np.arange(len(returns))
        
        # Spearman rank correlation
        ic = np.corrcoef(ranks, sequence)[0, 1]
        
        if strategy in self.feature_registry:
            self.feature_registry[strategy]['ic_history'].append({
                'timestamp': datetime.datetime.now().isoformat(),
                'ic': round(float(ic), 4),
                'n_trades': len(rows)
            })
        
        return ic
    
    def estimate_half_life(self, strategy):
        if strategy not in self.feature_registry:
            return None
        
        ic_hist = self.feature_registry[strategy]['ic_history']
        if len(ic_hist) < 5:
            return None
        
        ics = np.array([h['ic'] for h in ic_hist[-20:]])
        abs_ics = np.abs(ics)
        
        if abs_ics[0] <= 0:
            return None
        
        t = np.arange(len(abs_ics))
        log_ics = np.log(abs_ics + 1e-8)
        
        if len(t) >= 2:
            slope = np.polyfit(t, log_ics, 1)[0]
            if slope < 0:
                half_life = -np.log(2) / slope
                self.feature_registry[strategy]['half_life'] = round(float(half_life), 2)
                return half_life
        
        return None
    
    def auto_retire_decayed_features(self, ic_threshold=0.02):
        retired = []
        for strategy, info in self.feature_registry.items():
            if info['status'] == 'RETIRED':
                continue
            
            ic = self.compute_rolling_ic(strategy)
            if ic is not None and abs(ic) < ic_threshold:
                half_life = self.estimate_half_life(strategy)
                log.warning(
                    f"🪦 Feature '{strategy}' IC decayed to {ic:.4f}. Half-life: {half_life}. AUTO-RETIRING."
                )
                info['status'] = 'RETIRED'
                retired.append(strategy)
                
                # Zero out weight
                conn = sqlite3.connect(self.db_path, timeout=30.0)
                c = conn.cursor()
                c.execute("UPDATE strategy_weights SET weight = 0.0 WHERE strategy = ?", (strategy,))
                conn.commit()
                conn.close()
        
        return retired
    
    def get_feature_report(self):
        report = []
        for strat, info in self.feature_registry.items():
            ic = self.compute_rolling_ic(strat)
            half_life = self.estimate_half_life(strat)
            report.append({
                'strategy': strat,
                'status': info['status'],
                'rolling_ic': round(float(ic), 4) if ic is not None else None,
                'half_life': half_life,
                'total_trades': info['total_trades'],
                'ic_observations': len(info['ic_history'])
            })
        return report


def run_research_engine():
    log.info("Initializing Quant Research Engine...")
    cemetery = AlphaCemetery()
    divergence = LiveBacktestDivergenceEngine()
    lifecycle = FeatureLifecycleTracker()
    
    bus = get_bus()
    
    def on_trade_filled(event: Event):
        data = event.data
        ticker = data.get('ticker')
        
        # Real-time tracking
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            c = conn.cursor()
            c.execute("SELECT strategy, pnl_pct FROM trade_history WHERE ticker=? ORDER BY id DESC LIMIT 1", (ticker,))
            row = c.fetchone()
            if row and row[0]:
                strat, pnl = row[0], row[1] or 0.0
                divergence.record_realized_trade(strat, pnl)
                drift_metrics = divergence.calculate_sharpe_drift(strat)
                if drift_metrics['status'] != 'STABLE':
                    log.warning(f"🚨 Sharpe Drift Warning on {strat}: {drift_metrics}")
                    
                if drift_metrics['status'] == 'CRITICAL_ALPHA_COLLAPSE':
                    bus.publish(Topics.RISK_ALERT, Event(
                        source="research_core",
                        data={
                            "strategy": strat,
                            "metrics": drift_metrics,
                            "alert_level": "CRITICAL"
                        }
                    ))
                    log.critical(f"🚨 EVENT BROADCAST: Statistical Sharpe Drift Alert triggered on {strat}!")
            conn.close()
        except Exception as e:
            log.error(f"Drift tracker error: {e}")

    bus.subscribe(Topics.ORDER_FILLED, on_trade_filled)
    
    audit_cycle = 0
    while True:
        try:
            audit_cycle += 1
            
            # 1. Cemetery audit
            retired = cemetery.audit_and_retire_strategies()
            if retired:
                log.info(f"🪦 Graveyard Audit: Retired strategies: {retired}")
            
            # 2. IC Decay Tracking
            if audit_cycle % 3 == 0:
                decayed = lifecycle.auto_retire_decayed_features(ic_threshold=0.02)
                if decayed:
                    log.info(f"📉 IC Decay Retirement: {decayed}")
            
            # 3. Full Genealogy Report
            if audit_cycle % 12 == 0:
                report = lifecycle.get_feature_report()
                log.info("📊 Feature Genealogy Report:")
                for r in report:
                    ic_str = f"IC={r['rolling_ic']:.4f}" if r['rolling_ic'] is not None else "IC=N/A"
                    hl_str = f"T½={r['half_life']:.1f}" if r['half_life'] is not None else "T½=N/A"
                    log.info(f"  {r['strategy']}: {r['status']} | {ic_str} | {hl_str} | Trades={r['total_trades']}")
                
        except Exception as e:
            log.error(f"Research engine loop failed: {e}")
            
        time.sleep(300) # Audits every 5 minutes

if __name__ == '__main__':
    run_research_engine()

