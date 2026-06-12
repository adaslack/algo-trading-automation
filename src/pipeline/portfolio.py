"""
Portfolio — Bayesian Portfolio Construction Engine
====================================================
Replaces rule stacks and softmax reallocation with true Bayesian inference.

Architecture:
  BayesianPortfolio.update(fill)  → updates posterior on each trade outcome
  BayesianPortfolio.size(output)  → returns uncertainty-aware position size
  BayesianPortfolio.weights()     → returns posterior strategy weights

Theory
------
We maintain a Normal-Normal conjugate model over each strategy's expected return:

  Prior:       μ_0 ~ N(μ_0, σ_0²)       (weakly informative: μ=0, σ=0.02)
  Likelihood:  r_t | μ ~ N(μ, σ_obs²)   (observed trade return)
  Posterior:   μ_n ~ N(μ_n, σ_n²)

Posterior update (closed form):
  σ_n² = 1 / (1/σ_{n-1}² + 1/σ_obs²)
  μ_n  = σ_n² * (μ_{n-1}/σ_{n-1}² + r_t/σ_obs²)

Position sizing
---------------
Kelly fraction adjusted by posterior uncertainty:
  f* = μ_n / σ_obs²                      (raw Kelly)
  f  = f* × (1 - 2σ_n / |μ_n|)          (uncertainty haircut)
  f  = clip(f, MIN_ALLOC, MAX_ALLOC)

The haircut term shrinks position size when posterior std is large relative
to posterior mean — i.e. when we are uncertain, we bet less.
This is the key property missing from the old Kelly + softmax approach.

Capacity awareness
------------------
Position size is further capped at 2% ADV participation to prevent
microstructure self-impact.
"""
import sqlite3
import numpy as np
import time
import os
import datetime
from dataclasses import dataclass, field
from typing import Optional
from logger import get_logger
from alpha_engine import AlphaOutput

log = get_logger("BayesianPortfolio")

DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'data', 'trading_brain.db'
)

# Position size bounds
MIN_ALLOC = 0.01   # 1%  of portfolio
MAX_ALLOC = 0.12   # 12% of portfolio (hard cap)
ADV_PARTICIPATION = 0.02   # max 2% of average daily volume


def estimate_slippage(price: float, quantity: float, adv: float, volatility: float, vix: float) -> float:
    """
    Computes an empirical multi-factor slippage cost (in bps).
    Slippage = BaseSpread + c1 * Volatility + c2 * (Quantity / ADV)^2 + MarketStressPenalty
    """
    base_spread = 2.0  # base spread in bps
    vol_impact = 10.0 * max(volatility, 0.02)  # volatility contribution
    
    participation_rate = quantity / max(adv, 1.0)
    market_impact = 100.0 * (participation_rate ** 2)  # square ADV impact
    
    # Market stress penalty (exponential VIX penalty)
    market_stress = 0.0
    if vix > 20.0:
        market_stress = (vix - 20.0) * 0.5
        
    slippage_bps = base_spread + vol_impact + market_impact + market_stress
    return float(np.clip(slippage_bps, 2.0, 150.0))


# ── Bayesian state for a single strategy ─────────────────────────────────────

@dataclass
class _StrategyBelief:
    """
    Conjugate Normal-Normal posterior over a strategy's mean return.
    Updated on every closed trade.
    """
    mu:    float = 0.0       # posterior mean return
    sigma: float = 0.02      # posterior std (uncertainty)
    n:     int   = 0         # number of observed trades
    wins:  int   = 0
    total_pnl: float = 0.0

    @property
    def win_rate(self) -> float:
        return self.wins / max(self.n, 1)

    @property
    def sharpe_proxy(self) -> float:
        """Annualised Sharpe using posterior mu and sigma."""
        return (self.mu / max(self.sigma, 1e-8)) * np.sqrt(252)

    def update(self, observed_return: float, obs_noise: float = 0.03):
        """
        Bayesian posterior update (Normal-Normal conjugate).

        Parameters
        ----------
        observed_return : realised return from a single closed trade
        obs_noise       : assumed observation noise std (default 3%)
        """
        prior_precision    = 1.0 / max(self.sigma ** 2, 1e-10)
        obs_precision      = 1.0 / max(obs_noise ** 2, 1e-10)
        post_precision     = prior_precision + obs_precision
        post_variance      = 1.0 / post_precision
        post_mean          = post_variance * (
            self.mu * prior_precision + observed_return * obs_precision
        )
        self.sigma = float(np.sqrt(post_variance))
        self.mu    = float(post_mean)
        self.n    += 1
        if observed_return > 0:
            self.wins += 1
        self.total_pnl += observed_return


class _RunningCovariance:
    """Helper for recursive online tracking of returns means and covariance matrix."""
    __slots__ = ('tickers', 'alpha', 'mu', 'cov', 'initialized')

    def __init__(self, tickers: list[str], alpha: float = 0.96):
        self.tickers = list(tickers)
        self.alpha = alpha
        n = len(tickers)
        self.mu = np.zeros(n, dtype=float)
        self.cov = np.eye(n, dtype=float) * 1e-4
        self.initialized = False

    def update(self, returns: np.ndarray):
        """Update recursive covariance and mean using a decay factor alpha."""
        if not self.initialized:
            self.mu = np.array(returns, dtype=float)
            self.cov = np.eye(len(returns), dtype=float) * 1e-4
            self.initialized = True
            return
            
        # mu = (1 - alpha) * returns + alpha * mu
        self.mu = (1.0 - self.alpha) * returns + self.alpha * self.mu
        # cov = alpha * cov + (1 - alpha) * (returns - mu) * (returns - mu)^T
        diff = returns - self.mu
        self.cov = self.alpha * self.cov + (1.0 - self.alpha) * np.outer(diff, diff)


# ── Main engine ───────────────────────────────────────────────────────────────

class BayesianPortfolio:
    """
    Uncertainty-aware Bayesian portfolio construction engine.

    Usage
    -----
    portfolio = BayesianPortfolio()

    # On each alpha signal:
    size_pct = portfolio.size(alpha_output, price, portfolio_value, adv)

    # On each closed trade:
    portfolio.update(strategy='Microstructure_Ranker', realised_return=0.021)

    # Get current posterior weights:
    weights = portfolio.weights()
    """

    def __init__(self, strategies: Optional[list[str]] = None):
        default_strategies = [
            'factor_unified_expected_return',
            'factor_microstructure_flow',
            'factor_mean_reversion',
            'factor_latent_alpha',
        ]
        self._beliefs: dict[str, _StrategyBelief] = {
            s: _StrategyBelief() for s in (strategies or default_strategies)
        }
        self._running_cov: dict[tuple, _RunningCovariance] = {}
        self._max_cov_entries = 50  # V13: LRU eviction cap for _running_cov
        self._prev_weights: dict[str, float] = {}  # Dynamic turnover regularization
        self._peak_value: float = 0.0  # V13: Track peak for drawdown scaling

    # ── Posterior update on trade close ──────────────────────────────────────

    def update(self, strategy: str, realised_return: float, obs_noise: float = 0.03):
        """
        Update the posterior belief for `strategy` with a realized return.

        Call this every time a trade closes.

        Parameters
        ----------
        strategy         : strategy name
        realised_return  : net return (e.g. 0.015 = +1.5%)
        obs_noise        : observation noise std (default 3% per trade)
        """
        if strategy not in self._beliefs:
            self._beliefs[strategy] = _StrategyBelief()
        self._beliefs[strategy].update(realised_return, obs_noise)

        b = self._beliefs[strategy]
        log.info(
            f"Posterior update [{strategy}]: "
            f"μ={b.mu*100:+.2f}% σ={b.sigma*100:.2f}% "
            f"n={b.n} WR={b.win_rate*100:.0f}% "
            f"Sharpe≈{b.sharpe_proxy:.2f}"
        )

    # ── Uncertainty-aware position sizing ────────────────────────────────────

    def size(
        self,
        alpha_out:       AlphaOutput,
        price:           float,
        portfolio_value: float,
        adv:             float = 5_000_000.0,
    ) -> dict:
        """
        Compute uncertainty-aware position size for an alpha signal.

        Returns a dict with:
          alloc_pct  : fraction of portfolio to allocate
          qty        : number of shares
          kelly_raw  : raw Kelly fraction (before haircut)
          uncertainty_haircut : how much uncertainty shrank the position
          adv_capped : True if ADV participation cap was binding

        Parameters
        ----------
        alpha_out       : AlphaOutput from AlphaEngine.evaluate()
        price           : current price of the ticker
        portfolio_value : total portfolio value ($)
        adv             : average daily volume in shares
        """
        strategy = 'factor_unified_expected_return'
        belief = self._beliefs.get(strategy, _StrategyBelief())

        # V13 FIX: Stable blending with floor on weights to prevent noise amplification
        # When cold-starting (n=0), alpha_weight dominates with a minimum floor of 0.1
        alpha_weight   = max(alpha_out.confidence, 0.10)  # Floor prevents near-zero denominator
        posterior_weight = min(0.8, belief.n / max(belief.n + 20, 1))  # grows with experience

        blended_er = (
            alpha_weight      * alpha_out.expected_return +
            posterior_weight  * belief.mu
        ) / (alpha_weight + posterior_weight)  # Safe: alpha_weight >= 0.10 guarantees denom >= 0.10

        # Raw Kelly: E[r] / Var[r]  (simplified: assume obs_noise as variance)
        obs_noise = max(belief.sigma, 0.02)
        kelly_raw = blended_er / max(obs_noise ** 2, 1e-8)
        kelly_raw = float(np.clip(kelly_raw, -1.0, 1.0))

        # Uncertainty haircut: shrink position when posterior std is large
        # relative to posterior mean. When we have no data, sigma ≈ 0.02 and
        # mu ≈ 0 → haircut is large → start small and grow with evidence.
        if abs(belief.mu) > 1e-6:
            relative_uncertainty = 2.0 * belief.sigma / max(abs(belief.mu), 1e-6)
            uncertainty_haircut  = max(0.0, 1.0 - relative_uncertainty)
        else:
            # No trades yet — conservative 30% of Kelly
            uncertainty_haircut = 0.30

        kelly_adjusted = kelly_raw * uncertainty_haircut

        # Estimate slippage cost dynamically using the multi-factor model
        approx_qty = abs(kelly_adjusted) * 0.5 * portfolio_value / max(price, 1e-6)
        vix_val = 15.0
        try:
            from alpha_engine import get_engine
            # Extract HMM VIX approximation
            vix_val = float(get_engine()._hmm_means[0] * get_engine()._regime_probs[0] + 
                            get_engine()._hmm_means[1] * get_engine()._regime_probs[1] + 
                            get_engine()._hmm_means[2] * get_engine()._regime_probs[2])
        except Exception:
            pass
            
        slippage_bps = estimate_slippage(
            price=price,
            quantity=approx_qty,
            adv=adv,
            volatility=max(belief.sigma, 0.02),
            vix=vix_val
        )
        liq_penalty = slippage_bps / 10_000.0   # convert bps to fraction
        kelly_adjusted = kelly_adjusted * (1.0 - liq_penalty * 5.0)

        # Position Throttling (V14 Alpha Excellence)
        # Scale down exposure dynamically under drawdown, regime volatility, and liquidity stress
        self._peak_value = max(self._peak_value, portfolio_value)
        
        # A. Drawdown Throttling
        if self._peak_value > 0:
            current_dd = (self._peak_value - portfolio_value) / self._peak_value
            dd_multiplier = max(0.1, 1.0 - (current_dd / 0.05) * 0.9)  # scale down to 10% at 5% drawdown
        else:
            dd_multiplier = 1.0
            
        # B. Volatility Throttling
        if vix_val > 20.0:
            vol_multiplier = max(0.1, np.exp(-0.05 * (vix_val - 20.0)))
        else:
            vol_multiplier = 1.0
            
        # C. Liquidity Throttling
        vol_ratio = float(alpha_out.factor_breakdown.get('volume_ratio', 1.0) if hasattr(alpha_out, 'factor_breakdown') else 1.0)
        if vol_ratio < 0.5:
            liq_multiplier = 0.5
        elif vol_ratio < 0.8:
            liq_multiplier = 0.8
        else:
            liq_multiplier = 1.0
            
        throttling_multiplier = dd_multiplier * vol_multiplier * liq_multiplier
        kelly_adjusted *= throttling_multiplier

        # Clip to hard bounds
        alloc_pct = float(np.clip(abs(kelly_adjusted) * 0.5, MIN_ALLOC, MAX_ALLOC))

        # ADV participation cap
        adv_capped = False
        max_qty_adv = adv * ADV_PARTICIPATION
        qty = alloc_pct * portfolio_value / max(price, 1e-6)
        if qty > max_qty_adv:
            qty = max_qty_adv
            alloc_pct = (qty * price) / max(portfolio_value, 1e-6)
            adv_capped = True

        qty = round(qty, 4)
        alloc_pct = round(alloc_pct, 5)

        log.debug(
            f"Size [{alpha_out.ticker}]: "
            f"E[r]={blended_er:+.3f} Kelly_raw={kelly_raw:.3f} "
            f"Haircut={uncertainty_haircut:.2f} Alloc={alloc_pct*100:.1f}% "
            f"Qty={qty} ADV_cap={adv_capped}"
        )

        return dict(
            alloc_pct           = alloc_pct,
            qty                 = qty,
            kelly_raw           = round(kelly_raw, 4),
            blended_er          = round(blended_er, 5),
            uncertainty_haircut = round(uncertainty_haircut, 4),
            adv_capped          = adv_capped,
        )

    def size_portfolio(
        self,
        alpha_outputs: list[AlphaOutput],
        price_history: dict[str, np.ndarray],
        portfolio_value: float,
        risk_aversion: float = 1.5,
        target_shrinkage: float = 0.5
    ) -> dict[str, float]:
        """
        Calculates joint covariance-aware weights using Ledoit-Wolf Bayesian Shrinkage.
        """
        if not alpha_outputs or not price_history:
            return {}
            
        tickers = [o.ticker for o in alpha_outputs if o.ticker in price_history]
        if len(tickers) == 0:
            return {}
            
        returns_dict = {}
        min_len = 999999
        for t in tickers:
            prices = np.array(price_history[t], dtype=float)
            if len(prices) > 2:
                # Log returns
                rets = np.diff(np.log(prices + 1e-9))
                returns_dict[t] = rets
                min_len = min(min_len, len(rets))
                
        if len(returns_dict) < 1 or min_len < 3:
            return {o.ticker: 0.05 for o in alpha_outputs}
            
        # Filter tickers to only those with valid returns data
        tickers = [t for t in tickers if t in returns_dict]
        N = len(tickers)
        if N == 0:
            return {o.ticker: 0.05 for o in alpha_outputs}

        aligned_returns = []
        for t in tickers:
            aligned_returns.append(returns_dict[t][-min_len:])
            
        # Online Bayesian Recursive Covariance tracking
        tickers_key = tuple(tickers)
        if tickers_key not in self._running_cov:
            # V13 Opt 4: LRU eviction — purge oldest entries when cache exceeds max
            if len(self._running_cov) >= self._max_cov_entries:
                oldest_key = next(iter(self._running_cov))
                del self._running_cov[oldest_key]
            self._running_cov[tickers_key] = _RunningCovariance(tickers, alpha=0.96)
        rc = self._running_cov[tickers_key]
        
        # Extract latest return for each asset in active cross-sectional order
        latest_returns = np.array([returns_dict[t][-1] if len(returns_dict[t]) > 0 else 0.0 for t in tickers], dtype=float)
        
        # Update running covariance recursively
        rc.update(latest_returns)
        S = rc.cov
        # Annualize daily covariance matrix to match the annualized expected return horizon
        S_annual = S * 252
            
        # Ledoit-Wolf Shrinkage Target matrix T (constant variance)
        mean_var = np.mean(np.diag(S_annual))
        target_matrix = np.eye(N) * mean_var
        
        # Blended Covariance Matrix
        delta = target_shrinkage
        cov_shrink = (delta * target_matrix) + ((1.0 - delta) * S_annual)
        
        # Regularization for stability
        cov_shrink += np.eye(N) * 1e-6
        
        # Bayesian Mean-Variance Optimization: w* = (1/gamma) * inv(Cov) * E[r]
        # Noise Filtering: Use continuous expected returns to prevent step-function portfolio chattering
        er_list = []
        for o in alpha_outputs:
            if o.ticker in returns_dict:
                er_list.append(o.expected_return)
        er = np.array(er_list, dtype=float)

        
        # Dynamic Risk Aversion: scale risk aversion up when average asset volatility is high to protect capital
        vols = []
        for t in tickers:
            if t in returns_dict:
                vols.append(np.std(returns_dict[t]) * np.sqrt(252))
        avg_vol = np.mean(vols) if vols else 0.20
        dynamic_risk_aversion = risk_aversion * (1.0 + max(0.0, (avg_vol - 0.20) * 3.0))

        try:
            inv_cov = np.linalg.inv(cov_shrink)
            raw_weights = (1.0 / dynamic_risk_aversion) * np.dot(inv_cov, er)
        except Exception:
            raw_weights = er * 0.1

            
        allocated_weights = {}
        kappa = 0.25  # Dynamic update rate: responsive adjustments (25% update per period)
        
        # HMM Panic Regime Capital Protection: contract exposure up to 90% in panic states
        try:
            from alpha_engine import get_engine
            p_panic = get_engine()._regime_probs[2]  # State index 2 is Panic
        except Exception:
            p_panic = 0.0
        exposure_multiplier = max(0.10, 1.0 - p_panic * 0.90)
        
        # Smooth and adjust active tickers
        for i, t in enumerate(tickers):
            w = raw_weights[i]
            # Clip weights to bounds (-10% to +10% per asset)
            w_clipped = float(np.clip(w, -0.10, 0.10))
            
            # Retrieve previous weight to penalize turnover
            w_prev = self._prev_weights.get(t, 0.0)
            w_smoothed = (1.0 - kappa) * w_prev + kappa * w_clipped
            
            w_final = float(np.clip(w_smoothed * exposure_multiplier, -0.10, 0.10))
            allocated_weights[t] = round(w_final, 4)
            self._prev_weights[t] = w_final
            
        # Orderly, gradual liquidations for assets dropped from the watchlist
        for t in list(self._prev_weights.keys()):
            if t not in tickers:
                w_prev = self._prev_weights[t]
                if abs(w_prev) > 0.005:
                    w_decayed = (1.0 - kappa) * w_prev
                    if abs(w_decayed) < 0.01:
                        w_decayed = 0.0
                    if w_decayed != 0.0:
                        allocated_weights[t] = round(w_decayed, 4)
                        self._prev_weights[t] = w_decayed
                    else:
                        self._prev_weights.pop(t, None)
                else:
                    self._prev_weights.pop(t, None)
                    
        return allocated_weights

    # ── Posterior strategy weights ────────────────────────────────────────────

    def weights(self) -> dict[str, float]:
        """
        Compute posterior strategy weights proportional to
        posterior Sharpe × sqrt(n) (reliability-weighted).

        Returns
        -------
        dict: strategy → normalized weight [0, 1], sums to 1.
        """
        scores = {}
        for name, b in self._beliefs.items():
            reliability = min(1.0, np.sqrt(max(b.n, 1)) / 10.0)
            score = max(0.01, b.sharpe_proxy * reliability)
            scores[name] = score

        total = sum(scores.values())
        return {s: round(v / total, 4) for s, v in scores.items()}

    # ── Posterior summary report ──────────────────────────────────────────────

    def report(self) -> list[dict]:
        """
        Return a summary of all posterior beliefs.
        Useful for monitoring, dashboards, and drift detection.
        """
        out = []
        for name, b in self._beliefs.items():
            out.append(dict(
                strategy    = name,
                posterior_mu     = round(b.mu, 5),
                posterior_sigma  = round(b.sigma, 5),
                n_trades         = b.n,
                win_rate         = round(b.win_rate, 3),
                sharpe_proxy     = round(b.sharpe_proxy, 3),
                total_pnl        = round(b.total_pnl, 4),
            ))
        out.sort(key=lambda x: x['sharpe_proxy'], reverse=True)
        return out

    # ── Persistence: sync beliefs from SQLite trade_history ──────────────────

    def sync_from_db(self, db_path: str = DB_PATH):
        """
        Bootstrap posterior beliefs from closed trades in trade_history.
        Call once at startup to restore state from prior sessions.
        """
        try:
            conn = sqlite3.connect(db_path, timeout=15.0)
            c    = conn.cursor()
            c.execute("""
                SELECT strategy, pnl_pct FROM trade_history
                WHERE status = 'CLOSED' AND pnl_pct IS NOT NULL
                ORDER BY exit_date ASC
            """)
            rows = c.fetchall()
            conn.close()

            count = 0
            for strategy, pnl_pct in rows:
                self.update(strategy, float(pnl_pct), obs_noise=0.03)
                count += 1

            log.info(f"Posterior beliefs bootstrapped from {count} historical trades.")
        except Exception as e:
            log.warning(f"Could not sync beliefs from DB: {e}")

    def persist_to_db(self, db_path: str = DB_PATH):
        """
        Write current posterior summaries to strategy_metrics table.
        """
        try:
            conn = sqlite3.connect(db_path, timeout=15.0)
            c    = conn.cursor()
            for name, b in self._beliefs.items():
                c.execute("""
                    UPDATE strategy_metrics
                    SET win_rate = ?, sharpe_ratio = ?, total_trades = ?,
                        avg_win = ?, avg_loss = ?
                    WHERE strategy = ?
                """, (
                    round(b.win_rate, 4),
                    round(b.sharpe_proxy, 4),
                    b.n,
                    round(max(b.mu, 0.0), 4),
                    round(max(-b.mu, 0.0), 4),
                    name,
                ))
            conn.commit()
            conn.close()
        except Exception as e:
            log.warning(f"Could not persist beliefs to DB: {e}")


# ── Module-level singleton ────────────────────────────────────────────────────

_portfolio = BayesianPortfolio()


def get_portfolio() -> BayesianPortfolio:
    """Return the module-level singleton BayesianPortfolio."""
    return _portfolio

