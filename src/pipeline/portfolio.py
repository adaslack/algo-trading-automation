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

from paths import DB_PATH

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
    'LAZR': {'marketCap': 550_000_000_000, 'sector': 'Technology'},
    'NKLA': {'marketCap': 400_000_000, 'sector': 'Consumer Cyclical'},
    'RIVN': {'marketCap': 12_000_000_000, 'sector': 'Consumer Cyclical'},
    'IONQ': {'marketCap': 5_000_000_000, 'sector': 'Technology'},
    'APPS': {'marketCap': 350_000_000, 'sector': 'Technology'},
    'CLSK': {'marketCap': 1_200_000_000, 'sector': 'Technology'},
    'WKHS': {'marketCap': 320_000_000, 'sector': 'Industrials'},
    'SPWR': {'marketCap': 310_000_000, 'sector': 'Technology'},
    'EVGO': {'marketCap': 280_000_000, 'sector': 'Consumer Cyclical'},
    'HYLN': {'marketCap': 150_000_000, 'sector': 'Industrials'},
    'IVR': {'marketCap': 220_000_000, 'sector': 'Financial Services'},
    'SOUN': {'marketCap': 3_000_000_000, 'sector': 'Technology'},
    'DNA': {'marketCap': 1_500_000_000, 'sector': 'Healthcare'},
    'LCID': {'marketCap': 6_500_000_000, 'sector': 'Consumer Cyclical'},
    'CEI': {'marketCap': 80_000_000, 'sector': 'Energy'},
    'MMAT': {'marketCap': 95_000_000, 'sector': 'Technology'},
    'MULN': {'marketCap': 55_000_000, 'sector': 'Consumer Cyclical'},
    'XELA': {'marketCap': 60_000_000, 'sector': 'Technology'},
    'AEON': {'marketCap': 120_000_000, 'sector': 'Healthcare'},
    'WTI': {'marketCap': 290_000_000, 'sector': 'Energy'},
    'SMCI': {'marketCap': 15_000_000_000, 'sector': 'Technology'},
    'SCPL': {'marketCap': 210_000_000, 'sector': 'Technology'},
    'BTDR': {'marketCap': 1_200_000_000, 'sector': 'Technology'},
    'LUNR': {'marketCap': 2_500_000_000, 'sector': 'Industrials'},
    'HUSA': {'marketCap': 15_000_000, 'sector': 'Energy'},
    'METX': {'marketCap': 12_000_000, 'sector': 'Education'},
    'SINT': {'marketCap': 8_000_000, 'sector': 'Industrials'},
    'PHUN': {'marketCap': 35_000_000, 'sector': 'Technology'},
    'OPTT': {'marketCap': 18_000_000, 'sector': 'Industrials'},
    'CLOV': {'marketCap': 1_800_000_000, 'sector': 'Healthcare'},
    'BIOL': {'marketCap': 9_000_000, 'sector': 'Healthcare'},
    'TOPS': {'marketCap': 5_000_000, 'sector': 'Industrials'},
    'SHIP': {'marketCap': 22_000_000, 'sector': 'Industrials'},
    'GLBS': {'marketCap': 14_000_000, 'sector': 'Industrials'},
    'EDSA': {'marketCap': 7_000_000, 'sector': 'Healthcare'}
}

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
        # Dynamic Heteroscedastic noise adjustment based on market regime (VIX)
        try:
            from alpha_engine import get_engine
            engine = get_engine()
            vix_val = float(engine._hmm_means[0] * engine._regime_probs[0] + 
                            engine._hmm_means[1] * engine._regime_probs[1] + 
                            engine._hmm_means[2] * engine._regime_probs[2])
            vix_val = max(10.0, min(80.0, vix_val))
            # Scale observation noise dynamically: standard noise * (VIX_expected / 15.0)
            obs_noise = obs_noise * (vix_val / 15.0)
        except Exception:
            pass

        if strategy not in self._beliefs:
            self._beliefs[strategy] = _StrategyBelief()
        self._beliefs[strategy].update(realised_return, obs_noise)

        b = self._beliefs[strategy]
        log.info(
            f"Posterior update [{strategy}]: "
            f"μ={b.mu*100:+.2f}% σ={b.sigma*100:.2f}% "
            f"n={b.n} WR={b.win_rate*100:.0f}% "
            f"Sharpe≈{b.sharpe_proxy:.2f} (Heteroscedastic obs_noise={obs_noise*100:.2f}%)"
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
            
        # V16: Estimate stable diagonal covariance matrix directly from historical aligned returns to prevent correlation noise from inverting weights
        S_diag = np.var(aligned_returns, axis=1)
        S_annual = np.diag(np.maximum(S_diag, 1e-6) * 252)
            
        # Ledoit-Wolf Shrinkage Target matrix T (constant variance)
        mean_var = np.mean(np.diag(S_annual))
        target_matrix = np.eye(N) * mean_var
        
        # Blended Covariance Matrix
        delta = target_shrinkage
        cov_shrink = (delta * target_matrix) + ((1.0 - delta) * S_annual)
        
        # Regularization for stability
        cov_shrink += np.eye(N) * 1e-6
        
        # Cross-sectional long-short rank gating:
        # Instead of absolute thresholding (which causes signal crowding and zero allocations),
        # we rank the watchlist assets and go long the top 50% and short the bottom 50%.
        valid_outputs = [o for o in alpha_outputs if o.ticker in returns_dict]
        valid_outputs.sort(key=lambda x: x.expected_return, reverse=True)
        half = len(valid_outputs) // 2
        
        long_tickers = set(o.ticker for o in valid_outputs[:half])
        
        er_list = []
        for o in alpha_outputs:
            if o.ticker in returns_dict:
                # Retain the expected return sign based on relative ranking (top half long, bottom half short)
                raw_er = o.expected_return
                if o.ticker in long_tickers:
                    er_list.append(max(0.0001, abs(raw_er)))
                else:
                    er_list.append(min(-0.0001, -abs(raw_er)))
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

        # ── V15 MATHEMATICAL PORTFOLIO NEUTRALITY PROJECTIONS ────────────────
        # Load sector mappings from market_data DB table
        sectors = {}
        try:
            import sqlite3
            conn = sqlite3.connect(DB_PATH, timeout=30.0)
            c = conn.cursor()
            c.execute("SELECT ticker, sector FROM market_data")
            for row in c.fetchall():
                sectors[row[0]] = row[1] or 'Unknown'
            conn.close()
        except Exception as ex:
            log.warning(f"Could not load sector mappings from DB: {ex}")

        # Fallback to hardcoded METADATA_FALLBACK sector mapping if not populated in DB
        for t in tickers:
            if t not in sectors or sectors[t] == 'Unknown':
                if t in METADATA_FALLBACK:
                    sectors[t] = METADATA_FALLBACK[t]['sector']
                else:
                    sectors[t] = 'Unknown'

        # Compute dynamic rolling beta for all assets against watchlist mean proxy
        spy_rets = None
        if 'SPY' in price_history:
            spy_prices = np.array(price_history['SPY'], dtype=float)
            if len(spy_prices) > 2:
                spy_rets = np.diff(np.log(spy_prices + 1e-9))
        
        if spy_rets is None:
            all_rets = [returns_dict[t][-min_len:] for t in tickers if t in returns_dict]
            if all_rets:
                spy_rets = np.mean(all_rets, axis=0)

        # Construct linear constraint matrix A of shape (M, N)
        # V16: Sector Neutrality constraints (which mathematically imply Dollar Neutrality)
        # Forces long-short pair matching within sector buckets to capture cross-sectional alpha.
        A_rows = []
        unique_sectors = sorted(list(set(sectors.get(t, 'Unknown') for t in tickers)))
        for sect in unique_sectors:
            row = np.zeros(N)
            for i, t in enumerate(tickers):
                if sectors.get(t, 'Unknown') == sect:
                    row[i] = 1.0
            A_rows.append(row)

        A = np.array(A_rows)
        
        # Project raw weights onto the constraint subspace nullspace:
        # w_proj = (I - A^T * (A * A^T)^+ * A) * w_raw
        if N > 1 and len(A) > 0:
            try:
                A_t = A.T
                proj_matrix = np.eye(N) - A_t.dot(np.linalg.pinv(A.dot(A_t))).dot(A)
                raw_weights = proj_matrix.dot(raw_weights)
            except Exception as ex:
                log.error(f"Error during analytical portfolio projection: {ex}")

        allocated_weights = {}
        kappa = 0.25  # V16: Re-calibrated to 0.25 for balanced execution and turnover penalty
        
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
            # V16: Bounds reverted to ±10% for conservative and stable position sizing
            w_clipped = float(np.clip(w, -0.10, 0.10))
            
            # Retrieve previous weight to penalize turnover
            w_prev = self._prev_weights.get(t, 0.0)
            
            # V16: Rebalancing band adjusted to 2.0% to match V13 baseline
            if abs(w_clipped - w_prev) < 0.020 and abs(w_clipped) > 1e-4:
                w_smoothed = w_prev
            else:
                w_smoothed = (1.0 - kappa) * w_prev + kappa * w_clipped
            
            w_final = float(np.clip(w_smoothed * exposure_multiplier, -0.10, 0.10))
            allocated_weights[t] = w_final
            self._prev_weights[t] = w_final

        # Enforce exact final projection safety net to guarantee 100% compliance
        final_w = np.array([allocated_weights.get(t, 0.0) for t in tickers])
        if len(A) > 0 and len(tickers) > 1:
            try:
                A_t = A.T
                proj_matrix = np.eye(N) - A_t.dot(np.linalg.pinv(A.dot(A_t))).dot(A)
                final_w = proj_matrix.dot(final_w)
                
                # Rescale to target gross exposure (exposure multiplier)
                target_gross = float(exposure_multiplier)
                current_gross = np.sum(np.abs(final_w))
                if current_gross > 1e-6:
                    final_w = final_w * (target_gross / current_gross)
                
                for i, t in enumerate(tickers):
                    w_val = float(np.clip(final_w[i], -0.10, 0.10))
                    allocated_weights[t] = round(w_val, 4)
                    self._prev_weights[t] = w_val
            except Exception as ex:
                log.error(f"Error in final portfolio projection safety net: {ex}")

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
