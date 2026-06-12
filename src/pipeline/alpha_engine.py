"""
Alpha Engine — Unified Probabilistic Expected Return Surface (V13 Overhaul)
=============================================================================
Complete rewrite of alpha factor pipeline. Replaces fake proxy features
(OBI, VPIN, GEX, insider_score derived from same-day returns) with real,
academically-validated predictive factors.

Architecture:
  AlphaEngine.evaluate(snapshot) -> AlphaOutput

Inputs (real predictive features):
  - momentum_12_1   : 12-month minus 1-month return (Jegadeesh-Titman momentum)
  - reversal_5d     : 5-day return reversal signal
  - volume_breakout : Short-term volume surge vs rolling average
  - vol_regime      : Realized vol ratio (5d/20d) — volatility regime shift
  - trend_strength  : Hurst-adjusted directional trend quality
  - garch_vol       : Realized conditional volatility (risk scalar)

Output (AlphaOutput):
  - expected_return  : E[r] — raw probabilistic expected return
  - confidence       : posterior confidence [0, 1]
  - decay_half_life  : estimated signal half-life in minutes
  - liquidity_cost   : estimated round-trip execution cost in bps
  - signal           : 'BUY' | 'SELL' | None (threshold gated)
  - cross_rank       : cross-sectional Z-score rank (populated by rank_universe)

Cross-sectional ranking:
  AlphaEngine.rank_universe(snapshots) -> list[AlphaOutput] sorted by E[r]
  Used for L/S pair selection.
"""
import numpy as np
from dataclasses import dataclass, field
from typing import Optional
from logger import get_logger
from ml_ranker import CrossSectionalMLRanker, MetaLabelClassifier

log = get_logger("AlphaEngine")

# Signal thresholds (lowered from ±2% to ±0.5% to generate actionable signals)
LONG_THRESHOLD  =  0.005   # E[r] > +0.5% → BUY
SHORT_THRESHOLD = -0.005   # E[r] < -0.5% → SELL

# VIX regime gates
VIX_CAUTION = 20.0
VIX_PANIC   = 30.0


# ── Output dataclass ──────────────────────────────────────────────────────────

@dataclass
class AlphaOutput:
    ticker: str
    expected_return:  float       # E[r]: signed, continuous
    confidence:       float       # posterior confidence [0, 1]
    decay_half_life:  float       # signal half-life (minutes)
    liquidity_cost:   float       # estimated round-trip cost (bps)
    signal:           Optional[str] = None   # 'BUY' | 'SELL' | None
    cross_rank:       float = 0.0            # cross-sectional Z-score
    factor_breakdown: dict = field(default_factory=dict)


# ── Engine ────────────────────────────────────────────────────────────────────

class AlphaEngine:
    """
    V13 Unified Alpha Engine with real predictive factors.

    Call evaluate() for single-ticker signals.
    Call rank_universe() for cross-sectional L/S selection.
    """

    def __init__(self):
        self.calib_alpha = 0.0
        self.calib_beta = 1.0   # No downscaling — trust the signals
        self._regime_probs = np.array([0.65, 0.25, 0.10], dtype=float)
        self._transition_matrix = np.array([
            [0.90, 0.08, 0.02],
            [0.15, 0.80, 0.05],
            [0.05, 0.20, 0.75]
        ], dtype=float)
        # 3-State HMM Gaussian parameters (means and variances/covariances)
        # Initial priors: Stable (14.0), Caution (22.0), Panic (35.0)
        self._hmm_means = np.array([14.0, 22.0, 35.0], dtype=float)
        self._hmm_covars = np.array([4.0, 16.0, 64.0], dtype=float)
        
        # Factor weights (empirically tuned, can be overridden by calibration engine)
        self._factor_weights = {
            'momentum':  0.30,   # Cross-sectional momentum (strongest documented factor)
            'reversal':  0.20,   # Short-term mean reversion
            'volume':    0.15,   # Volume breakout signal
            'vol_regime': 0.15,  # Volatility regime shift
            'trend':     0.20,   # Hurst-adjusted trend quality
        }
        
        # V14 Alpha Excellence ML Models
        self.ml_ranker = None
        self.meta_labeler = None

    def calibrate_hmm_from_data(self, vix_history: list[float]):
        """
        Dynamically fits a 3-State Gaussian Hidden Markov Model sequence using Baum-Welch Expectation-Maximization,
        estimating latent transition matrices (A_jk) and emission parameters (mu_k, covar_k) recursively.
        """
        try:
            O = np.array(vix_history, dtype=float)
            T = O.size
            if T < 30:
                log.warning("Insufficient observations to run HMM Baum-Welch EM. Using default priors.")
                return

            # Initialize states via quantile splits to guarantee semantic ordering: Stable < Caution < Panic
            O_sorted = np.sort(O)
            q50 = np.percentile(O_sorted, 50.0)
            q85 = np.percentile(O_sorted, 85.0)
            
            states = np.zeros(T, dtype=int)
            states[O > q50] = 1
            states[O > q85] = 2

            # M-Step: Initialize parameters
            means = np.zeros(3)
            covars = np.zeros(3)
            for k in range(3):
                cluster = O[states == k]
                means[k] = np.mean(cluster) if cluster.size > 0 else [14.0, 22.0, 35.0][k]
                covars[k] = np.var(cluster) if cluster.size > 0 else [4.0, 16.0, 64.0][k]
                
            # Sort states to ensure Stable < Caution < Panic
            sorted_indices = np.argsort(means)
            means = means[sorted_indices]
            covars = np.maximum(covars[sorted_indices], 1e-2)

            # Initialize transition matrix and priors
            A = np.array([
                [0.90, 0.08, 0.02],
                [0.15, 0.80, 0.05],
                [0.05, 0.20, 0.75]
            ], dtype=float)
            pi = np.array([0.65, 0.25, 0.10], dtype=float)

            # Baum-Welch Expectation-Maximization Iterative Loop
            max_iter = 20
            tolerance = 1e-4
            prev_log_lik = -np.inf

            for iteration in range(max_iter):
                # ── 1. Expectation Step (E-step) ──────────────────────────────────
                # Calculate emission probabilities B_t(k) = N(O_t; mu_k, covar_k)
                B = np.zeros((T, 3))
                for k in range(3):
                    B[:, k] = (1.0 / np.sqrt(2.0 * np.pi * covars[k])) * np.exp(-((O - means[k]) ** 2) / (2.0 * covars[k]))
                B = np.maximum(B, 1e-10)

                # Forward recursion (Alpha) with scaling to prevent numerical underflow
                alpha = np.zeros((T, 3))
                c = np.zeros(T) # Scaling factors
                
                alpha[0] = pi * B[0]
                c[0] = 1.0 / np.sum(alpha[0])
                alpha[0] *= c[0]
                
                for t in range(1, T):
                    alpha[t] = np.dot(alpha[t-1], A) * B[t]
                    c[t] = 1.0 / np.sum(alpha[t])
                    alpha[t] *= c[t]

                # Backward recursion (Beta) with same scaling
                beta = np.zeros((T, 3))
                beta[T-1] = np.ones(3) * c[T-1]
                
                for t in range(T-2, -1, -1):
                    beta[t] = np.dot(beta[t+1] * B[t+1], A.T) * c[t]

                # Compute posteriors (Gamma) and transition posteriors (Xi)
                gamma = alpha * beta
                gamma = gamma / np.sum(gamma, axis=1, keepdims=True)

                xi = np.zeros((T-1, 3, 3))
                for t in range(T-1):
                    denom = np.dot(np.dot(alpha[t], A), B[t+1] * beta[t+1])
                    for i in range(3):
                        xi[t, i] = alpha[t, i] * A[i] * B[t+1] * beta[t+1] / max(denom, 1e-10)

                # ── 2. Maximization Step (M-step) ──────────────────────────────────
                # Update transition matrix A and state priors pi
                pi = gamma[0] / np.sum(gamma[0])
                
                sum_xi = np.sum(xi, axis=0)
                sum_gamma = np.sum(gamma[:-1], axis=0).reshape(-1, 1)
                A = sum_xi / np.maximum(sum_gamma, 1e-10)
                A = np.clip(A, 1e-4, 1.0)
                A = A / A.sum(axis=1, keepdims=True)

                # Update emission parameters (means & variances)
                for k in range(3):
                    sum_g = np.sum(gamma[:, k])
                    means[k] = np.sum(gamma[:, k] * O) / max(sum_g, 1e-10)
                    covars[k] = np.sum(gamma[:, k] * ((O - means[k]) ** 2)) / max(sum_g, 1e-10)
                covars = np.maximum(covars, 1e-2)

                # Check Log-Likelihood convergence
                log_lik = -np.sum(np.log(c))
                if abs(log_lik - prev_log_lik) < tolerance:
                    break
                prev_log_lik = log_lik

            # Update active parameters with fully optimized latent sequences
            self._transition_matrix = A
            self._hmm_means = means
            self._hmm_covars = covars

            log.info(f"Baum-Welch EM HMM Calibrated. Transition Matrix:\n{self._transition_matrix}")
            log.info(f"HMM Stable (μ={self._hmm_means[0]:.1f}, σ²={self._hmm_covars[0]:.2f})")
            log.info(f"HMM Caution (μ={self._hmm_means[1]:.1f}, σ²={self._hmm_covars[1]:.2f})")
            log.info(f"HMM Panic (μ={self._hmm_means[2]:.1f}, σ²={self._hmm_covars[2]:.2f})")
            
        except Exception as e:
            log.warning(f"Failed Baum-Welch HMM EM fitting: {e}. Reverting to standard parameterized estimators.")

    def calibrate_empirical(self, db_path: str):
        """
        Dynamically updates the linear calibration model (OLS regression)
        relating predicted raw E[r] to realized trade returns. Also calibrates
        latent HMM transition variables using expectation maximization.

        When predicted_er is stored alongside trades, fits:
            r_actual = alpha + beta * r_predicted
        Otherwise falls back to marginal PnL statistics.
        """
        try:
            import sqlite3
            conn = sqlite3.connect(db_path, timeout=15.0)
            c = conn.cursor()

            # Attempt conditional calibration: predicted vs realized (preferred)
            has_predicted_er = False
            try:
                c.execute("""
                    SELECT predicted_er, pnl_pct FROM trade_history
                    WHERE status = 'CLOSED' AND pnl_pct IS NOT NULL AND predicted_er IS NOT NULL
                    ORDER BY exit_date DESC LIMIT 100
                """)
                paired_rows = c.fetchall()
                if len(paired_rows) >= 10:
                    has_predicted_er = True
            except Exception:
                paired_rows = []

            # Fetch marginal PnL as fallback
            c.execute("""
                SELECT pnl_pct FROM trade_history
                WHERE status = 'CLOSED' AND pnl_pct IS NOT NULL
                ORDER BY exit_date DESC LIMIT 50
            """)
            rows = c.fetchall()

            # Fetch historical VIX levels to calibrate the HMM
            c.execute("SELECT vix_level FROM macro_sentiment ORDER BY timestamp DESC LIMIT 200")
            vix_rows = c.fetchall()
            conn.close()

            if vix_rows:
                vix_vals = [float(v[0]) for v in vix_rows if v[0] is not None]
                self.calibrate_hmm_from_data(vix_vals)

            if has_predicted_er and len(paired_rows) >= 10:
                # True OLS calibration: r_actual = alpha + beta * r_predicted
                predicted = np.array([float(r[0]) for r in paired_rows], dtype=float)
                realized = np.array([float(r[1]) for r in paired_rows], dtype=float)
                # OLS closed-form: beta = Cov(pred, real) / Var(pred), alpha = mean(real) - beta * mean(pred)
                cov_pr = np.cov(predicted, realized)
                var_pred = max(cov_pr[0, 0], 1e-10)
                self.calib_beta = float(np.clip(cov_pr[0, 1] / var_pred, 0.05, 2.0))
                self.calib_alpha = float(np.clip(
                    np.mean(realized) - self.calib_beta * np.mean(predicted), -0.02, 0.02
                ))
                log.info(
                    f"OLS Forecast Calibrated (n={len(paired_rows)}): "
                    f"α={self.calib_alpha*100:+.3f}%, β={self.calib_beta:.4f}"
                )
            elif len(rows) >= 5:
                # Fallback: marginal PnL statistics calibration
                actuals = np.array([float(r[0]) for r in rows], dtype=float)
                mean_pnl = float(np.mean(actuals))
                std_pnl = float(np.std(actuals))
                self.calib_beta = float(np.clip(std_pnl / max(std_pnl + 0.10, 1e-4), 0.05, 1.0))
                self.calib_alpha = float(np.clip(mean_pnl, -0.02, 0.02))
                log.info(f"Marginal PnL Calibrated: α={self.calib_alpha*100:+.3f}%, β={self.calib_beta:.4f}")
        except Exception as e:
            log.warning(f"Could not calibrate expected return empirically: {e}")

    # ── Single-ticker evaluation ──────────────────────────────────────────────

    def evaluate(self, ticker: str, snapshot: dict, vix: float = 15.0) -> AlphaOutput:
        """
        Compute probabilistic E[r] for a single ticker using real predictive factors.

        Parameters
        ----------
        ticker   : ticker symbol
        snapshot : feature dict containing real predictive factors
        vix      : current VIX level (macro vol gate)

        Returns
        -------
        AlphaOutput with all continuous metrics populated.
        """
        close       = snapshot.get('close_price', 0.0) or snapshot.get('close', 0.0)
        garch_vol   = float(snapshot.get('garch_volatility', 0.02)) or 0.02
        volume_ratio = float(snapshot.get('volume_ratio',  1.0)) or 1.0

        if close <= 0:
            return AlphaOutput(ticker=ticker, expected_return=0.0, confidence=0.0,
                               decay_half_life=0.0, liquidity_cost=0.0)

        # ── Extract real predictive features ──────────────────────────────────
        # These are pre-computed from historical price/volume data in the backtester
        f_momentum  = float(snapshot.get('momentum_12_1', 0.0))    # 12-1 month momentum
        f_reversal  = float(snapshot.get('reversal_5d', 0.0))      # 5-day return reversal
        f_volume    = float(snapshot.get('volume_breakout', 0.0))   # Volume surge signal
        f_vol_regime = float(snapshot.get('vol_regime', 0.0))       # Volatility regime shift
        f_trend     = float(snapshot.get('trend_strength', 0.0))    # Hurst-adjusted trend

        # ── Dynamic 3-State HMM Volatility Regime Classifier ──────────────────
        vix_val = max(1e-3, vix)
        
        # Emission likelihoods under Baum-Welch EM calibrated states
        L_stable  = (1.0 / np.sqrt(2.0 * np.pi * self._hmm_covars[0])) * np.exp(-((vix_val - self._hmm_means[0]) ** 2) / (2.0 * self._hmm_covars[0]))
        L_caution = (1.0 / np.sqrt(2.0 * np.pi * self._hmm_covars[1])) * np.exp(-((vix_val - self._hmm_means[1]) ** 2) / (2.0 * self._hmm_covars[1]))
        L_panic   = (1.0 / np.sqrt(2.0 * np.pi * self._hmm_covars[2])) * np.exp(-((vix_val - self._hmm_means[2]) ** 2) / (2.0 * self._hmm_covars[2]))
        
        likelihoods = np.array([L_stable, L_caution, L_panic], dtype=float)
        
        # Recursive HMM forward pass
        p_predicted = np.dot(self._regime_probs, self._transition_matrix)
        posteriors = likelihoods * p_predicted
        
        denom = np.sum(posteriors)
        if denom > 1e-12:
            self._regime_probs = posteriors / denom
        else:
            self._regime_probs = p_predicted
            
        p_stable, p_caution, p_panic = self._regime_probs

        # ── Regime-Adaptive Factor Weights ────────────────────────────────────
        # In panic: momentum crashes (factor reversal), increase reversal weight
        # In stable: full momentum effect
        w = {
            'momentum':  self._factor_weights['momentum']  * (p_stable * 1.0 + p_caution * 0.6 + p_panic * 0.2),
            'reversal':  self._factor_weights['reversal']  * (p_stable * 1.0 + p_caution * 1.3 + p_panic * 1.8),
            'volume':    self._factor_weights['volume']    * (p_stable * 1.0 + p_caution * 1.0 + p_panic * 0.8),
            'vol_regime': self._factor_weights['vol_regime'] * (p_stable * 1.0 + p_caution * 1.2 + p_panic * 1.5),
            'trend':     self._factor_weights['trend']     * (p_stable * 1.0 + p_caution * 0.8 + p_panic * 0.4),
        }
        
        # Normalize weights to sum to 1.0
        sum_w = sum(w.values())
        if sum_w > 0:
            w = {k: v / sum_w for k, v in w.items()}

        # ── Compute Expected Return ───────────────────────────────────────────
        if hasattr(self, 'ml_ranker') and self.ml_ranker is not None and self.ml_ranker.is_fitted:
            feat_row = [
                float(snapshot.get('momentum_12_1', 0.0)),
                float(snapshot.get('reversal_5d', 0.0)),
                float(snapshot.get('realized_volatility', snapshot.get('garch_volatility', 0.02))),
                float(snapshot.get('volume_breakout', 0.0)),
                float(snapshot.get('rolling_correlation', 0.0)),
                float(snapshot.get('rolling_beta', 1.0)),
                float(snapshot.get('volatility_expansion', 1.0)),
                float(snapshot.get('drawdown_depth', 0.0)),
                float(snapshot.get('sector_relative_strength', 0.0))
            ]
            try:
                predicted_percentile = self.ml_ranker.predict([feat_row])[0]
                # Center and scale to expected return range ±5%
                expected_return = (predicted_percentile - 50.0) / 50.0 * 0.05
            except Exception as e:
                # fallback
                expected_return = (
                    w['momentum']   * f_momentum +
                    w['reversal']   * f_reversal +
                    w['volume']     * f_volume +
                    w['vol_regime'] * f_vol_regime +
                    w['trend']      * f_trend
                )
        else:
            # Direct weighted combination — no broken Z-score + tanh pipeline
            expected_return = (
                w['momentum']   * f_momentum +
                w['reversal']   * f_reversal +
                w['volume']     * f_volume +
                w['vol_regime'] * f_vol_regime +
                w['trend']      * f_trend
            )

        # Apply empirical calibration
        expected_return = self.calib_alpha + self.calib_beta * expected_return

        # ── Continuous confidence: factor agreement ───────────────────────────
        # Confidence is higher when multiple factors agree on direction
        factor_vals = [f_momentum, f_reversal, f_volume, f_vol_regime, f_trend]
        signs = [1 if f > 0 else (-1 if f < 0 else 0) for f in factor_vals]
        agreement = abs(sum(signs)) / max(len(signs), 1)
        
        # Scale by signal magnitude and VIX dampening
        magnitude = abs(expected_return) / max(garch_vol * 5.0, 0.01)
        vix_damp = float(p_stable * 1.0 + p_caution * 0.8 + p_panic * 0.5)
        confidence = float(min(1.0, max(0.0, agreement * magnitude * vix_damp)))

        # ── Decay half-life (minutes) ─────────────────────────────────────────
        # Momentum signals have long half-life; reversal signals have short half-life
        base_hl = 45.0
        if abs(f_momentum) > abs(f_reversal):
            decay_half_life = float(base_hl * 2.0)  # Momentum: slow decay
        else:
            decay_half_life = float(base_hl * 0.5)  # Reversal: fast decay

        # Scale decay half-life inversely with VPIN (high adverse selection = faster decay)
        vpin_val = float(snapshot.get('vpin', 0.3))
        vpin_scaler = max(0.2, 1.5 - vpin_val * 1.5)
        decay_half_life = decay_half_life * vpin_scaler

        # ── Liquidity cost in bps ─────────────────────────────────────────────
        base_spread = 2.0
        impact_cost = max(0.0, 1.0 / max(volume_ratio, 0.1) - 1.0) * 3.0
        liquidity_cost = float(base_spread + impact_cost)

        # ── Signal threshold gate ─────────────────────────────────────────────
        # V13 Arch 3 FIX: Net-of-cost threshold gating
        cost_frac = liquidity_cost / 10000.0
        signal = None
        if (expected_return - cost_frac) > LONG_THRESHOLD and vix < VIX_PANIC:
            signal = 'BUY'
        elif (expected_return + cost_frac) < SHORT_THRESHOLD and vix < VIX_PANIC:
            signal = 'SELL'

        # ── Meta-Label Gating ─────────────────────────────────────────────────
        meta_prob = 1.0
        if signal is not None and hasattr(self, 'meta_labeler') and self.meta_labeler is not None and self.meta_labeler.is_fitted:
            meta_row = [
                float(liquidity_cost),
                float(vix),
                float(volume_ratio),
                float(np.sign(expected_return)),
                float(garch_vol),
                0.0 # base sector regime
            ]
            try:
                meta_prob = float(self.meta_labeler.predict_proba([meta_row])[0])
                if meta_prob < 0.55:
                    # Suppress signal due to low probability of success
                    log.info(f"Signal for {ticker} suppressed by Meta-Labeling: Prob={meta_prob:.2f}")
                    signal = None
            except Exception as e:
                pass

        return AlphaOutput(
            ticker          = ticker,
            expected_return = round(expected_return, 6),
            confidence      = round(confidence, 4),
            decay_half_life = round(decay_half_life, 1),
            liquidity_cost  = round(liquidity_cost, 2),
            signal          = signal,
            factor_breakdown = dict(
                f_momentum  = round(f_momentum, 5),
                f_reversal  = round(f_reversal, 5),
                f_volume    = round(f_volume,   5),
                f_vol_regime = round(f_vol_regime, 5),
                f_trend     = round(f_trend,    5),
                meta_prob   = round(meta_prob,   4)
            )
        )

    # ── Cross-sectional ranking ───────────────────────────────────────────────

    def rank_universe(
        self,
        universe: dict[str, dict],
        vix: float = 15.0
    ) -> list[AlphaOutput]:
        """
        Evaluate all tickers in `universe` and rank by E[r].

        Cross-sectional Z-score of E[r] is added to each AlphaOutput.cross_rank.
        Returns list sorted descending by expected_return.

        Parameters
        ----------
        universe : {ticker: feature_snapshot}
        vix      : current VIX level

        Returns
        -------
        Sorted list of AlphaOutput (index 0 = highest E[r] = best long).
        """
        outputs = []
        for ticker, snapshot in universe.items():
            out = self.evaluate(ticker, snapshot, vix=vix)
            outputs.append(out)

        if not outputs:
            return []

        # Cross-sectional Z-score of E[r]
        er_arr = np.array([o.expected_return for o in outputs])
        mean_er = np.mean(er_arr)
        std_er  = np.std(er_arr)
        if std_er > 1e-6:
            z_scores = (er_arr - mean_er) / std_er
        else:
            z_scores = np.zeros(len(outputs))

        for i, out in enumerate(outputs):
            out.cross_rank = round(float(z_scores[i]), 4)

        outputs.sort(key=lambda o: o.expected_return, reverse=True)
        return outputs


# ── Module-level singleton ────────────────────────────────────────────────────

_engine = AlphaEngine()


def get_engine() -> AlphaEngine:
    """Return the module-level singleton AlphaEngine."""
    return _engine

