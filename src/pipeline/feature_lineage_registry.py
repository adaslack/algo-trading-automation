"""
Institutional Feature Lineage Registry
======================================
Tracks feature metadata, lineage formulas, compute parameters, versions, 
Spearman IC history, and live Kolmogorov-Smirnov distribution drift warnings.
"""

import numpy as np
from dataclasses import dataclass, field
from typing import Optional, Any
from logger import get_logger

log = get_logger("FeatureLineageRegistry")

@dataclass
class FeatureMetadata:
    name: str
    version: str
    formula: str
    parameters: dict[str, Any] = field(default_factory=dict)
    spearman_ic_history: list[float] = field(default_factory=list)
    reference_distribution: np.ndarray = field(default_factory=lambda: np.array([], dtype=float))
    inference_history: list[float] = field(default_factory=list)
    decay_half_life_history: list[float] = field(default_factory=list)

class FeatureLineageRegistry:
    """
    Central repository for feature auditing, lineage tracking, and live statistical defense.
    Prevents silent feature drift and model degradation.
    """

    def __init__(self):
        self._registry: dict[str, FeatureMetadata] = {}

    def register_feature(
        self,
        name: str,
        version: str,
        formula: str,
        parameters: dict[str, Any],
        reference_distribution: Optional[np.ndarray] = None
    ) -> None:
        """Register a feature in the lineage log with its reference training distribution."""
        if reference_distribution is None:
            reference_distribution = np.array([], dtype=float)
        else:
            reference_distribution = np.array(reference_distribution, dtype=float)

        self._registry[name] = FeatureMetadata(
            name=name,
            version=version,
            formula=formula,
            parameters=parameters,
            reference_distribution=reference_distribution
        )
        log.info(f"Registered Feature Lineage: '{name}' [v{version}] | Formula: {formula}")

    def log_inference_value(self, name: str, value: float) -> None:
        """Record new inference-time values to monitor for live distribution shifts."""
        if name not in self._registry:
            return
        meta = self._registry[name]
        meta.inference_history.append(float(value))
        
        # Defer reference distribution seeding until we collect 100 real observations
        if meta.reference_distribution.size == 0:
            if len(meta.inference_history) >= 100:
                meta.reference_distribution = np.array(meta.inference_history[:100], dtype=float)
                log.info(f"Empirical reference distribution of 100 samples seeded for feature '{name}' from live observations.")

        # Keep inference window bounded to last 500 ticks for dynamic drift testing
        if len(meta.inference_history) > 500:
            meta.inference_history.pop(0)

    def log_spearman_ic(self, name: str, ic: float) -> None:
        """Record rolling Spearman Information Coefficient (IC) metrics."""
        if name not in self._registry:
            return
        self._registry[name].spearman_ic_history.append(float(ic))

    def log_decay_half_life(self, name: str, half_life: float) -> None:
        """Record estimated feature decay rate / signal half-life in minutes."""
        if name not in self._registry:
            return
        self._registry[name].decay_half_life_history.append(float(half_life))

    def check_drift(self, name: str, alpha: float = 0.05) -> dict[str, Any]:
        """
        Perform a two-sample Kolmogorov-Smirnov test to detect feature drift.
        Compares the reference (training) distribution against recent live inference observations.
        """
        if name not in self._registry:
            return {"status": "UNKNOWN", "message": "Feature not registered"}

        meta = self._registry[name]
        ref = meta.reference_distribution
        inf = np.array(meta.inference_history, dtype=float)

        if ref.size < 10 or inf.size < 10:
            return {
                "status": "INSUFFICIENT_DATA",
                "reference_samples": ref.size,
                "inference_samples": inf.size,
                "message": "Need at least 10 samples in both reference and inference sets to run KS test"
            }

        # Run Kolmogorov-Smirnov 2-sample test
        try:
            from scipy.stats import ks_2samp
            statistic, p_value = ks_2samp(ref, inf)
        except ImportError:
            # High-performance analytical KS-test fallback if SciPy is not available
            # Computes the empirical cumulative distribution functions and finds the max distance
            ref_sorted = np.sort(ref)
            inf_sorted = np.sort(inf)
            all_vals = np.concatenate([ref_sorted, inf_sorted])
            
            # ECDF calculations
            ref_ecdf = np.searchsorted(ref_sorted, all_vals, side='right') / len(ref)
            inf_ecdf = np.searchsorted(inf_sorted, all_vals, side='right') / len(inf)
            
            statistic = float(np.max(np.abs(ref_ecdf - inf_ecdf)))
            
            # Analytical critical value approximation for KS test:
            # D_alpha = c(alpha) * sqrt((n1 + n2) / (n1 * n2))
            # for alpha=0.05, c(alpha) is approx 1.36
            n1 = len(ref)
            n2 = len(inf)
            critical_val = 1.36 * np.sqrt((n1 + n2) / (n1 * n2))
            p_value = 0.01 if statistic > critical_val else 0.50

        drift_detected = p_value < alpha
        status = "DRIFT_ALERT_CRITICAL" if drift_detected else "STABLE"

        if drift_detected:
            log.warning(f"⚠️ FEATURE DRIFT DETECTED for '{name}' [v{meta.version}]! KS p-val={p_value:.5f} < {alpha}")

        return {
            "status": status,
            "feature_name": name,
            "version": meta.version,
            "ks_statistic": round(float(statistic), 4),
            "p_value": round(float(p_value), 5),
            "drift_detected": drift_detected,
            "inference_samples": len(inf)
        }

    def get_lineage_report(self, name: str) -> dict[str, Any]:
        """Generate a complete mathematical audit report of a feature's performance and drift status."""
        if name not in self._registry:
            return {}

        meta = self._registry[name]
        recent_ic = meta.spearman_ic_history[-1] if meta.spearman_ic_history else 0.0
        avg_ic = float(np.mean(meta.spearman_ic_history)) if meta.spearman_ic_history else 0.0
        avg_hl = float(np.mean(meta.decay_half_life_history)) if meta.decay_half_life_history else 0.0

        drift = self.check_drift(name)

        return {
            "name": name,
            "version": meta.version,
            "formula": meta.formula,
            "parameters": meta.parameters,
            "average_spearman_ic": round(avg_ic, 4),
            "recent_spearman_ic": round(recent_ic, 4),
            "average_decay_half_life_mins": round(avg_hl, 1),
            "drift_status": drift
        }

# Singleton instance
_registry = FeatureLineageRegistry()

def get_registry() -> FeatureLineageRegistry:
    return _registry

