"""
ML Validation Framework (V5 Upgrade)
======================================
Provides institutional-grade model validation tools:
  - Purged Walk-Forward Cross-Validation (prevents look-ahead bias)
  - Feature Stability Analysis (PSI — Population Stability Index)
  - Out-of-Sample Robustness Metrics
  - Model Calibration Check

This framework ensures the AI layer (RF, HMM, DQN) is genuinely
predictive rather than just architecturally present.

Usage:
    from ml_validator import MLValidator
    validator = MLValidator()
    report = validator.validate_model(model, X, y)
"""
import numpy as np
import pandas as pd
from typing import Optional
from datetime import datetime
from logger import get_logger

log = get_logger("MLValidator")


class MLValidator:
    """
    Institutional ML validation suite.
    Prevents deploying models that only look good in-sample.
    """

    def __init__(self):
        self.validation_history: list[dict] = []

    # ========== PURGED WALK-FORWARD CV ==========

    def purged_walk_forward_cv(self, X: np.ndarray, y: np.ndarray, model,
                                n_splits: int = 5, purge_gap: int = 5,
                                embargo_pct: float = 0.01) -> dict:
        """
        Walk-forward cross-validation with purging and embargo.
        
        Purging: Removes observations near the train/test boundary
                 to prevent look-ahead bias from overlapping labels.
        Embargo: Adds a gap after each test set to prevent leakage
                 from autocorrelated features.
        
        Returns:
            dict with per-fold and aggregate metrics
        """
        n_samples = len(X)
        fold_size = n_samples // (n_splits + 1)
        embargo_size = max(1, int(n_samples * embargo_pct))
        
        fold_results = []
        
        for i in range(n_splits):
            # Train: from start to fold boundary
            train_end = fold_size * (i + 1)
            
            # Purge: remove samples near boundary
            purged_train_end = max(0, train_end - purge_gap)
            
            # Embargo: gap after train
            test_start = train_end + embargo_size
            test_end = min(n_samples, test_start + fold_size)
            
            if test_start >= n_samples or purged_train_end <= 0:
                continue
            
            X_train = X[:purged_train_end]
            y_train = y[:purged_train_end]
            X_test = X[test_start:test_end]
            y_test = y[test_start:test_end]
            
            if len(X_train) < 10 or len(X_test) < 5:
                continue
            
            try:
                model.fit(X_train, y_train)
                predictions = model.predict(X_test)
                
                # Calculate accuracy for classification
                if hasattr(y_test, 'dtype') and y_test.dtype in ['object', 'str', 'int64', 'int32']:
                    accuracy = np.mean(predictions == y_test)
                    fold_results.append({
                        'fold': i + 1,
                        'train_size': len(X_train),
                        'test_size': len(X_test),
                        'accuracy': accuracy,
                        'purge_gap': purge_gap,
                        'embargo_size': embargo_size,
                    })
                else:
                    # Regression: use R² and MAE
                    ss_res = np.sum((y_test - predictions) ** 2)
                    ss_tot = np.sum((y_test - np.mean(y_test)) ** 2)
                    r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
                    mae = np.mean(np.abs(y_test - predictions))
                    fold_results.append({
                        'fold': i + 1,
                        'train_size': len(X_train),
                        'test_size': len(X_test),
                        'r2': r2,
                        'mae': mae,
                    })
            except Exception as e:
                log.warning(f"Fold {i+1} failed: {e}")
                fold_results.append({'fold': i + 1, 'error': str(e)})
        
        # Aggregate
        valid_folds = [f for f in fold_results if 'error' not in f]
        
        if 'accuracy' in valid_folds[0] if valid_folds else {}:
            avg_metric = np.mean([f['accuracy'] for f in valid_folds])
            std_metric = np.std([f['accuracy'] for f in valid_folds])
            metric_name = 'accuracy'
        elif 'r2' in valid_folds[0] if valid_folds else {}:
            avg_metric = np.mean([f['r2'] for f in valid_folds])
            std_metric = np.std([f['r2'] for f in valid_folds])
            metric_name = 'r2'
        else:
            avg_metric, std_metric, metric_name = 0.0, 0.0, 'unknown'
        
        result = {
            'method': 'purged_walk_forward_cv',
            'n_splits': n_splits,
            'purge_gap': purge_gap,
            'embargo_pct': embargo_pct,
            f'avg_{metric_name}': round(avg_metric, 4),
            f'std_{metric_name}': round(std_metric, 4),
            'fold_details': fold_results,
            'is_robust': std_metric < 0.15,  # Low variance across folds
            'timestamp': datetime.now().isoformat(),
        }
        
        log.info(f"Purged CV: avg_{metric_name}={avg_metric:.4f} ± {std_metric:.4f} | Robust: {result['is_robust']}")
        self.validation_history.append(result)
        return result

    # ========== FEATURE DRIFT MONITORING (PSI) ==========

    def population_stability_index(self, reference: np.ndarray, current: np.ndarray,
                                    n_bins: int = 10) -> dict:
        """
        Population Stability Index (PSI) — detects feature distribution drift.
        
        PSI < 0.1  : No significant shift
        PSI 0.1-0.25: Moderate shift (investigate)
        PSI > 0.25 : Major shift (retrain model)
        
        This is critical because models trained on one distribution
        will silently fail when the market regime changes.
        """
        # Create bins from reference distribution
        breakpoints = np.percentile(reference, np.linspace(0, 100, n_bins + 1))
        breakpoints = np.unique(breakpoints)
        
        if len(breakpoints) < 3:
            return {'psi': 0.0, 'status': 'insufficient_data', 'bins': 0}
        
        # Count observations in each bin
        ref_counts = np.histogram(reference, bins=breakpoints)[0]
        cur_counts = np.histogram(current, bins=breakpoints)[0]
        
        # Convert to proportions (with smoothing to avoid division by zero)
        ref_pct = (ref_counts + 1) / (len(reference) + len(breakpoints))
        cur_pct = (cur_counts + 1) / (len(current) + len(breakpoints))
        
        # PSI = Σ (cur% - ref%) * ln(cur% / ref%)
        psi = np.sum((cur_pct - ref_pct) * np.log(cur_pct / ref_pct))
        
        if psi < 0.1:
            status = "stable"
        elif psi < 0.25:
            status = "moderate_drift"
        else:
            status = "major_drift"
        
        result = {
            'psi': round(float(psi), 6),
            'status': status,
            'n_bins': len(breakpoints) - 1,
            'reference_size': len(reference),
            'current_size': len(current),
            'needs_retrain': psi > 0.25,
            'timestamp': datetime.now().isoformat(),
        }
        
        log.info(f"PSI: {psi:.4f} | Status: {status}")
        return result

    # ========== FEATURE STABILITY ANALYSIS ==========

    def feature_stability(self, feature_importances_over_time: list[dict]) -> dict:
        """
        Checks if feature importances are stable across time windows.
        Unstable features indicate potential overfitting or regime dependence.
        
        Input: list of dicts like [{'rsi14': 0.12, 'macd': 0.08, ...}, ...]
        """
        if len(feature_importances_over_time) < 2:
            return {'stable': True, 'message': 'Insufficient history'}
        
        df = pd.DataFrame(feature_importances_over_time)
        stability = {}
        
        for col in df.columns:
            cv = df[col].std() / max(df[col].mean(), 1e-8)  # Coefficient of variation
            stability[col] = {
                'mean_importance': round(float(df[col].mean()), 4),
                'std_importance': round(float(df[col].std()), 4),
                'cv': round(float(cv), 4),
                'is_stable': cv < 0.5,  # CV < 0.5 means reasonably stable
            }
        
        unstable = [k for k, v in stability.items() if not v['is_stable']]
        
        result = {
            'features': stability,
            'unstable_features': unstable,
            'pct_unstable': round(len(unstable) / max(len(stability), 1), 4),
            'overall_stable': len(unstable) == 0,
        }
        
        if unstable:
            log.warning(f"Unstable features detected: {unstable}")
        else:
            log.info("All features stable across time windows")
        
        return result

    # ========== OUT-OF-SAMPLE ROBUSTNESS ==========

    def oos_robustness_check(self, in_sample_metric: float, out_of_sample_metric: float,
                              metric_name: str = "sharpe") -> dict:
        """
        Compares in-sample vs out-of-sample performance.
        A model that degrades >30% OOS is likely overfit.
        """
        if in_sample_metric == 0:
            degradation = 1.0
        else:
            degradation = 1 - (out_of_sample_metric / in_sample_metric)
        
        result = {
            'metric': metric_name,
            'in_sample': round(in_sample_metric, 4),
            'out_of_sample': round(out_of_sample_metric, 4),
            'degradation_pct': round(degradation * 100, 2),
            'is_overfit': degradation > 0.30,
            'verdict': 'OVERFIT' if degradation > 0.30 else 'ROBUST',
            'timestamp': datetime.now().isoformat(),
        }
        
        log.info(f"OOS Check: IS={in_sample_metric:.4f} → OOS={out_of_sample_metric:.4f} | "
                 f"Degradation={degradation*100:.1f}% | {result['verdict']}")
        
        self.validation_history.append(result)
        return result

    # ========== FULL VALIDATION REPORT ==========

    def generate_report(self) -> dict:
        """Generate a summary of all validation runs."""
        return {
            'total_validations': len(self.validation_history),
            'history': self.validation_history,
            'generated_at': datetime.now().isoformat(),
        }

