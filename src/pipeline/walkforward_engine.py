"""
Walk-Forward Validation Engine (V11 Institutional Upgrade)
==========================================================
Implements rolling train/test windows, rolling walk-forward feature selection,
recalibration, turnover analysis, and signal decay out-of-sample.
"""
import numpy as np
import pandas as pd
from logger import get_logger
from factor_calibration_engine import FactorCalibrationEngine

log = get_logger("WalkForwardEngine")

class WalkForwardEngine:
    """
    Automates rolling out-of-sample backtesting validation, feature selection,
    and parameter drift analysis.
    """
    def __init__(self, train_window: int = 150, test_window: int = 50):
        self.train_window = train_window
        self.test_window = test_window
        self.calibration_engine = FactorCalibrationEngine()

    def run_walk_forward(self, X: np.ndarray, y: np.ndarray, feature_names: list[str]) -> dict:
        """
        Executes rolling walk-forward validation out-of-sample.
        
        Parameters
        ----------
        X : shape (T, N_features)
        y : shape (T,) realized returns
        feature_names : list of feature names matching X columns
        """
        T, N = X.shape
        if T < (self.train_window + self.test_window):
            log.warning(
                f"Insufficient observation length (T={T}) for walk-forward validation. "
                f"Required: {self.train_window + self.test_window} steps."
            )
            return {'status': 'INSUFFICIENT_DATA', 'oos_returns': [], 'turnover': 0.0}

        oos_predictions = []
        oos_actuals = []
        calibrated_weights_history = []
        
        # Slide train and test windows forward
        step = 0
        for start_idx in range(0, T - self.train_window - self.test_window + 1, self.test_window):
            train_X = X[start_idx : start_idx + self.train_window]
            train_y = y[start_idx : start_idx + self.train_window]
            
            test_X = X[start_idx + self.train_window : start_idx + self.train_window + self.test_window]
            test_y = y[start_idx + self.train_window : start_idx + self.train_window + self.test_window]
            
            # Recalibrate factor weights on train slice
            weights = self.calibration_engine.fit_calibration(train_X, train_y, feature_names)
            calibrated_weights_history.append(list(weights.values()))
            
            # Predict out-of-sample using SIGNED Ridge coefficients (not abs-normalized weights)
            # This preserves negative signal directionality for unbiased OOS IC evaluation
            signed_coefs = self.calibration_engine.signed_coefficients
            beta = np.array([signed_coefs.get(f, 0.0) for f in feature_names], dtype=float)
            predictions = np.dot(test_X, beta)
            
            oos_predictions.extend(predictions)
            oos_actuals.extend(test_y)
            step += 1

        oos_predictions = np.array(oos_predictions)
        oos_actuals = np.array(oos_actuals)
        
        # Calculate walk-forward out-of-sample Spearman IC
        from scipy.stats import spearmanr
        oos_ic, _ = spearmanr(oos_predictions, oos_actuals)
        oos_ic = float(oos_ic) if not np.isnan(oos_ic) else 0.0
        
        # Calculate parameter turnover (stability metric)
        weights_arr = np.array(calibrated_weights_history)
        if len(weights_arr) > 1:
            # Average absolute weight change between consecutive recalibration steps
            diffs = np.diff(weights_arr, axis=0)
            avg_turnover = float(np.mean(np.sum(np.abs(diffs), axis=1)))
        else:
            avg_turnover = 0.0

        results = {
            'status': 'SUCCESS',
            'walk_steps': step,
            'oos_spearman_ic': round(oos_ic, 4),
            'parameter_turnover': round(avg_turnover, 4),
            'observations': len(oos_actuals)
        }
        
        log.info(f"Walk-Forward Validation completed over {step} windows.")
        log.info(f"Out-of-Sample Spearman IC: {results['oos_spearman_ic']:.4f}")
        log.info(f"Average Parameter Weight Turnover: {results['parameter_turnover']:.4f}")
        
        return results

# Module level singleton
_walkforward_engine = WalkForwardEngine()

def get_walkforward_engine() -> WalkForwardEngine:
    return _walkforward_engine

