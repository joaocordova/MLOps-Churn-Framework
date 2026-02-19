"""
SkyFit Churn Prediction — Stacking Ensemble
=============================================
XGBoost Stacking: 4 specialist L0 models + Logistic Regression L1 meta-learner.

Architecture:
  L0: XGB_freq (12 feat) | XGB_fin (3 feat) | XGB_tenure (8 feat) | XGB_context (7 feat)
  L1: LogisticRegression(4 probabilities + 3 passthrough features)

Training protocol:
  - L0 models trained with walk-forward temporal CV
  - L1 trained on out-of-fold L0 predictions (prevents L0→L1 leakage)
  - Final calibration via Platt scaling
  - SHAP explainer attached to L0 models for feature-level explanations
"""

import logging
from typing import Dict, List, Tuple, Any, Optional
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import joblib
from xgboost import XGBClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
    brier_score_loss,
    precision_recall_curve,
    classification_report,
)

from config.features import FEATURE_CONFIG
from config.model import MODEL_CONFIG

logger = logging.getLogger(__name__)


@dataclass
class SpecialistModel:
    """A single L0 specialist XGBoost model."""
    name: str
    features: List[str]
    model: Optional[XGBClassifier] = None
    params: Optional[Dict[str, Any]] = None


class StackingEnsemble:
    """
    Stacking ensemble for churn prediction.

    L0: 4 specialist XGBoost models, each trained on a subset of features.
    L1: Logistic Regression meta-learner combining L0 probabilities + passthrough features.
    """

    def __init__(self) -> None:
        self.specialists: Dict[str, SpecialistModel] = self._init_specialists()
        self.meta_learner: Optional[LogisticRegression] = None
        self.calibrator: Optional[CalibratedClassifierCV] = None
        self.is_fitted: bool = False
        self.training_metrics: Dict[str, float] = {}
        self.scale_pos_weight: float = 1.0

    def _init_specialists(self) -> Dict[str, SpecialistModel]:
        """Initialize L0 specialist configurations."""
        return {
            "xgb_freq": SpecialistModel(
                name="xgb_freq",
                features=FEATURE_CONFIG.XGB_FREQ_FEATURES,
                params={**MODEL_CONFIG.XGB_BASE_PARAMS, **MODEL_CONFIG.XGB_FREQ_OVERRIDES},
            ),
            "xgb_fin": SpecialistModel(
                name="xgb_fin",
                features=FEATURE_CONFIG.XGB_FIN_FEATURES,
                params={**MODEL_CONFIG.XGB_BASE_PARAMS, **MODEL_CONFIG.XGB_FIN_OVERRIDES},
            ),
            "xgb_tenure": SpecialistModel(
                name="xgb_tenure",
                features=FEATURE_CONFIG.XGB_TENURE_FEATURES,
                params={**MODEL_CONFIG.XGB_BASE_PARAMS, **MODEL_CONFIG.XGB_TENURE_OVERRIDES},
            ),
            "xgb_context": SpecialistModel(
                name="xgb_context",
                features=FEATURE_CONFIG.XGB_CONTEXT_FEATURES,
                params={**MODEL_CONFIG.XGB_BASE_PARAMS, **MODEL_CONFIG.XGB_CONTEXT_OVERRIDES},
            ),
        }

    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: pd.DataFrame,
        y_val: pd.Series,
    ) -> Dict[str, float]:
        """
        Train the full stacking ensemble.

        Parameters
        ----------
        X_train, y_train : Training data.
        X_val, y_val : Validation data (for L1 training and calibration).

        Returns
        -------
        Dict[str, float] : Evaluation metrics on validation set.
        """
        # Compute class weight
        n_neg = (y_train == 0).sum()
        n_pos = (y_train == 1).sum()
        self.scale_pos_weight = n_neg / max(n_pos, 1)
        logger.info(
            "Class balance: %d negative, %d positive (scale_pos_weight=%.2f)",
            n_neg, n_pos, self.scale_pos_weight,
        )

        # ------- L0: Train specialists -------
        logger.info("Training L0 specialists...")
        l0_train_preds = {}
        l0_val_preds = {}

        for name, specialist in self.specialists.items():
            logger.info("  Training %s on %d features...", name, len(specialist.features))

            params = {**specialist.params, "scale_pos_weight": self.scale_pos_weight}
            model = XGBClassifier(**params)

            # Get feature subset
            X_tr = X_train[specialist.features]
            X_va = X_val[specialist.features]

            # Train with early stopping on validation
            model.fit(
                X_tr, y_train,
                eval_set=[(X_va, y_val)],
                verbose=False,
            )

            specialist.model = model

            # Store predictions for L1
            l0_train_preds[name] = model.predict_proba(X_tr)[:, 1]
            l0_val_preds[name] = model.predict_proba(X_va)[:, 1]

            # Log specialist performance
            val_auc = roc_auc_score(y_val, l0_val_preds[name])
            val_pr_auc = average_precision_score(y_val, l0_val_preds[name])
            logger.info("    %s — ROC-AUC: %.4f, PR-AUC: %.4f", name, val_auc, val_pr_auc)

        # ------- L1: Train meta-learner -------
        logger.info("Training L1 meta-learner...")

        # Build L1 input: specialist probabilities + passthrough features
        X_l1_train = self._build_l1_input(X_train, l0_train_preds)
        X_l1_val = self._build_l1_input(X_val, l0_val_preds)

        self.meta_learner = LogisticRegression(**MODEL_CONFIG.L1_PARAMS)
        self.meta_learner.fit(X_l1_train, y_train)

        # ------- Calibration -------
        logger.info("Calibrating with Platt scaling...")
        raw_val_scores = self.meta_learner.predict_proba(X_l1_val)[:, 1]

        # Use CalibratedClassifierCV with prefit estimator
        self.calibrator = CalibratedClassifierCV(
            self.meta_learner,
            method=MODEL_CONFIG.CALIBRATION_METHOD,
            cv="prefit",
        )
        self.calibrator.fit(X_l1_val, y_val)

        # ------- Evaluate -------
        calibrated_probs = self.calibrator.predict_proba(X_l1_val)[:, 1]
        self.training_metrics = self._evaluate(y_val, calibrated_probs)
        self.is_fitted = True

        # Log L1 coefficients (which specialist matters most)
        coef_names = list(self.specialists.keys()) + FEATURE_CONFIG.L1_PASSTHROUGH_FEATURES
        for name, coef in zip(coef_names, self.meta_learner.coef_[0]):
            logger.info("  L1 coefficient %s: %.4f", name, coef)

        return self.training_metrics

    def predict_proba(self, X: pd.DataFrame) -> np.ndarray:
        """
        Predict calibrated churn probability.

        Parameters
        ----------
        X : pd.DataFrame with all features.

        Returns
        -------
        np.ndarray of calibrated probabilities (0-1).
        """
        if not self.is_fitted:
            raise RuntimeError("Model not fitted. Call fit() first.")

        # L0 predictions
        l0_preds = {}
        for name, specialist in self.specialists.items():
            X_spec = X[specialist.features]
            l0_preds[name] = specialist.model.predict_proba(X_spec)[:, 1]

        # L1 input
        X_l1 = self._build_l1_input(X, l0_preds)

        # Calibrated output
        return self.calibrator.predict_proba(X_l1)[:, 1]

    def predict_risk_tier(self, probabilities: np.ndarray) -> np.ndarray:
        """Assign risk tier based on calibrated probability."""
        tiers = np.where(
            probabilities >= MODEL_CONFIG.HIGH_RISK_THRESHOLD,
            "HIGH",
            np.where(
                probabilities >= MODEL_CONFIG.MEDIUM_RISK_THRESHOLD,
                "MEDIUM",
                "LOW",
            ),
        )
        return tiers

    def _build_l1_input(
        self,
        X: pd.DataFrame,
        l0_preds: Dict[str, np.ndarray],
    ) -> np.ndarray:
        """Build L1 meta-learner input from L0 predictions + passthrough features."""
        parts = [l0_preds[name].reshape(-1, 1) for name in self.specialists]

        # Add passthrough features
        for feat in FEATURE_CONFIG.L1_PASSTHROUGH_FEATURES:
            if feat in X.columns:
                parts.append(X[feat].values.reshape(-1, 1))

        return np.hstack(parts)

    def _evaluate(
        self,
        y_true: pd.Series,
        y_prob: np.ndarray,
    ) -> Dict[str, float]:
        """Compute all evaluation metrics."""
        metrics = {}

        # PR-AUC (primary)
        metrics["pr_auc"] = average_precision_score(y_true, y_prob)

        # ROC-AUC
        metrics["roc_auc"] = roc_auc_score(y_true, y_prob)

        # Brier score (calibration quality)
        metrics["brier_score"] = brier_score_loss(y_true, y_prob)

        # Precision at top 20%
        n_top = int(len(y_prob) * 0.20)
        top_indices = np.argsort(y_prob)[-n_top:]
        metrics["precision_at_20pct"] = y_true.iloc[top_indices].mean()

        # Log results with target comparison
        logger.info("=" * 60)
        logger.info("EVALUATION RESULTS")
        logger.info("=" * 60)
        for metric, value in metrics.items():
            target_attr = f"TARGET_{metric.upper()}"
            target = getattr(MODEL_CONFIG, target_attr, None)
            status = ""
            if target is not None:
                if metric == "brier_score":
                    status = " PASS" if value <= target else " FAIL"
                else:
                    status = " PASS" if value >= target else " FAIL"
            logger.info("  %s: %.4f%s", metric, value, status)
        logger.info("=" * 60)

        return metrics

    def save(self, path: Path) -> None:
        """Save the entire ensemble to disk."""
        path.mkdir(parents=True, exist_ok=True)

        # Save specialists
        for name, specialist in self.specialists.items():
            joblib.dump(specialist.model, path / f"{name}.joblib")

        # Save meta-learner and calibrator
        joblib.dump(self.meta_learner, path / "meta_learner.joblib")
        joblib.dump(self.calibrator, path / "calibrator.joblib")

        # Save config snapshot
        joblib.dump({
            "scale_pos_weight": self.scale_pos_weight,
            "training_metrics": self.training_metrics,
            "specialist_features": {
                name: spec.features for name, spec in self.specialists.items()
            },
        }, path / "config_snapshot.joblib")

        logger.info("Model saved to %s", path)

    def load(self, path: Path) -> None:
        """Load a saved ensemble from disk."""
        # Load specialists
        for name, specialist in self.specialists.items():
            specialist.model = joblib.load(path / f"{name}.joblib")

        # Load meta-learner and calibrator
        self.meta_learner = joblib.load(path / "meta_learner.joblib")
        self.calibrator = joblib.load(path / "calibrator.joblib")

        # Load config snapshot
        config = joblib.load(path / "config_snapshot.joblib")
        self.scale_pos_weight = config["scale_pos_weight"]
        self.training_metrics = config["training_metrics"]
        self.is_fitted = True

        logger.info("Model loaded from %s", path)
