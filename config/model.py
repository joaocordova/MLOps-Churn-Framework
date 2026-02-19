"""
SkyFit Churn Prediction — Model Configuration
==============================================
Hyperparameters, thresholds, and pipeline settings.

Business rules (D17-D19):
  D17: Contracts are monthly/recurring, auto-renew every ~30 days.
       days_until_contract_end is always <= 30 for active members.
       "Financial churn" = non-payment/default, NOT contract expiration.
  D18: Non-payment → automatic default → blocked at turnstile.
       Consequence: days_since_last_checkin for defaulters reflects
       BLOCKED ACCESS, not disengagement. The model must distinguish
       behavioral absence (choosing not to attend) from forced absence
       (cannot pass turnstile due to default).
  D19: V1 evaluation uses 2024-2025 data as test period for broader
       temporal coverage. Walk-forward trains on early data, tests on later.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List


@dataclass(frozen=True)
class ModelConfig:
    """Immutable model configuration."""

    # ------------------------------------------------------------------
    # RISK TIER THRESHOLDS
    # ------------------------------------------------------------------
    HIGH_RISK_THRESHOLD: float = 0.70
    MEDIUM_RISK_THRESHOLD: float = 0.40
    # LOW = anything below MEDIUM_RISK_THRESHOLD

    # ------------------------------------------------------------------
    # BEHAVIORAL CHURN (D9, D17, D18)
    # ------------------------------------------------------------------
    BEHAVIORAL_CHURN_DAYS: int = 10  # Decision D9
    # IMPORTANT (D18): If has_open_receivable=True AND days_since_last_checkin
    # is high, the absence is likely FORCED (default/blocked turnstile),
    # not behavioral. The churn_type classifier handles this distinction.

    # ------------------------------------------------------------------
    # CONTRACT RENEWAL (D17)
    # ------------------------------------------------------------------
    # Monthly contracts auto-renew. A new contract starts every ~30 days.
    # days_until_contract_end is always <= 30 for paying active members.
    # "Financial churn" signal = open receivable / non-payment,
    # NOT contract_expiring_30d (which is always True for monthly plans).
    CONTRACT_RENEWAL_CYCLE_DAYS: int = 30

    # ------------------------------------------------------------------
    # WALK-FORWARD VALIDATION (D19)
    # ------------------------------------------------------------------
    MIN_CHURN_EVENTS_PER_FOLD: int = 200
    VALIDATION_WINDOW_MONTHS: int = 1  # From ML_VAL_01: 500+ churns/month

    # V1: Extended training and test window covering 2024-2025 (D19)
    # Data available from start of records through 2026-02-10
    TRAIN_START_DATE: str = "2024-03-01"
    # Walk-forward validation uses expanding window through 2025
    # Final holdout test: 2025-H2 data
    TEST_START_DATE: str = "2025-07-01"
    TEST_END_DATE: str = "2026-01-01"
    # Data cutoff
    DATA_CUTOFF_DATE: str = "2026-02-10"

    # ------------------------------------------------------------------
    # XGBoost L0 SPECIALIST HYPERPARAMETERS
    # ------------------------------------------------------------------
    # Conservative defaults — will be tuned via Optuna after baseline
    XGB_BASE_PARAMS: Dict[str, Any] = field(default_factory=lambda: {
        "objective": "binary:logistic",
        "eval_metric": "aucpr",  # PR-AUC for imbalanced data
        "tree_method": "hist",   # Fast histogram-based
        "max_depth": 6,
        "learning_rate": 0.05,
        "n_estimators": 500,
        "min_child_weight": 10,  # Conservative for noisy gym data
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "gamma": 1.0,
        "reg_alpha": 0.1,
        "reg_lambda": 1.0,
        "random_state": 42,
        "n_jobs": -1,
        # scale_pos_weight computed dynamically at training time
    })

    # Specialist-specific overrides
    XGB_FREQ_OVERRIDES: Dict[str, Any] = field(default_factory=lambda: {
        "max_depth": 7,       # More complex interactions in frequency data
        "n_estimators": 600,
    })

    XGB_FIN_OVERRIDES: Dict[str, Any] = field(default_factory=lambda: {
        "max_depth": 4,       # Only 3 features — keep simple
        "n_estimators": 300,
    })

    XGB_TENURE_OVERRIDES: Dict[str, Any] = field(default_factory=lambda: {
        "max_depth": 6,
        "n_estimators": 500,
    })

    XGB_CONTEXT_OVERRIDES: Dict[str, Any] = field(default_factory=lambda: {
        "max_depth": 5,
        "n_estimators": 400,
    })

    # ------------------------------------------------------------------
    # L1 META-LEARNER
    # ------------------------------------------------------------------
    L1_PARAMS: Dict[str, Any] = field(default_factory=lambda: {
        "penalty": "l2",
        "C": 1.0,
        "max_iter": 1000,
        "random_state": 42,
    })

    # ------------------------------------------------------------------
    # CALIBRATION
    # ------------------------------------------------------------------
    CALIBRATION_METHOD: str = "sigmoid"  # Platt scaling
    CALIBRATION_CV: int = 5

    # ------------------------------------------------------------------
    # EVALUATION TARGETS
    # ------------------------------------------------------------------
    TARGET_PR_AUC: float = 0.45
    TARGET_PRECISION_AT_20: float = 0.50
    TARGET_BRIER_SCORE: float = 0.10
    TARGET_ROC_AUC: float = 0.80

    # ------------------------------------------------------------------
    # MONITORING THRESHOLDS
    # ------------------------------------------------------------------
    PSI_DRIFT_THRESHOLD: float = 0.20     # Feature drift alert
    HIT_RATE_MIN_THRESHOLD: float = 0.50  # Retrain if hit rate drops below 50%
    NULL_RATE_CIRCUIT_BREAKER: float = 0.05  # Halt scoring if > 5% NULL in any feature

    # ------------------------------------------------------------------
    # BRANCHES
    # ------------------------------------------------------------------
    MVP_BRANCH_IDS: List[int] = field(default_factory=lambda: [
        345, 181, 59, 233, 401, 166, 33, 6, 149
    ])

    # ------------------------------------------------------------------
    # PLAYBOOK MAPPING (D17, D18)
    # ------------------------------------------------------------------
    # Updated mapping includes DEFAULT churn type (D18)
    # DEFAULT = member blocked at turnstile due to non-payment
    PLAYBOOK_MAPPING: Dict[str, str] = field(default_factory=lambda: {
        "HIGH_BEHAVIORAL": "PB_HIGH_BEHAVIORAL",
        "HIGH_FINANCIAL": "PB_HIGH_FINANCIAL",
        "HIGH_DEFAULT": "PB_HIGH_FINANCIAL",     # Default → same as financial
        "HIGH_FULL": "PB_HIGH_FULL",
        "MEDIUM_BEHAVIORAL": "PB_MEDIUM_BEHAVIORAL",
        "MEDIUM_FINANCIAL": "PB_MEDIUM_FINANCIAL",
        "MEDIUM_DEFAULT": "PB_MEDIUM_FINANCIAL",  # Default → same as financial
        "LOW_NONE": "PB_LOW_ACTIVE",
    })


# Singleton instance
MODEL_CONFIG = ModelConfig()
