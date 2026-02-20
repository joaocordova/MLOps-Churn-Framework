"""
SkyFit Churn Prediction — Training Entrypoint (V1)
====================================================
Orchestrates the full training pipeline:
  1. Load data from ml.training_samples
  2. Run walk-forward temporal validation (2024-H2 through 2025-H1)
  3. Train final model on [2024-03, 2025-07)
  4. Evaluate on holdout test set [2025-07, 2026-01)
  5. Save model artifacts with version metadata

V1 Strategy (D19):
  - Training data spans 2024-2025 for broad temporal coverage
  - Walk-forward validates across seasons (captures New Year effect)
  - 6-month holdout test provides robust out-of-time evaluation
  - This is a baseline release; results will inform V2 tuning

Usage:
  python -m src.training.train --db-url postgresql://... --output-dir models/v1
"""

import argparse
import logging
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
import json

from config.features import FEATURE_CONFIG
from config.model import MODEL_CONFIG
from src.training.data_loader import load_training_data, split_features_target
from src.training.stacking_ensemble import StackingEnsemble
from src.training.walk_forward import (
    generate_walk_forward_folds,
    generate_holdout_test,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_walk_forward_validation(
    df: pd.DataFrame,
) -> dict:
    """
    Run walk-forward cross-validation to estimate model performance.

    V1 (D19): Folds span 2024-H2 through 2025-H1, giving 10 monthly
    folds that capture seasonality, New Year resolution cohort, etc.

    Returns aggregated metrics across folds.
    """
    folds = generate_walk_forward_folds(df)

    if not folds:
        logger.warning("No valid folds generated. Check date ranges and data.")
        return {}

    all_metrics = []

    for fold_num, (train_df, val_df) in enumerate(folds, 1):
        logger.info("=" * 60)
        logger.info("FOLD %d / %d", fold_num, len(folds))
        logger.info("=" * 60)

        X_train, y_train = split_features_target(train_df)
        X_val, y_val = split_features_target(val_df)

        ensemble = StackingEnsemble()
        metrics = ensemble.fit(X_train, y_train, X_val, y_val)
        all_metrics.append(metrics)

    # Aggregate across folds
    agg_metrics = {}
    for key in all_metrics[0]:
        values = [m[key] for m in all_metrics]
        agg_metrics[f"{key}_mean"] = np.mean(values)
        agg_metrics[f"{key}_std"] = np.std(values)
        agg_metrics[f"{key}_min"] = np.min(values)
        agg_metrics[f"{key}_max"] = np.max(values)

    logger.info("=" * 60)
    logger.info("WALK-FORWARD AGGREGATED RESULTS (%d folds)", len(folds))
    logger.info("=" * 60)
    for key, value in agg_metrics.items():
        logger.info("  %s: %.4f", key, value)

    return agg_metrics


def train_final_model(
    df: pd.DataFrame,
    output_dir: Path,
) -> StackingEnsemble:
    """
    Train the final production model and evaluate on holdout test set.

    V1 (D19):
      Train: [2024-03, 2025-07) — ~16 months of data
      Test:  [2025-07, 2026-01) — 6 months out-of-time holdout
    """
    train_df, test_df = generate_holdout_test(df)

    if len(test_df) == 0:
        logger.error("Empty test set. Check date ranges.")
        sys.exit(1)

    X_train, y_train = split_features_target(train_df)
    X_test, y_test = split_features_target(test_df)

    logger.info("=" * 60)
    logger.info("FINAL MODEL TRAINING (V1)")
    logger.info("  Train: %d samples (%.1f%% positive)",
                len(X_train), y_train.mean() * 100)
    logger.info("  Test:  %d samples (%.1f%% positive)",
                len(X_test), y_test.mean() * 100)
    logger.info("=" * 60)

    ensemble = StackingEnsemble()
    test_metrics = ensemble.fit(X_train, y_train, X_test, y_test)

    # Save model
    model_version = datetime.now().strftime("v%Y%m%d_%H%M%S")
    model_path = output_dir / model_version
    ensemble.save(model_path)

    # Save comprehensive metrics
    metrics_path = model_path / "metrics.json"
    with open(metrics_path, "w") as f:
        json.dump({
            "model_version": model_version,
            "trained_at": datetime.now().isoformat(),
            "release": "V1",
            "test_metrics": test_metrics,
            "train_samples": len(train_df),
            "test_samples": len(test_df),
            "train_positive_rate": float(y_train.mean()),
            "test_positive_rate": float(y_test.mean()),
            "train_date_range": {
                "start": MODEL_CONFIG.TRAIN_START_DATE,
                "end": MODEL_CONFIG.TEST_START_DATE,
            },
            "test_date_range": {
                "start": MODEL_CONFIG.TEST_START_DATE,
                "end": MODEL_CONFIG.TEST_END_DATE,
            },
            "feature_count": len(FEATURE_CONFIG.ALL_FEATURES),
            "features": FEATURE_CONFIG.ALL_FEATURES,
            "business_rules": {
                "D17": "Monthly contracts auto-renew every 30 days",
                "D18": "Non-payment blocks turnstile, is_defaulter feature added",
                "D19": "V1 evaluation uses 2024-2025 as test period",
            },
        }, f, indent=2, default=str)

    logger.info("Final model saved: %s", model_path)
    logger.info("Metrics saved: %s", metrics_path)

    # Check targets
    logger.info("=" * 60)
    logger.info("TARGET CHECKS (V1 Baseline)")
    logger.info("=" * 60)
    checks = {
        "PR-AUC": (test_metrics["pr_auc"], MODEL_CONFIG.TARGET_PR_AUC, ">="),
        "ROC-AUC": (test_metrics["roc_auc"], MODEL_CONFIG.TARGET_ROC_AUC, ">="),
        "Brier Score": (test_metrics["brier_score"], MODEL_CONFIG.TARGET_BRIER_SCORE, "<="),
        "Precision@20%": (test_metrics["precision_at_20pct"], MODEL_CONFIG.TARGET_PRECISION_AT_20, ">="),
    }

    all_passed = True
    for name, (actual, target, op) in checks.items():
        passed = actual >= target if op == ">=" else actual <= target
        status = "PASS" if passed else "FAIL"
        logger.info("  %s: %.4f %s %.4f — %s", name, actual, op, target, status)
        if not passed:
            all_passed = False

    if all_passed:
        logger.info("All targets passed. V1 model ready for deployment.")
    else:
        logger.warning(
            "Some targets failed. This is V1 baseline — review results "
            "before deploying. Consider hyperparameter tuning for V2."
        )

    return ensemble


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SkyFit Churn Prediction — Training Pipeline (V1)"
    )
    parser.add_argument(
        "--db-url",
        default=None,
        help="PostgreSQL connection string. If not provided, loads from "
             "C:\\skyfit-datalake\\config\\.env automatically.",
    )
    parser.add_argument(
        "--env-file",
        default=None,
        help="Path to .env file with database credentials.",
    )
    parser.add_argument(
        "--output-dir",
        default="models",
        help="Directory to save model artifacts",
    )
    parser.add_argument(
        "--skip-cv",
        action="store_true",
        help="Skip walk-forward validation (faster, for testing)",
    )
    args = parser.parse_args()

    # Resolve database connection
    if args.db_url:
        db_url = args.db_url
    else:
        from config.database import get_connection_string
        env_path = Path(args.env_file) if args.env_file else None
        db_url = get_connection_string(env_path)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load data
    logger.info("=" * 70)
    logger.info("SKYFIT CHURN PREDICTION — V1 TRAINING PIPELINE")
    logger.info("=" * 70)
    logger.info("Step 1: Loading training data [%s, %s)...",
                MODEL_CONFIG.TRAIN_START_DATE, MODEL_CONFIG.DATA_CUTOFF_DATE)
    df = load_training_data(db_url)

    # 2. Walk-forward validation
    if not args.skip_cv:
        logger.info("Step 2: Walk-forward temporal validation...")
        cv_metrics = run_walk_forward_validation(df)
    else:
        logger.info("Step 2: SKIPPED (--skip-cv)")

    # 3. Train final model
    logger.info("Step 3: Training final V1 model...")
    ensemble = train_final_model(df, output_dir)

    logger.info("=" * 70)
    logger.info("V1 TRAINING PIPELINE COMPLETE")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
