"""
SkyFit Churn Prediction — Walk-Forward Temporal Validation
===========================================================
Never use random cross-validation for time-series churn data.

V1 Walk-forward protocol (D19: 2024-2025 test period):
  FOLD 1: Train [2024-03..2024-08] -> Validate [2024-09]
  FOLD 2: Train [2024-03..2024-09] -> Validate [2024-10]
  FOLD 3: Train [2024-03..2024-10] -> Validate [2024-11]
  FOLD 4: Train [2024-03..2024-11] -> Validate [2024-12]
  FOLD 5: Train [2024-03..2024-12] -> Validate [2025-01]
  FOLD 6: Train [2024-03..2025-01] -> Validate [2025-02]
  FOLD 7: Train [2024-03..2025-02] -> Validate [2025-03]
  FOLD 8: Train [2024-03..2025-03] -> Validate [2025-04]
  FOLD 9: Train [2024-03..2025-04] -> Validate [2025-05]
  FOLD 10: Train [2024-03..2025-05] -> Validate [2025-06]
  Final:  Train [2024-03..2025-06] -> Test [2025-07..2025-12]

This gives broad temporal coverage across seasons and New Year effects.
Window size: 1 month (validated by ML_VAL_01: 500+ REGULAR churns/month)
"""

import logging
from typing import List, Tuple, Dict
from dataclasses import dataclass

import pandas as pd
import numpy as np

from config.model import MODEL_CONFIG

logger = logging.getLogger(__name__)


@dataclass
class FoldResult:
    """Results from a single walk-forward fold."""
    fold_num: int
    train_start: str
    train_end: str
    val_start: str
    val_end: str
    n_train: int
    n_val: int
    n_train_pos: int
    n_val_pos: int
    metrics: Dict[str, float]


def generate_walk_forward_folds(
    df: pd.DataFrame,
    train_start: str = None,
    val_start_month: str = None,
    val_end_month: str = None,
    window_months: int = None,
) -> List[Tuple[pd.DataFrame, pd.DataFrame]]:
    """
    Generate walk-forward temporal folds.

    Each fold:
    - Train: [train_start, val_start)  — expanding window
    - Validate: [val_start, val_start + window_months)
    - Next fold: val_start advances by window_months

    Parameters
    ----------
    df : pd.DataFrame
        Full training dataset with 'reference_date' column.
    train_start : str
        Start of training window (fixed for all folds).
        Defaults to MODEL_CONFIG.TRAIN_START_DATE.
    val_start_month : str
        First validation window start.
        Defaults to 6 months after train_start.
    val_end_month : str
        Last validation window start.
        Defaults to 1 month before TEST_START_DATE.
    window_months : int
        Validation window size in months.
        Defaults to MODEL_CONFIG.VALIDATION_WINDOW_MONTHS.

    Returns
    -------
    List of (train_df, val_df) tuples.
    """
    # Apply defaults from config
    train_start = train_start or MODEL_CONFIG.TRAIN_START_DATE
    window_months = window_months or MODEL_CONFIG.VALIDATION_WINDOW_MONTHS

    # Default: start validation 6 months after training starts
    if val_start_month is None:
        val_start_month = str(
            (pd.Timestamp(train_start) + pd.DateOffset(months=6)).date()
        )

    # Default: last validation ends 1 month before test period
    if val_end_month is None:
        val_end_month = str(
            (pd.Timestamp(MODEL_CONFIG.TEST_START_DATE) - pd.DateOffset(months=1)).date()
        )

    folds = []
    train_start_dt = pd.Timestamp(train_start)
    current_val = pd.Timestamp(val_start_month)
    last_val = pd.Timestamp(val_end_month)

    fold_num = 1
    while current_val <= last_val:
        val_end = current_val + pd.DateOffset(months=window_months)

        train_mask = (
            (df["reference_date"] >= train_start_dt)
            & (df["reference_date"] < current_val)
        )
        val_mask = (
            (df["reference_date"] >= current_val)
            & (df["reference_date"] < val_end)
        )

        train_df = df[train_mask].copy()
        val_df = df[val_mask].copy()

        n_train_pos = train_df["churned_in_30d"].sum()
        n_val_pos = val_df["churned_in_30d"].sum()

        logger.info(
            "Fold %d: Train [%s, %s) (%d samples, %d pos) -> "
            "Val [%s, %s) (%d samples, %d pos)",
            fold_num,
            train_start_dt.strftime("%Y-%m-%d"),
            current_val.strftime("%Y-%m-%d"),
            len(train_df),
            n_train_pos,
            current_val.strftime("%Y-%m-%d"),
            val_end.strftime("%Y-%m-%d"),
            len(val_df),
            n_val_pos,
        )

        if n_val_pos < 50:
            logger.warning(
                "Fold %d has only %d positive samples in validation. "
                "Metrics may be unreliable.",
                fold_num,
                n_val_pos,
            )

        if len(train_df) > 0 and len(val_df) > 0:
            folds.append((train_df, val_df))
        else:
            logger.warning(
                "Fold %d skipped: train=%d, val=%d samples",
                fold_num, len(train_df), len(val_df),
            )

        current_val = val_end
        fold_num += 1

    logger.info("Generated %d walk-forward folds.", len(folds))
    return folds


def generate_holdout_test(
    df: pd.DataFrame,
    train_end: str = None,
    test_start: str = None,
    test_end: str = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generate final train/test split.

    Train: everything from TRAIN_START_DATE before test_start.
    Test: [test_start, test_end) — held out, never seen during CV.

    V1 (D19): test covers 2025-H2 (Jul-Dec 2025), giving 6 months
    of out-of-time evaluation across multiple seasons.

    Returns
    -------
    (train_df, test_df)
    """
    train_end = train_end or MODEL_CONFIG.TEST_START_DATE
    test_start = test_start or MODEL_CONFIG.TEST_START_DATE
    test_end = test_end or MODEL_CONFIG.TEST_END_DATE

    train_mask = (
        (df["reference_date"] >= pd.Timestamp(MODEL_CONFIG.TRAIN_START_DATE))
        & (df["reference_date"] < pd.Timestamp(train_end))
    )
    test_mask = (
        (df["reference_date"] >= pd.Timestamp(test_start))
        & (df["reference_date"] < pd.Timestamp(test_end))
    )

    train_df = df[train_mask].copy()
    test_df = df[test_mask].copy()

    logger.info(
        "Holdout split: Train %d samples (%d pos, %.1f%%) [%s, %s), "
        "Test %d samples (%d pos, %.1f%%) [%s, %s)",
        len(train_df),
        train_df["churned_in_30d"].sum(),
        train_df["churned_in_30d"].mean() * 100,
        MODEL_CONFIG.TRAIN_START_DATE,
        train_end,
        len(test_df),
        test_df["churned_in_30d"].sum(),
        test_df["churned_in_30d"].mean() * 100 if len(test_df) > 0 else 0,
        test_start,
        test_end,
    )

    return train_df, test_df
