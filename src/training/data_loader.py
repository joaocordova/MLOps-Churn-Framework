"""
SkyFit Churn Prediction — Data Loader
======================================
Loads training samples from PostgreSQL with type enforcement.
Handles the 37% NULL check-in issue (ML_VAL_05) by adding
has_ever_checked_in feature.

Business rules applied here:
  D17: Monthly contracts auto-renew. is_defaulter derived from
       has_open_receivable + days_since_last_payment > 30.
  D18: Default → blocked turnstile. is_defaulter distinguishes
       forced absence from behavioral absence.

Anti-leakage: This module only reads from ml.training_samples,
which was built with point-in-time correctness in SQL.
"""

import logging
from typing import Tuple, Optional

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

from config.features import FEATURE_CONFIG
from config.model import MODEL_CONFIG

logger = logging.getLogger(__name__)


def load_training_data(
    connection_string: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load training samples from ml.training_samples.

    Parameters
    ----------
    connection_string : str
        PostgreSQL connection string.
    start_date : str, optional
        Filter reference_date >= start_date. Defaults to MODEL_CONFIG.TRAIN_START_DATE.
    end_date : str, optional
        Filter reference_date < end_date. Defaults to MODEL_CONFIG.DATA_CUTOFF_DATE.

    Returns
    -------
    pd.DataFrame
        Training samples with all features and metadata.
    """
    start_date = start_date or MODEL_CONFIG.TRAIN_START_DATE
    end_date = end_date or MODEL_CONFIG.DATA_CUTOFF_DATE

    engine = create_engine(connection_string)

    query = text("""
        SELECT *
        FROM ml.training_samples
        WHERE reference_date >= :start_date
          AND reference_date < :end_date
          AND label_type IN ('CHURN', 'ACTIVE')
        ORDER BY reference_date, member_id
    """)

    logger.info(
        "Loading training data: %s to %s",
        start_date,
        end_date,
    )

    df = pd.read_sql(query, engine, params={
        "start_date": start_date,
        "end_date": end_date,
    })

    logger.info(
        "Loaded %d samples (%d positive, %d negative)",
        len(df),
        df["churned_in_30d"].sum(),
        (~df["churned_in_30d"]).sum(),
    )

    df = _enforce_types(df)
    df = _add_derived_features(df)
    df = _validate_no_leakage(df)

    return df


def _enforce_types(df: pd.DataFrame) -> pd.DataFrame:
    """Enforce correct dtypes for all columns."""
    # Booleans → float (XGBoost needs numeric, float preserves NaN)
    for col in FEATURE_CONFIG.BOOLEAN_FEATURES:
        if col in df.columns:
            df[col] = df[col].astype(float)

    # Categorical → category
    for col in FEATURE_CONFIG.CATEGORICAL_FEATURES:
        if col in df.columns:
            df[col] = df[col].astype("category")

    # Target → int
    if "churned_in_30d" in df.columns:
        df["churned_in_30d"] = df["churned_in_30d"].astype(int)

    # Dates
    if "reference_date" in df.columns:
        df["reference_date"] = pd.to_datetime(df["reference_date"])

    return df


def _add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add features derived from existing columns.
    These are computed in Python because they're simpler here than in SQL.

    Derived features:
      - has_ever_checked_in: TRUE if days_since_last_checkin is not NULL (ML_VAL_05)
      - is_defaulter: TRUE if has_open_receivable AND days_since_last_payment > 30 (D18)
      - gender encoding: M=0, F=1, Unknown=0.5
    """
    # has_ever_checked_in: TRUE if days_since_last_checkin is not NULL
    # This addresses ML_VAL_05: 37% of REGULAR members never checked in
    df["has_ever_checked_in"] = (~df["days_since_last_checkin"].isna()).astype(float)

    logger.info(
        "has_ever_checked_in distribution: %.1f%% True, %.1f%% False",
        df["has_ever_checked_in"].mean() * 100,
        (1 - df["has_ever_checked_in"].mean()) * 100,
    )

    # is_defaulter (D18): member blocked at turnstile due to non-payment
    # Logic: has_open_receivable=True AND last payment was > 30 days ago
    # This means the monthly renewal failed → member is in default
    if "has_open_receivable" in df.columns and "days_since_last_payment" in df.columns:
        df["is_defaulter"] = (
            (df["has_open_receivable"] == 1.0)
            & (df["days_since_last_payment"] > MODEL_CONFIG.CONTRACT_RENEWAL_CYCLE_DAYS)
        ).astype(float)

        n_defaulters = (df["is_defaulter"] == 1.0).sum()
        logger.info(
            "is_defaulter: %d members (%.1f%%) identified as defaulters",
            n_defaulters,
            n_defaulters / max(len(df), 1) * 100,
        )
    else:
        df["is_defaulter"] = 0.0
        logger.warning(
            "Cannot compute is_defaulter: missing has_open_receivable "
            "or days_since_last_payment columns"
        )

    # Encode gender as numeric (XGBoost handles categories but LogReg doesn't)
    if "gender" in df.columns:
        df["gender"] = df["gender"].map({"M": 0, "F": 1}).fillna(0.5)

    return df


def _validate_no_leakage(df: pd.DataFrame) -> pd.DataFrame:
    """
    Runtime leakage checks. Raises ValueError if leakage detected.
    """
    # Check 1: Positive samples at spell_end should have low recent activity
    positives_at_end = df[
        (df["churned_in_30d"] == 1) & (df["prediction_horizon"] == "at_spell_end")
    ]

    if len(positives_at_end) > 0:
        avg_checkins_7d = positives_at_end["checkins_last_7d"].mean()
        avg_days_inactive = positives_at_end["days_since_last_checkin"].mean()

        logger.info(
            "Leakage check — churned at spell_end: avg checkins_7d=%.2f, "
            "avg days_inactive=%.1f",
            avg_checkins_7d,
            avg_days_inactive,
        )

        # If churned members at spell_end have high recent activity,
        # something is wrong with the point-in-time logic
        if avg_checkins_7d > 3.0:
            logger.warning(
                "POTENTIAL LEAKAGE: churned members at spell_end have avg "
                "%.1f checkins in last 7 days. Expected < 1.",
                avg_checkins_7d,
            )

    # Check 2: No future dates
    cutoff = pd.Timestamp(MODEL_CONFIG.DATA_CUTOFF_DATE)
    future_refs = df[df["reference_date"] > cutoff]
    if len(future_refs) > 0:
        raise ValueError(
            f"LEAKAGE DETECTED: {len(future_refs)} samples have "
            f"reference_date after data cutoff {MODEL_CONFIG.DATA_CUTOFF_DATE}"
        )

    # Check 3 (D18): Validate defaulters have expected patterns
    defaulters = df[df.get("is_defaulter", pd.Series(dtype=float)) == 1.0]
    if len(defaulters) > 0:
        avg_default_checkins = defaulters["checkins_last_7d"].mean()
        logger.info(
            "Defaulter check: %d defaulters, avg checkins_7d=%.2f "
            "(expected ~0 since turnstile blocked)",
            len(defaulters),
            avg_default_checkins,
        )

    return df


def split_features_target(
    df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Split DataFrame into features (X) and target (y).

    Returns
    -------
    X : pd.DataFrame
        Feature columns only.
    y : pd.Series
        Target column (churned_in_30d).
    """
    feature_cols = FEATURE_CONFIG.ALL_FEATURES
    missing_cols = [c for c in feature_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing feature columns: {missing_cols}")

    X = df[feature_cols].copy()
    y = df[FEATURE_CONFIG.TARGET].copy()

    return X, y
