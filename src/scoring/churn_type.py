"""
SkyFit Churn Prediction — Churn Type Classifier
==================================================
Classifies each prediction into churn type based on business rules.

Churn Types (updated with D17, D18):
  BEHAVIORAL: Active contract, paying, but absent 10+ consecutive days (D9)
              Member CHOOSES not to attend despite having access.
  DEFAULT:    Non-payment → blocked at turnstile (D18). Absence is FORCED,
              not a choice. Monthly contract failed to renew due to non-payment.
  FINANCIAL:  Contract approaching end + attendance dropped, but still paying.
              (Less common with monthly auto-renew D17, but covers edge cases.)
  FULL:       Both behavioral signals AND financial/default signals present.
  NONE:       Low risk, no immediate signals.

Key distinction (D18):
  If is_defaulter=True, the member is BLOCKED at the turnstile.
  Their days_since_last_checkin is a CONSEQUENCE of non-payment,
  not a behavioral choice. The model treats this differently:
  - DEFAULT → immediate financial intervention (payment recovery)
  - BEHAVIORAL → engagement intervention (re-motivation)

This is rule-based (D7) — no additional ML model needed.
Deterministic and explainable for gym managers.
"""

import logging

import numpy as np
import pandas as pd

from config.model import MODEL_CONFIG

logger = logging.getLogger(__name__)


def classify_churn_type(df: pd.DataFrame) -> pd.Series:
    """
    Classify churn type for each scored member.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain:
        - days_since_last_checkin (int or NaN)
        - contract_expiring_30d (bool/float)
        - days_until_contract_end (int)
        - risk_tier (str: HIGH/MEDIUM/LOW)
        - has_ever_checked_in (float: 0.0 or 1.0)
        - is_defaulter (float: 0.0 or 1.0) — D18

    Returns
    -------
    pd.Series of churn_type strings.
    """
    behavioral_threshold = MODEL_CONFIG.BEHAVIORAL_CHURN_DAYS

    # D18: Default signal — member blocked at turnstile due to non-payment
    # This takes priority over behavioral classification because the
    # absence is FORCED, not a choice
    is_defaulter = df.get("is_defaulter", pd.Series(0.0, index=df.index)) == 1.0

    # Behavioral signal: absent 10+ days AND NOT a defaulter
    # A defaulter's absence is forced (blocked turnstile), not behavioral
    is_behavioral = (
        (
            (df["days_since_last_checkin"] >= behavioral_threshold)
            | (df["has_ever_checked_in"] == 0.0)
        )
        & ~is_defaulter  # D18: exclude defaulters from behavioral
    )

    # Financial signal: contract ending + not paying on time
    # D17: With monthly auto-renew, contract_expiring_30d is always True.
    # The real financial signal is has_open_receivable or late payment,
    # but NOT yet in full default status.
    is_financial = (
        (df.get("has_open_receivable", pd.Series(0.0, index=df.index)) == 1.0)
        & ~is_defaulter  # Defaulters get their own category
    )

    # Only assign types for HIGH and MEDIUM risk
    at_risk = df["risk_tier"].isin(["HIGH", "MEDIUM"])

    # Priority-based classification
    churn_type = np.where(
        at_risk & is_defaulter, "DEFAULT",
        np.where(
            at_risk & is_behavioral & is_financial, "FULL",
            np.where(
                at_risk & is_behavioral, "BEHAVIORAL",
                np.where(
                    at_risk & is_financial, "FINANCIAL",
                    "NONE",
                ),
            ),
        ),
    )

    churn_type = pd.Series(churn_type, index=df.index)

    # Log distribution
    dist = churn_type.value_counts()
    logger.info("Churn type distribution:")
    for ctype, count in dist.items():
        logger.info("  %s: %d (%.1f%%)", ctype, count, count / len(df) * 100)

    return churn_type


def assign_playbook(
    risk_tier: pd.Series,
    churn_type: pd.Series,
) -> pd.Series:
    """
    Map (risk_tier, churn_type) to playbook_id.

    Uses MODEL_CONFIG.PLAYBOOK_MAPPING for the lookup.
    Falls back to PB_LOW_ACTIVE for unknown combinations.
    """
    mapping = MODEL_CONFIG.PLAYBOOK_MAPPING

    def _map_row(tier: str, ctype: str) -> str:
        key = f"{tier}_{ctype}"
        return mapping.get(key, "PB_LOW_ACTIVE")

    playbook_ids = pd.Series(
        [_map_row(t, c) for t, c in zip(risk_tier, churn_type)],
        index=risk_tier.index,
    )

    # Log distribution
    dist = playbook_ids.value_counts()
    logger.info("Playbook assignment distribution:")
    for pb_id, count in dist.items():
        logger.info("  %s: %d", pb_id, count)

    return playbook_ids
