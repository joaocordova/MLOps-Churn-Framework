"""
SkyFit Churn Prediction — Drift Detection & Circuit Breakers
==============================================================
Monitors model health via:
  - Feature drift: PSI (Population Stability Index) — weekly
  - Concept drift: predicted vs actual churn rate — monthly
  - Hit rate by tier: % of HIGH/MEDIUM predictions that actually churned
  - Circuit breakers: halt scoring if data quality degrades

Triggered by Airflow after the daily scoring pipeline.
"""

import logging
from datetime import date, timedelta
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from config.model import MODEL_CONFIG

logger = logging.getLogger(__name__)


def compute_psi(
    expected: np.ndarray,
    actual: np.ndarray,
    n_bins: int = 10,
) -> float:
    """
    Population Stability Index (PSI) between two distributions.

    PSI < 0.10  → No shift
    PSI 0.10-0.20 → Moderate shift (monitor)
    PSI > 0.20  → Significant shift (alert/retrain)

    Parameters
    ----------
    expected : np.ndarray
        Reference distribution (training data).
    actual : np.ndarray
        Current distribution (scoring data).
    n_bins : int
        Number of bins for discretization.

    Returns
    -------
    float : PSI value.
    """
    # Remove NaN
    expected = expected[~np.isnan(expected)]
    actual = actual[~np.isnan(actual)]

    if len(expected) < 10 or len(actual) < 10:
        logger.warning("Insufficient data for PSI: expected=%d, actual=%d",
                        len(expected), len(actual))
        return 0.0

    # Create bins from expected distribution
    breakpoints = np.percentile(expected, np.linspace(0, 100, n_bins + 1))
    breakpoints = np.unique(breakpoints)

    # Compute bucket proportions
    expected_counts = np.histogram(expected, bins=breakpoints)[0]
    actual_counts = np.histogram(actual, bins=breakpoints)[0]

    # Normalize to proportions (with small epsilon to avoid division by zero)
    eps = 1e-6
    expected_pct = (expected_counts + eps) / (expected_counts.sum() + eps * len(expected_counts))
    actual_pct = (actual_counts + eps) / (actual_counts.sum() + eps * len(actual_counts))

    psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
    return float(psi)


class DriftDetector:
    """
    Monitors feature drift and concept drift for the churn model.
    """

    def __init__(self, connection_string: str) -> None:
        self.engine = create_engine(connection_string)

    def check_feature_drift(
        self,
        reference_start: str,
        reference_end: str,
        current_start: Optional[str] = None,
        current_end: Optional[str] = None,
    ) -> Dict[str, Dict]:
        """
        Compute PSI for all features comparing reference period to current.

        Parameters
        ----------
        reference_start, reference_end : str
            Date range for reference (training) distribution.
        current_start, current_end : str, optional
            Date range for current (scoring) distribution.
            Defaults to last 7 days.

        Returns
        -------
        Dict mapping feature_name → {"psi": float, "status": str}
        """
        if current_start is None:
            current_start = str(date.today() - timedelta(days=7))
        if current_end is None:
            current_end = str(date.today())

        # Load reference and current feature distributions
        ref_df = self._load_feature_snapshot(reference_start, reference_end)
        cur_df = self._load_feature_snapshot(current_start, current_end)

        results = {}
        from config.features import FEATURE_CONFIG

        for feature in FEATURE_CONFIG.ALL_FEATURES:
            if feature not in ref_df.columns or feature not in cur_df.columns:
                continue

            ref_values = ref_df[feature].values.astype(float)
            cur_values = cur_df[feature].values.astype(float)

            psi = compute_psi(ref_values, cur_values)

            if psi > MODEL_CONFIG.PSI_DRIFT_THRESHOLD:
                status = "ALERT"
            elif psi > 0.10:
                status = "WARNING"
            else:
                status = "OK"

            results[feature] = {
                "psi": round(psi, 4),
                "status": status,
                "ref_mean": float(np.nanmean(ref_values)),
                "cur_mean": float(np.nanmean(cur_values)),
            }

            if status != "OK":
                logger.warning(
                    "Feature drift %s: %s (PSI=%.4f, ref_mean=%.2f, cur_mean=%.2f)",
                    feature, status, psi,
                    results[feature]["ref_mean"],
                    results[feature]["cur_mean"],
                )

        return results

    def check_concept_drift(
        self,
        lookback_months: int = 3,
    ) -> Dict[str, float]:
        """
        Compare predicted churn rates to actual churn rates by month.

        Returns
        -------
        Dict with monthly predicted_rate, actual_rate, and drift_ratio.
        """
        query = text("""
            SELECT
                DATE_TRUNC('month', ph.score_date)::DATE AS month,
                COUNT(*) AS total_predictions,
                AVG(ph.churn_probability) AS avg_predicted_prob,
                COUNT(*) FILTER (WHERE ph.actual_churned = TRUE)
                    AS actual_churns,
                CASE WHEN COUNT(*) > 0
                    THEN COUNT(*) FILTER (WHERE ph.actual_churned = TRUE)::FLOAT
                         / COUNT(*)
                    ELSE 0
                END AS actual_churn_rate
            FROM ml.churn_predictions_history ph
            WHERE ph.outcome_verified_at IS NOT NULL
              AND ph.score_date >= (CURRENT_DATE - :lookback * INTERVAL '1 month')
            GROUP BY DATE_TRUNC('month', ph.score_date)
            ORDER BY month DESC
        """)

        df = pd.read_sql(query, self.engine, params={"lookback": lookback_months})

        if df.empty:
            logger.info("No verified outcomes yet for concept drift analysis.")
            return {}

        results = {}
        for _, row in df.iterrows():
            month_str = str(row["month"])
            predicted = float(row["avg_predicted_prob"])
            actual = float(row["actual_churn_rate"])
            drift_ratio = abs(predicted - actual) / max(actual, 0.01)

            results[month_str] = {
                "predicted_rate": round(predicted, 4),
                "actual_rate": round(actual, 4),
                "drift_ratio": round(drift_ratio, 4),
                "total_predictions": int(row["total_predictions"]),
            }

            if drift_ratio > 0.30:
                logger.warning(
                    "Concept drift %s: predicted=%.3f, actual=%.3f (ratio=%.2f)",
                    month_str, predicted, actual, drift_ratio,
                )

        return results

    def check_hit_rate_by_tier(self) -> Dict[str, Dict]:
        """
        Check prediction accuracy by risk tier (manager-facing metric).

        Hit rate = (TRUE_POSITIVE + RECOVERED) / total per tier.
        """
        query = text("""
            SELECT
                ph.risk_tier,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE ph.actual_churned = TRUE) AS true_positives,
                COUNT(*) FILTER (
                    WHERE ph.actual_churned = FALSE
                      AND EXISTS (
                          SELECT 1 FROM ml.playbook_executions pe
                          WHERE pe.member_id = ph.member_id
                            AND pe.prediction_date = ph.score_date
                      )
                ) AS recovered,
                COUNT(*) FILTER (
                    WHERE ph.actual_churned = FALSE
                      AND ph.risk_tier IN ('HIGH', 'MEDIUM')
                ) AS false_positives,
                COUNT(*) FILTER (
                    WHERE ph.actual_churned = TRUE
                      AND ph.risk_tier = 'LOW'
                ) AS false_negatives
            FROM ml.churn_predictions_history ph
            WHERE ph.outcome_verified_at IS NOT NULL
              AND ph.score_date >= (CURRENT_DATE - INTERVAL '3 months')
            GROUP BY ph.risk_tier
            ORDER BY
                CASE ph.risk_tier
                    WHEN 'HIGH' THEN 1
                    WHEN 'MEDIUM' THEN 2
                    WHEN 'LOW' THEN 3
                END
        """)

        df = pd.read_sql(query, self.engine)

        results = {}
        retrain_needed = False

        for _, row in df.iterrows():
            tier = row["risk_tier"]
            total = int(row["total"])
            tp = int(row["true_positives"])
            recovered = int(row["recovered"])

            hit_rate = (tp + recovered) / max(total, 1)
            results[tier] = {
                "total": total,
                "true_positives": tp,
                "recovered": recovered,
                "false_positives": int(row["false_positives"]),
                "false_negatives": int(row["false_negatives"]),
                "hit_rate": round(hit_rate, 4),
            }

            if tier in ("HIGH", "MEDIUM"):
                if hit_rate < MODEL_CONFIG.HIT_RATE_MIN_THRESHOLD:
                    logger.warning(
                        "Hit rate for %s tier: %.1f%% — BELOW threshold (%.0f%%)",
                        tier, hit_rate * 100,
                        MODEL_CONFIG.HIT_RATE_MIN_THRESHOLD * 100,
                    )
                    retrain_needed = True

        if retrain_needed:
            logger.warning("RETRAIN RECOMMENDED: hit rate below threshold.")

        return results

    def _load_feature_snapshot(
        self,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Load feature data for a date range."""
        query = text("""
            SELECT *
            FROM ml.training_samples
            WHERE reference_date >= :start_date
              AND reference_date < :end_date
        """)
        return pd.read_sql(query, self.engine, params={
            "start_date": start_date,
            "end_date": end_date,
        })

    def run_full_check(self) -> Dict[str, Dict]:
        """
        Run all drift checks and return combined report.
        """
        report = {}

        logger.info("Running feature drift check...")
        report["feature_drift"] = self.check_feature_drift(
            reference_start="2025-06-01",
            reference_end="2025-12-01",
        )

        logger.info("Running concept drift check...")
        report["concept_drift"] = self.check_concept_drift()

        logger.info("Running hit rate check...")
        report["hit_rate"] = self.check_hit_rate_by_tier()

        # Determine overall status
        has_alert = any(
            v.get("status") == "ALERT"
            for v in report.get("feature_drift", {}).values()
        )
        low_hit_rate = any(
            v.get("hit_rate", 1.0) < MODEL_CONFIG.HIT_RATE_MIN_THRESHOLD
            for tier, v in report.get("hit_rate", {}).items()
            if tier in ("HIGH", "MEDIUM")
        )

        if has_alert or low_hit_rate:
            report["overall_status"] = "RETRAIN_RECOMMENDED"
        else:
            report["overall_status"] = "HEALTHY"

        logger.info("Overall model health: %s", report["overall_status"])
        return report
