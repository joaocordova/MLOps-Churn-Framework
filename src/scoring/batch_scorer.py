"""
SkyFit Churn Prediction — Daily Batch Scorer
==============================================
Scores all active REGULAR members daily (Decision D5).

Pipeline Steps (run by Airflow at 4AM):
  1. Refresh materialized views (Bronze → Silver → Gold)
  2. Load active members' features from ml.training_samples logic
  3. Run stacking ensemble → calibrated probabilities
  4. Classify churn type (BEHAVIORAL/FINANCIAL/FULL/NONE)
  5. Compute SHAP explanations (top 3 in Portuguese)
  6. Assign playbooks
  7. Write predictions to ml.churn_predictions
  8. Append to ml.churn_predictions_history
  9. Run quality gates (circuit breakers)

Usage:
  python -m src.scoring.batch_scorer \\
      --db-url postgresql://... \\
      --model-dir models/v20260219_120000
"""

import argparse
import logging
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

from config.features import FEATURE_CONFIG
from config.model import MODEL_CONFIG
from src.training.stacking_ensemble import StackingEnsemble
from src.training.data_loader import _enforce_types, _add_derived_features
from src.scoring.churn_type import classify_churn_type, assign_playbook
from src.scoring.shap_explainer import SHAPExplainer, compute_branch_stats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


class BatchScorer:
    """
    Daily batch scoring pipeline for churn predictions.

    Loads a trained ensemble model and scores all active REGULAR members.
    """

    def __init__(
        self,
        connection_string: str,
        model_dir: Path,
    ) -> None:
        self.connection_string = connection_string
        self.engine = create_engine(connection_string)
        self.model_dir = model_dir
        self.model_version = model_dir.name
        self.score_date = date.today()

        # Load model
        self.ensemble = StackingEnsemble()
        self.ensemble.load(model_dir)
        logger.info("Model loaded: %s", self.model_version)

        # Initialize SHAP explainer
        self.explainer = SHAPExplainer(self.ensemble)

    def run(self) -> Dict[str, int]:
        """
        Execute the full scoring pipeline.

        Returns
        -------
        Dict with scoring summary:
          - total_scored, high_count, medium_count, low_count
          - behavioral_count, financial_count, full_count
        """
        logger.info("=" * 70)
        logger.info("DAILY SCORING PIPELINE — %s", self.score_date)
        logger.info("=" * 70)

        # Step 1: Refresh materialized views
        self._refresh_materialized_views()

        # Step 2: Load active members' features
        df = self._load_active_members()
        if df.empty:
            logger.warning("No active REGULAR members found. Aborting.")
            return {"total_scored": 0}

        # Step 3: Quality gate — check NULL rates
        self._check_null_rates(df)

        # Step 4: Compute features and predict
        X, metadata = self._prepare_features(df)
        probabilities = self.ensemble.predict_proba(X)
        risk_tiers = self.ensemble.predict_risk_tier(probabilities)

        # Step 5: Classify churn type
        scored_df = metadata.copy()
        scored_df["churn_probability"] = probabilities
        scored_df["risk_tier"] = risk_tiers

        # Add features needed for churn type classification (D17, D18)
        for col in ["days_since_last_checkin", "contract_expiring_30d",
                     "days_until_contract_end", "has_ever_checked_in",
                     "is_defaulter", "has_open_receivable"]:
            if col in X.columns:
                scored_df[col] = X[col].values

        scored_df["churn_type"] = classify_churn_type(scored_df)

        # Step 6: Assign playbooks
        scored_df["playbook_id"] = assign_playbook(
            scored_df["risk_tier"],
            scored_df["churn_type"],
        )

        # Step 7: Compute SHAP explanations per branch
        scored_df["top_3_reasons"] = self._compute_shap_all(X, scored_df)

        # Step 8: Add context columns for gym managers
        scored_df = self._add_context_columns(scored_df, X)

        # Step 9: Write predictions
        self._write_predictions(scored_df)
        self._append_to_history(scored_df)

        # Step 10: Summary
        summary = self._log_summary(scored_df)

        logger.info("=" * 70)
        logger.info("SCORING COMPLETE — %d members scored", len(scored_df))
        logger.info("=" * 70)

        return summary

    def _refresh_materialized_views(self) -> None:
        """Refresh the 4 cascading materialized views (Bronze → Silver → Gold)."""
        views = [
            "analytics.mv_contract_classified",
            "analytics.mv_spells_v2",
            "analytics.mv_churn_events",
            "analytics.mv_member_kpi_base",
        ]

        with self.engine.begin() as conn:
            for view in views:
                logger.info("Refreshing %s...", view)
                conn.execute(text(
                    f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"
                ))
        logger.info("All materialized views refreshed.")

    def _load_active_members(self) -> pd.DataFrame:
        """
        Load features for all active REGULAR members.

        Uses the same feature computation logic as ml.training_samples
        but with reference_date = TODAY.
        """
        query = text("""
            WITH active_regular AS (
                SELECT DISTINCT
                    m.id_member AS member_id,
                    m.id_branch AS branch_id
                FROM core.evo_members m
                JOIN analytics.mv_contract_classified cc
                    ON cc.member_id = m.id_member
                WHERE cc.segmento = 'REGULAR'
                  AND cc.status = 'Ativo'
                  AND m.id_branch = ANY(:branch_ids)
            )
            SELECT
                ar.member_id,
                ar.branch_id,
                kpi.*
            FROM active_regular ar
            LEFT JOIN analytics.mv_member_kpi_base kpi
                ON kpi.id_member = ar.member_id
        """)

        df = pd.read_sql(
            query,
            self.engine,
            params={"branch_ids": MODEL_CONFIG.MVP_BRANCH_IDS},
        )

        logger.info("Loaded %d active REGULAR members for scoring", len(df))
        return df

    def _check_null_rates(self, df: pd.DataFrame) -> None:
        """
        Circuit breaker: halt if NULL rate exceeds threshold.

        Decision: NULL_RATE_CIRCUIT_BREAKER = 5% (MODEL_CONFIG).
        Exception: check-in features allowed up to 40% NULL (ML_VAL_05).
        """
        threshold = MODEL_CONFIG.NULL_RATE_CIRCUIT_BREAKER
        checkin_features = {
            "days_since_last_checkin", "checkins_last_7d",
            "checkins_last_14d", "checkins_last_30d", "checkins_last_90d",
            "checkin_trend", "avg_weekly_checkins_90d", "checkin_consistency",
        }

        critical_nulls = []
        for col in FEATURE_CONFIG.ALL_FEATURES:
            if col in df.columns:
                null_rate = df[col].isna().mean()
                # Check-in features have known 37% NULL rate (ML_VAL_05)
                col_threshold = 0.45 if col in checkin_features else threshold

                if null_rate > col_threshold:
                    critical_nulls.append((col, null_rate))
                    logger.warning(
                        "CIRCUIT BREAKER: %s NULL rate %.1f%% > %.1f%% threshold",
                        col, null_rate * 100, col_threshold * 100,
                    )

        if critical_nulls:
            msg = (
                f"Scoring halted: {len(critical_nulls)} features exceed "
                f"NULL rate threshold. Fix data quality before scoring."
            )
            logger.error(msg)
            raise RuntimeError(msg)

        logger.info("Quality gate passed: all NULL rates within thresholds.")

    def _prepare_features(self, df: pd.DataFrame):
        """
        Prepare feature matrix and metadata from active members.

        Returns
        -------
        X : pd.DataFrame (features only)
        metadata : pd.DataFrame (member_id, branch_id)
        """
        # Apply same type enforcement as training
        df = _enforce_types(df)
        df = _add_derived_features(df)

        # Extract metadata
        metadata = df[["member_id", "branch_id"]].copy()

        # Build feature matrix — fill missing columns with NaN
        feature_cols = FEATURE_CONFIG.ALL_FEATURES
        X = pd.DataFrame(index=df.index)
        for col in feature_cols:
            if col in df.columns:
                X[col] = df[col]
            else:
                X[col] = np.nan
                logger.warning("Feature %s not in scoring data — filled with NaN", col)

        return X, metadata

    def _compute_shap_all(
        self,
        X: pd.DataFrame,
        scored_df: pd.DataFrame,
    ) -> list:
        """Compute SHAP explanations, grouped by branch for context."""
        import json

        all_explanations = [None] * len(X)
        branches = scored_df["branch_id"].unique()

        for branch_id in branches:
            mask = scored_df["branch_id"] == branch_id
            X_branch = X.loc[mask]
            branch_indices = X_branch.index.tolist()

            # Get branch-level stats for comparison
            branch_stats = compute_branch_stats(self.engine, int(branch_id))

            # Compute explanations
            explanations = self.explainer.explain_batch(X_branch, branch_stats)

            for idx, exp in zip(branch_indices, explanations):
                all_explanations[idx] = json.dumps(exp, ensure_ascii=False)

        return all_explanations

    def _add_context_columns(
        self,
        scored_df: pd.DataFrame,
        X: pd.DataFrame,
    ) -> pd.DataFrame:
        """Add quick-reference context columns for gym managers."""
        # These are stored alongside predictions for fast API queries
        context_cols = {
            "days_until_contract_end": "days_until_contract_end",
            "days_since_last_checkin": "days_since_last_checkin",
            "avg_weekly_checkins_90d": "avg_weekly_checkins",
        }

        for src_col, tgt_col in context_cols.items():
            if src_col in X.columns:
                scored_df[tgt_col] = X[src_col].values

        # Add last_checkin_date from raw data if available
        scored_df["last_checkin_date"] = None  # Will be populated from entries
        scored_df["segmento"] = "REGULAR"  # All scored members are REGULAR (D13)

        return scored_df

    def _write_predictions(self, scored_df: pd.DataFrame) -> None:
        """
        Write predictions to ml.churn_predictions (UPSERT).

        Deletes today's predictions first, then inserts fresh batch.
        """
        with self.engine.begin() as conn:
            # Delete existing predictions for today
            conn.execute(
                text("DELETE FROM ml.churn_predictions WHERE score_date = :sd"),
                {"sd": self.score_date},
            )

            # Insert new predictions
            insert_query = text("""
                INSERT INTO ml.churn_predictions (
                    member_id, branch_id, scored_at, churn_probability,
                    risk_tier, churn_type, top_3_reasons, playbook_id,
                    days_until_contract_end, last_checkin_date,
                    days_since_last_checkin, avg_weekly_checkins,
                    segmento, model_version, score_date
                ) VALUES (
                    :member_id, :branch_id, :scored_at, :churn_probability,
                    :risk_tier, :churn_type, :top_3_reasons::JSONB, :playbook_id,
                    :days_until_contract_end, :last_checkin_date,
                    :days_since_last_checkin, :avg_weekly_checkins,
                    :segmento, :model_version, :score_date
                )
            """)

            records = []
            for _, row in scored_df.iterrows():
                records.append({
                    "member_id": int(row["member_id"]),
                    "branch_id": int(row["branch_id"]),
                    "scored_at": datetime.now(),
                    "churn_probability": round(float(row["churn_probability"]), 4),
                    "risk_tier": str(row["risk_tier"]),
                    "churn_type": str(row["churn_type"]),
                    "top_3_reasons": row["top_3_reasons"],
                    "playbook_id": str(row["playbook_id"]),
                    "days_until_contract_end": (
                        int(row["days_until_contract_end"])
                        if pd.notna(row.get("days_until_contract_end")) else None
                    ),
                    "last_checkin_date": row.get("last_checkin_date"),
                    "days_since_last_checkin": (
                        int(row["days_since_last_checkin"])
                        if pd.notna(row.get("days_since_last_checkin")) else None
                    ),
                    "avg_weekly_checkins": (
                        round(float(row["avg_weekly_checkins"]), 2)
                        if pd.notna(row.get("avg_weekly_checkins")) else None
                    ),
                    "segmento": "REGULAR",
                    "model_version": self.model_version,
                    "score_date": self.score_date,
                })

            conn.execute(insert_query, records)

        logger.info(
            "Wrote %d predictions to ml.churn_predictions (score_date=%s)",
            len(records), self.score_date,
        )

    def _append_to_history(self, scored_df: pd.DataFrame) -> None:
        """Append predictions to history table (append-only, for drift analysis)."""
        with self.engine.begin() as conn:
            insert_query = text("""
                INSERT INTO ml.churn_predictions_history (
                    member_id, branch_id, scored_at, score_date,
                    churn_probability, risk_tier, churn_type,
                    model_version
                ) VALUES (
                    :member_id, :branch_id, :scored_at, :score_date,
                    :churn_probability, :risk_tier, :churn_type,
                    :model_version
                )
            """)

            records = []
            for _, row in scored_df.iterrows():
                records.append({
                    "member_id": int(row["member_id"]),
                    "branch_id": int(row["branch_id"]),
                    "scored_at": datetime.now(),
                    "score_date": self.score_date,
                    "churn_probability": round(float(row["churn_probability"]), 4),
                    "risk_tier": str(row["risk_tier"]),
                    "churn_type": str(row["churn_type"]),
                    "model_version": self.model_version,
                })

            conn.execute(insert_query, records)

        logger.info("Appended %d records to predictions_history", len(records))

    def _log_summary(self, scored_df: pd.DataFrame) -> Dict[str, int]:
        """Log scoring summary and return stats."""
        summary = {
            "total_scored": len(scored_df),
            "score_date": str(self.score_date),
            "model_version": self.model_version,
        }

        # Risk tier distribution
        for tier in ["HIGH", "MEDIUM", "LOW"]:
            count = (scored_df["risk_tier"] == tier).sum()
            summary[f"{tier.lower()}_count"] = count
            logger.info(
                "  %s risk: %d (%.1f%%)",
                tier, count, count / len(scored_df) * 100,
            )

        # Churn type distribution (D18: added DEFAULT)
        for ctype in ["BEHAVIORAL", "FINANCIAL", "DEFAULT", "FULL", "NONE"]:
            count = (scored_df["churn_type"] == ctype).sum()
            summary[f"{ctype.lower()}_count"] = count

        return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SkyFit Churn Prediction — Daily Batch Scorer"
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
        "--model-dir",
        required=True,
        help="Path to saved model directory",
    )
    args = parser.parse_args()

    # Resolve database connection
    if args.db_url:
        db_url = args.db_url
    else:
        from config.database import get_connection_string
        env_path = Path(args.env_file) if args.env_file else None
        db_url = get_connection_string(env_path)

    scorer = BatchScorer(
        connection_string=db_url,
        model_dir=Path(args.model_dir),
    )
    summary = scorer.run()

    logger.info("Summary: %s", summary)


if __name__ == "__main__":
    main()
