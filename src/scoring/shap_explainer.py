"""
SkyFit Churn Prediction — SHAP Explainer
==========================================
Computes SHAP explanations for each prediction and translates them
to plain Portuguese for gym managers (Decision D6, D15).

Architecture:
  - Uses SHAP TreeExplainer on L0 specialists (not the meta-learner)
  - Takes the top 3 features by absolute SHAP value
  - Maps each feature to a Portuguese template from FEATURE_CONFIG
  - Returns JSONB-ready list of explanations

Pre-computed at scoring time → stored in ml.churn_predictions.top_3_reasons.
Azure Functions just reads the JSONB — no computation at API time.
"""

import logging
from typing import Dict, List, Any, Optional

import numpy as np
import pandas as pd
import shap

from config.features import FEATURE_CONFIG

logger = logging.getLogger(__name__)


class SHAPExplainer:
    """
    Generates SHAP-based explanations for churn predictions.

    Uses TreeExplainer on L0 specialists and aggregates SHAP values
    across all specialists to find the top 3 most impactful features.
    """

    def __init__(self, ensemble) -> None:
        """
        Parameters
        ----------
        ensemble : StackingEnsemble
            Fitted stacking ensemble with L0 specialist models.
        """
        self.ensemble = ensemble
        self.explainers: Dict[str, shap.TreeExplainer] = {}
        self._init_explainers()

    def _init_explainers(self) -> None:
        """Create SHAP TreeExplainer for each L0 specialist."""
        for name, specialist in self.ensemble.specialists.items():
            if specialist.model is not None:
                self.explainers[name] = shap.TreeExplainer(specialist.model)
                logger.info("SHAP explainer initialized for %s", name)

    def explain_batch(
        self,
        X: pd.DataFrame,
        branch_stats: Optional[Dict[str, float]] = None,
    ) -> List[List[Dict[str, Any]]]:
        """
        Generate top-3 SHAP explanations for a batch of predictions.

        Parameters
        ----------
        X : pd.DataFrame
            Feature DataFrame (same shape as training data).
        branch_stats : dict, optional
            Branch-level averages for comparison (e.g., {"days_since_last_checkin": 3.2}).
            Used to populate "media da academia" in templates.

        Returns
        -------
        List of lists, one per row. Each inner list contains 3 dicts:
            {"feature": str, "value": any, "impact": float, "explanation": str}
        """
        branch_stats = branch_stats or {}

        # Compute SHAP values per specialist
        all_shap_values = {}

        for name, specialist in self.ensemble.specialists.items():
            if name not in self.explainers:
                continue

            X_spec = X[specialist.features]
            sv = self.explainers[name].shap_values(X_spec)

            # TreeExplainer returns array for binary classification
            # Use the positive class SHAP values
            if isinstance(sv, list):
                sv = sv[1]

            # Map specialist feature SHAP values back to global feature names
            for idx, feat_name in enumerate(specialist.features):
                if feat_name not in all_shap_values:
                    all_shap_values[feat_name] = sv[:, idx]
                else:
                    # If a feature appears in multiple specialists (shouldn't
                    # happen with our design, but be safe), take the max
                    all_shap_values[feat_name] = np.maximum(
                        np.abs(all_shap_values[feat_name]),
                        np.abs(sv[:, idx]),
                    ) * np.sign(sv[:, idx])

        # Build SHAP matrix (n_samples x n_features)
        feature_names = list(all_shap_values.keys())
        shap_matrix = np.column_stack(
            [all_shap_values[f] for f in feature_names]
        )

        # Generate explanations for each sample
        explanations = []
        for row_idx in range(len(X)):
            row_explanations = self._explain_single(
                shap_values=shap_matrix[row_idx],
                feature_names=feature_names,
                feature_values=X.iloc[row_idx],
                branch_stats=branch_stats,
            )
            explanations.append(row_explanations)

        return explanations

    def _explain_single(
        self,
        shap_values: np.ndarray,
        feature_names: List[str],
        feature_values: pd.Series,
        branch_stats: Dict[str, float],
    ) -> List[Dict[str, Any]]:
        """Generate top-3 explanation for a single prediction."""
        # Sort by absolute SHAP value (descending)
        abs_shap = np.abs(shap_values)
        top_indices = np.argsort(abs_shap)[::-1][:3]

        reasons = []
        for idx in top_indices:
            feat_name = feature_names[idx]
            feat_value = feature_values.get(feat_name, None)
            shap_val = float(shap_values[idx])

            # Convert numpy types to Python native for JSON serialization
            if hasattr(feat_value, "item"):
                feat_value = feat_value.item()
            if isinstance(feat_value, float) and np.isnan(feat_value):
                feat_value = None

            explanation = self._render_template(
                feature=feat_name,
                value=feat_value,
                shap_value=shap_val,
                branch_stats=branch_stats,
            )

            reasons.append({
                "feature": feat_name,
                "value": feat_value,
                "impact": round(shap_val, 4),
                "explanation": explanation,
            })

        return reasons

    def _render_template(
        self,
        feature: str,
        value: Any,
        shap_value: float,
        branch_stats: Dict[str, float],
    ) -> str:
        """
        Render a Portuguese explanation from SHAP template.

        Falls back to a generic template if no specific one exists.
        """
        template = FEATURE_CONFIG.SHAP_TEMPLATES.get(feature)

        if template is None:
            # Generic fallback
            direction = "aumenta" if shap_value > 0 else "diminui"
            return f"{feature} {direction} o risco de cancelamento"

        # Build template context
        avg = branch_stats.get(feature, 0)

        try:
            if value is None:
                return template.split("(")[0].strip()

            # Compute derived values for templates
            context = {
                "value": value,
                "avg": avg,
                "months": int(value / 30) if isinstance(value, (int, float)) and value > 0 else 0,
                "pct": 0,
            }

            # Compute percentage for trend features
            if feature == "checkin_trend" and isinstance(value, (int, float)):
                context["pct"] = int((1 - value) * 100) if value <= 1 else 0
            elif feature == "peak_hour_ratio" and isinstance(value, (int, float)):
                context["pct"] = int(value * 100)
            elif feature == "weekend_ratio" and isinstance(value, (int, float)):
                context["pct"] = int(value * 100)
            elif feature == "payment_regularity" and isinstance(value, (int, float)):
                context["pct"] = int(value * 100)

            return template.format(**context)

        except (KeyError, ValueError, TypeError) as exc:
            logger.debug(
                "Template rendering failed for %s (value=%s): %s",
                feature, value, exc,
            )
            direction = "aumenta" if shap_value > 0 else "diminui"
            return f"{feature} {direction} o risco de cancelamento"


def compute_branch_stats(
    engine,
    branch_id: int,
) -> Dict[str, float]:
    """
    Compute branch-level averages for SHAP template rendering.

    These are used to compare individual members against their branch
    (e.g., "Sem check-in ha 18 dias (media da academia: 3 dias)").
    """
    from sqlalchemy import text

    query = text("""
        SELECT
            AVG(days_since_last_checkin)   AS avg_days_since_checkin,
            AVG(checkins_last_30d)         AS avg_checkins_30d,
            AVG(avg_weekly_checkins_90d)   AS avg_weekly_checkins,
            AVG(avg_monthly_payment_90d)   AS avg_monthly_payment,
            AVG(checkin_consistency)        AS avg_consistency
        FROM ml.training_samples
        WHERE branch_id = :branch_id
          AND label_type = 'ACTIVE'
          AND reference_date >= (CURRENT_DATE - INTERVAL '90 days')
    """)

    import pandas as pd
    result = pd.read_sql(query, engine, params={"branch_id": branch_id})

    if result.empty:
        return {}

    row = result.iloc[0]
    return {
        "days_since_last_checkin": float(row.get("avg_days_since_checkin", 0) or 0),
        "checkins_last_30d": float(row.get("avg_checkins_30d", 0) or 0),
        "avg_weekly_checkins_90d": float(row.get("avg_weekly_checkins", 0) or 0),
        "avg_monthly_payment_90d": float(row.get("avg_monthly_payment", 0) or 0),
        "checkin_consistency": float(row.get("avg_consistency", 0) or 0),
    }
