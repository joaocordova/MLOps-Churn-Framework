"""
SkyFit Churn Prediction — Feature Configuration
================================================
Central registry of all features used in the model.
Single source of truth for feature names, types, and specialist assignments.

Decision references:
  D10: uses_personal_trainer dropped (no data)
  D13: REGULAR members only (no segmento feature)
  D17: Monthly contracts auto-renew every ~30 days. contract_expiring_30d
       is always True for monthly plans → kept but less informative.
       days_since_last_payment is the primary financial signal.
  D18: Non-payment → default → turnstile blocked. Added is_defaulter
       boolean to separate behavioral absence from forced absence.
  ML_VAL_05: 37% NULL check-ins → added has_ever_checked_in
"""

from dataclasses import dataclass, field
from typing import List, Dict


@dataclass(frozen=True)
class FeatureConfig:
    """Immutable feature configuration."""

    # ------------------------------------------------------------------
    # TENURE FEATURES (5)
    # ------------------------------------------------------------------
    TENURE_FEATURES: List[str] = field(default_factory=lambda: [
        "tenure_days",
        "current_spell_duration_days",
        "contracts_in_current_spell",
        "total_previous_spells",
        "total_previous_churns",
    ])

    # ------------------------------------------------------------------
    # FREQUENCY FEATURES (10) — includes has_ever_checked_in from ML_VAL_05
    # ------------------------------------------------------------------
    FREQUENCY_FEATURES: List[str] = field(default_factory=lambda: [
        "checkins_last_7d",
        "checkins_last_14d",
        "checkins_last_30d",
        "checkins_last_90d",
        "days_since_last_checkin",
        "checkin_trend",
        "avg_weekly_checkins_90d",
        "checkin_consistency",
        "weekend_ratio",
        "has_ever_checked_in",  # Added: 37% never checked in (ML_VAL_05)
    ])

    # ------------------------------------------------------------------
    # ENGAGEMENT FEATURES (2)
    # ------------------------------------------------------------------
    ENGAGEMENT_FEATURES: List[str] = field(default_factory=lambda: [
        "peak_hour_ratio",
        "visited_other_branch",
    ])

    # ------------------------------------------------------------------
    # RECENCY FEATURES (3)
    # D17: contract_expiring_30d kept but less predictive for monthly plans.
    # days_since_last_payment is the primary financial timing signal.
    # ------------------------------------------------------------------
    RECENCY_FEATURES: List[str] = field(default_factory=lambda: [
        "days_until_contract_end",
        "contract_expiring_30d",
        "days_since_last_payment",
    ])

    # ------------------------------------------------------------------
    # FINANCIAL FEATURES (4) — added is_defaulter (D18)
    # is_defaulter = has_open_receivable AND days_since_last_payment > 30
    # This means the member is blocked at the turnstile (forced absence).
    # ------------------------------------------------------------------
    FINANCIAL_FEATURES: List[str] = field(default_factory=lambda: [
        "avg_monthly_payment_90d",
        "payment_regularity",
        "has_open_receivable",
        "is_defaulter",  # D18: blocked at turnstile due to non-payment
    ])

    # ------------------------------------------------------------------
    # SEASONALITY FEATURES (2)
    # ------------------------------------------------------------------
    SEASONALITY_FEATURES: List[str] = field(default_factory=lambda: [
        "month_of_year",
        "is_resolution_signup",
    ])

    # ------------------------------------------------------------------
    # DEMOGRAPHIC FEATURES (2)
    # ------------------------------------------------------------------
    DEMOGRAPHIC_FEATURES: List[str] = field(default_factory=lambda: [
        "idade",
        "gender",
    ])

    # ------------------------------------------------------------------
    # SEGMENT FEATURES (1) — had_segment_migration only (D13)
    # ------------------------------------------------------------------
    SEGMENT_FEATURES: List[str] = field(default_factory=lambda: [
        "had_segment_migration",
    ])

    # ------------------------------------------------------------------
    # TARGET
    # ------------------------------------------------------------------
    TARGET: str = "churned_in_30d"

    # ------------------------------------------------------------------
    # METADATA (not used as features)
    # ------------------------------------------------------------------
    METADATA_COLS: List[str] = field(default_factory=lambda: [
        "sample_id",
        "member_id",
        "reference_date",
        "branch_id",
        "prediction_horizon",
        "days_to_event",
        "label_type",
    ])

    # ------------------------------------------------------------------
    # CATEGORICAL FEATURES (need encoding)
    # ------------------------------------------------------------------
    CATEGORICAL_FEATURES: List[str] = field(default_factory=lambda: [
        "gender",
    ])

    # ------------------------------------------------------------------
    # BOOLEAN FEATURES (cast to int)
    # ------------------------------------------------------------------
    BOOLEAN_FEATURES: List[str] = field(default_factory=lambda: [
        "contract_expiring_30d",
        "has_open_receivable",
        "visited_other_branch",
        "is_resolution_signup",
        "had_segment_migration",
        "has_ever_checked_in",
        "is_defaulter",  # D18
    ])

    # ------------------------------------------------------------------
    # SPECIALIST MODEL ASSIGNMENTS
    # ------------------------------------------------------------------
    @property
    def XGB_FREQ_FEATURES(self) -> List[str]:
        """Specialist 1: Attendance decay patterns."""
        return self.FREQUENCY_FEATURES + self.ENGAGEMENT_FEATURES

    @property
    def XGB_FIN_FEATURES(self) -> List[str]:
        """Specialist 2: Payment health (D17/D18 aware)."""
        return self.FINANCIAL_FEATURES

    @property
    def XGB_TENURE_FEATURES(self) -> List[str]:
        """Specialist 3: Lifecycle position + contract timing."""
        return self.TENURE_FEATURES + self.RECENCY_FEATURES

    @property
    def XGB_CONTEXT_FEATURES(self) -> List[str]:
        """Specialist 4: Context, seasonality, demographics."""
        return (
            self.SEASONALITY_FEATURES
            + self.DEMOGRAPHIC_FEATURES
            + self.SEGMENT_FEATURES
        )

    @property
    def ALL_FEATURES(self) -> List[str]:
        """All features used in training (28 total)."""
        return (
            self.TENURE_FEATURES
            + self.FREQUENCY_FEATURES
            + self.ENGAGEMENT_FEATURES
            + self.RECENCY_FEATURES
            + self.FINANCIAL_FEATURES
            + self.SEASONALITY_FEATURES
            + self.DEMOGRAPHIC_FEATURES
            + self.SEGMENT_FEATURES
        )

    # ------------------------------------------------------------------
    # PASSTHROUGH FEATURES (sent directly to L1 meta-learner)
    # ------------------------------------------------------------------
    L1_PASSTHROUGH_FEATURES: List[str] = field(default_factory=lambda: [
        "days_since_last_checkin",
        "days_until_contract_end",
        "checkin_trend",
    ])

    # ------------------------------------------------------------------
    # SHAP EXPLANATION TEMPLATES (Portuguese)
    # ------------------------------------------------------------------
    SHAP_TEMPLATES: Dict[str, str] = field(default_factory=lambda: {
        "days_since_last_checkin": "Sem check-in ha {value} dias (media da academia: {avg} dias)",
        "checkin_trend": "Frequencia caiu {pct}% nas ultimas 2 semanas",
        "days_until_contract_end": "Contrato vence em {value} dias",
        "contract_expiring_30d": "Contrato proximo do vencimento mensal",
        "has_open_receivable": "Possui parcela em aberto",
        "is_defaulter": "Inadimplente — acesso bloqueado na catraca",
        "checkins_last_30d": "Apenas {value} check-ins nos ultimos 30 dias (media: {avg})",
        "checkins_last_7d": "Apenas {value} check-in(s) na ultima semana",
        "total_previous_churns": "Ja cancelou {value} vez(es) antes",
        "payment_regularity": "Regularidade de pagamento: {pct}% (abaixo da media)",
        "avg_monthly_payment_90d": "Ticket medio mensal: R${value:.2f}",
        "checkin_consistency": "Frequencia irregular (variacao de {value:.0f} dias entre visitas)",
        "tenure_days": "Membro ha apenas {months} meses (periodo critico)",
        "peak_hour_ratio": "Treina {pct}% no horario de pico",
        "has_ever_checked_in": "Nunca fez check-in na academia",
        "current_spell_duration_days": "No plano atual ha apenas {value} dias",
        "avg_weekly_checkins_90d": "Media de {value:.1f} treinos por semana (abaixo da media)",
        "weekend_ratio": "Treina principalmente nos fins de semana ({pct}%)",
        "visited_other_branch": "Frequentou outra unidade (possivel insatisfacao)",
        "is_resolution_signup": "Cadastro de janeiro/fevereiro (resolucao de ano novo)",
        "had_segment_migration": "Ja migrou entre plano regular e agregador",
        "idade": "Faixa etaria com maior risco de cancelamento",
        "days_since_last_payment": "Ultimo pagamento ha {value} dias",
        "contracts_in_current_spell": "Apenas {value} renovacao(oes) no periodo atual",
        "total_previous_spells": "Ja teve {value} periodo(s) de atividade antes",
        "month_of_year": "Mes com historico de maior cancelamento",
        "gender": "Perfil demografico com maior risco",
    })


# Singleton instance
FEATURE_CONFIG = FeatureConfig()
