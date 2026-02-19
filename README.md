# SkyFit Churn Prediction System

End-to-end ML system for predicting gym member churn, differentiating behavioral vs. financial disengagement, and driving intervention playbooks for gym managers.

**Target:** Senior/Staff ML Engineer technical case (Nubank-level rigor)

---

## Architecture Overview

```
BRONZE (core.*)  -->  SILVER (analytics.*)  -->  GOLD (ml.*)
EVO raw tables        Contract classification     Feature store (PIT)
                      Spells by segment           Training samples
                      Churn events                Predictions + SHAP
                                                  Playbook assignment
                                    |
                           +--------+--------+
                           |                 |
                      ML Pipeline       API Layer
                      XGBoost Stack     Azure Functions
                      Daily Airflow     Parameterized SQL
                           |                 |
                           +--------+--------+
                                    |
                            Lovable Dashboard
                            (Gym Manager Persona)
```

## Key Design Decisions

| # | Decision | Rationale |
|---|----------|-----------|
| D1 | Point-in-time feature store | Prevents data leakage; features computed as of reference_date, not today |
| D2 | Multi-horizon training (spell_end -30d, -15d, 0d) | Model learns "cooling off" gradient; marketing can intervene early |
| D3 | Behavioral vs financial churn types | Different playbooks: "come back" (still paying) vs "win-back" (contract ended) |
| D4 | 10-day behavioral churn threshold | Filters normal breaks; catches disengagement before mental checkout |
| D5 | Daily batch (not real-time) | Aligns with existing morning pipelines; upgrade path documented |
| D6 | SHAP pre-computed at scoring time | Stored as JSONB; Azure Functions stay fast; gym managers get plain language |
| D7 | Playbook = rule-based on tier + type | Deterministic, explainable; no additional ML model needed |
| D8 | dbt for data lineage | Idempotent, testable, documented; Nubank-style data contracts |

## Project Structure

```
skyfit-predict/
|-- docs/
|   |-- plans/
|   |   +-- 2026-02-19-churn-prediction-system-design.md  # Full design document
|   |-- architecture/
|   |   |-- system-architecture.mermaid                    # End-to-end system diagram
|   |   |-- data-layer.mermaid                             # Data pipeline diagram
|   |   +-- upgrade-path-event-driven.md                   # Batch -> real-time migration
|   +-- skyfit_data_analysis.md                            # Original data analysis + P1-P5 issues
|
|-- sql/
|   |-- bronze/                          # Source table definitions (reference only)
|   |-- silver/
|   |   |-- 00_architecture_complete.sql # 4 Materialized Views (built)
|   |   +-- 01_validacao_standalone.sql  # 15 validation queries (built)
|   |-- gold/
|   |   |-- ml_training_samples.sql      # Point-in-time feature store (designed)
|   |   +-- ml_churn_predictions.sql     # Predictions + playbooks tables (designed)
|   +-- ml/                              # Model-specific queries
|
|-- src/
|   |-- feature_store/                   # Feature computation Python code
|   |-- training/                        # Model training pipeline
|   |-- scoring/                         # Daily batch scoring
|   |-- api/                             # Azure Functions endpoints
|   +-- monitoring/                      # Drift detection + circuit breakers
|
|-- airflow/
|   +-- dags/                            # Airflow DAG definitions
|
|-- tests/                               # Unit + integration tests
|-- config/                              # Model hyperparameters, feature lists
+-- README.md
```

## Data Foundation (Built)

4 cascading Materialized Views in `analytics` schema:

1. **mv_contract_classified** — Each contract classified as REGULAR/AGREGADOR using 2-level hierarchy
2. **mv_spells_v2** — Continuous activity periods per segment (gap > 30d = new spell)
3. **mv_churn_events** — Each spell outcome: CHURN / MIGRACAO / ATIVO / INDETERMINADO
4. **mv_member_kpi_base** — 1 row per member, all KPIs (dashboard source)

Refresh order: 1 -> 2 -> 3 -> 4 (dependencies cascade)

## Known Issues Addressed (P1-P5)

| Issue | Problem | Resolution |
|-------|---------|------------|
| P1 | Aggregator classification incomplete | 2-level: plan name + gympass_id/entries cross-reference |
| P2 | value_next_month 100% NULL | Revenue from mv_receivables_normalized |
| P3 | Spells mix segments | Partitioned by REGULAR/AGREGADOR |
| P4 | Branch 149 missing | Added to MVP array |
| P5 | SQL injection in Azure Functions | Parameterized queries in all new endpoints |

## Business Rules

- **Churn definition:** 30 days without contract in any segment
- **Migration != Churn:** Segment change within <= 30 days is MIGRACAO
- **Behavioral churn:** Active contract but 10+ consecutive days absent
- **Aggregator revenue:** R$0 (B2B pass-through, not in database)
- **Revenue source:** ltv.mv_receivables_normalized (never value_next_month)

## ML System (In Design)

- **Features:** 27 point-in-time features across 8 groups (tenure, frequency, recency, financial, engagement, seasonality, demographic, segment)
- **Model:** XGBoost Stacking Ensemble (4 specialist L0 models + LogReg meta-learner)
- **Scoring:** Daily batch via Airflow (4AM), aligned with existing morning pipelines
- **Serving:** Azure Functions API -> Lovable dashboard
- **Monitoring:** PSI for feature drift, actual-vs-predicted for concept drift, circuit breakers for data quality

## Design Progress

- [x] Section 1: System Architecture Overview (approved)
- [x] Section 2: Feature Store with Point-in-Time Correctness (approved)
- [ ] Section 3: Model Architecture
- [ ] Section 4: Deployment & MLOps
- [ ] Section 5: Trade-off Analysis
- [ ] Section 6: Frontend & Playbooks

## How to Resume

If context window is exhausted, start a new conversation with:

> "I'm continuing the SkyFit Churn Prediction System design.
> Read `docs/plans/2026-02-19-churn-prediction-system-design.md` for full context.
> Sections 1-2 are approved. Continue with Section 3: Model Architecture."

All decisions, open questions, and approved designs are persisted in the design doc.
