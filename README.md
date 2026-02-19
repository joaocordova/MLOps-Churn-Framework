# SkyFit Churn Prediction System

End-to-end ML system for predicting gym member churn, differentiating behavioral vs. financial disengagement, and driving intervention playbooks for gym managers.

**Target:** Senior/Staff ML Engineer technical case (Nubank-level rigor)
**Release:** V1 — Baseline model with 2024-2025 training data

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
| D3 | Behavioral vs financial vs DEFAULT churn types | Different playbooks per situation (D18) |
| D4 | 10-day behavioral churn threshold | Filters normal breaks; catches disengagement early |
| D5 | Daily batch (not real-time) | Aligns with existing morning pipelines; upgrade path documented |
| D6 | SHAP pre-computed at scoring time | Stored as JSONB; Azure Functions stay fast; plain Portuguese |
| D7 | Monthly contracts auto-renew (D17) | days_since_last_payment is the real financial signal |
| D8 | Non-payment = blocked turnstile (D18) | is_defaulter distinguishes forced absence from behavioral |
| D9 | V1 trains on 2024-2025 data (D19) | Broad temporal coverage, captures seasonality |

## Project Structure

```
skyfit-predict/
|-- docs/
|   |-- plans/
|   |   +-- 2026-02-19-churn-prediction-system-design.md  # Full design (Sections 1-5)
|   |-- architecture/
|   |   |-- system-architecture.mermaid                    # End-to-end system diagram
|   |   |-- data-layer.mermaid                             # Data pipeline diagram
|   |   +-- upgrade-path-event-driven.md                   # Batch -> real-time migration
|   +-- skyfit_data_analysis.md                            # Original data analysis + P1-P5
|
|-- sql/
|   |-- silver/
|   |   |-- 00_architecture_complete.sql  # 4 Materialized Views (built)
|   |   +-- 01_validacao_standalone.sql   # 15 validation queries (built)
|   |-- gold/
|   |   |-- ml_training_samples.sql       # Point-in-time feature store (2024+)
|   |   |-- ml_churn_predictions.sql      # Predictions + playbooks tables
|   |   +-- ml_outcome_tracking.sql       # Retroactive outcome verification
|   +-- validation/
|       +-- 02_ml_readiness_validation.sql # 15 ML-specific validation queries
|
|-- config/
|   |-- __init__.py
|   |-- features.py       # 28 features across 8 groups (incl. is_defaulter)
|   +-- model.py           # Hyperparameters, thresholds, D17-D19 rules
|
|-- src/
|   |-- training/
|   |   |-- data_loader.py           # Load + derive features (is_defaulter)
|   |   |-- stacking_ensemble.py     # 4 XGB L0 + LogReg L1 + Platt
|   |   |-- walk_forward.py          # Temporal CV (2024-2025)
|   |   +-- train.py                 # V1 CLI entrypoint
|   |-- scoring/
|   |   |-- batch_scorer.py          # Daily scoring pipeline
|   |   |-- shap_explainer.py        # SHAP + Portuguese templates
|   |   +-- churn_type.py            # BEHAVIORAL/DEFAULT/FINANCIAL/FULL/NONE
|   |-- monitoring/
|   |   +-- drift_detector.py        # PSI, concept drift, hit rate
|   +-- api/                         # Azure Functions (planned)
|
|-- requirements.txt
+-- README.md
```

## Data Foundation (Built)

4 cascading Materialized Views in `analytics` schema:

1. **mv_contract_classified** -- Each contract classified as REGULAR/AGREGADOR
2. **mv_spells_v2** -- Continuous activity periods per segment (gap > 30d = new spell)
3. **mv_churn_events** -- Spell outcomes: CHURN / MIGRACAO / ATIVO / INDETERMINADO
4. **mv_member_kpi_base** -- 1 row per member, all KPIs

## Business Rules

- **Churn definition:** 30 days without contract in any segment
- **Migration != Churn:** Segment change within <= 30 days is MIGRACAO
- **Behavioral churn:** Active contract, paying, but 10+ consecutive days absent (D9)
- **Default (D18):** Non-payment blocks turnstile. Forced absence != behavioral absence
- **Monthly contracts (D17):** Auto-renew every ~30 days. Non-payment = default, not expiration
- **REGULAR only (D13):** Aggregators excluded (different economics, R$0 revenue)

## ML System (V1 Implemented)

- **Features:** 28 point-in-time features across 8 groups (added `is_defaulter` D18)
- **Model:** XGBoost Stacking Ensemble (4 specialist L0 + LogReg meta-learner)
- **Training:** Walk-forward temporal CV on 2024-2025 data (D19)
- **Scoring:** Daily batch via Airflow (4AM)
- **Serving:** Azure Functions API -> Lovable dashboard
- **Monitoring:** PSI drift, concept drift, hit rate, circuit breakers

## Design Progress

- [x] Section 1: System Architecture Overview
- [x] Section 2: Feature Store with Point-in-Time Correctness
- [x] Section 3: Model Architecture (REGULAR only, 4 XGB + LogReg)
- [x] Section 4: Outcome Tracking (retroactive verification + manager report)
- [x] Section 5: Deployment & MLOps (V1 release strategy)
- [ ] Section 6: Trade-off Analysis
- [ ] Section 7: Frontend & Playbooks

## V1 Training

```bash
# Install dependencies
pip install -r requirements.txt

# Run training pipeline (V1)
python -m src.training.train \
    --db-url postgresql://user:pass@host:5432/skyfit \
    --output-dir models/v1

# Run daily scoring
python -m src.scoring.batch_scorer \
    --db-url postgresql://user:pass@host:5432/skyfit \
    --model-dir models/v1/v20260219_120000
```

## How to Resume

If context window is exhausted, start a new conversation with:

> "I'm continuing the SkyFit Churn Prediction System.
> Read `docs/plans/2026-02-19-churn-prediction-system-design.md` for full context.
> Sections 1-5 complete. V1 Python implementation committed.
> Key: REGULAR only (D13), monthly auto-renew (D17), default=blocked (D18),
> 2024-2025 test period (D19). Next: run V1 training, evaluate results."

All decisions (D1-D19), open questions, and designs are in the design doc.
