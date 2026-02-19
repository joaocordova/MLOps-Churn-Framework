# SkyFit Churn Prediction System — Design Document

**Date:** 2026-02-19
**Version:** 2.0
**Author:** ML Systems Architecture
**Status:** Sections 1-4 approved. Sections 5-7 in design.
**Target:** Senior/Staff ML Engineer technical case (Nubank-level rigor)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Architecture Overview](#2-system-architecture-overview)
3. [Data Engineering & Feature Store](#3-data-engineering--feature-store)
4. [Model Architecture](#4-model-architecture)
5. [Deployment & MLOps](#5-deployment--mlops) — PENDING
6. [Trade-off Analysis](#6-trade-off-analysis) — PENDING
7. [Frontend & Playbooks](#7-frontend--playbooks) — PENDING
8. [Decisions Log](#8-decisions-log)
9. [Open Questions](#9-open-questions)
10. [Appendix: P1-P5 Issue Resolution](#10-appendix-p1-p5-issue-resolution)

---

## 1. Executive Summary

### What We're Building

An end-to-end churn prediction system for SkyFit gym network that:

- **Predicts** which REGULAR members will churn (30-day inactivity gap) with calibrated probabilities
- **Differentiates** real churn from segment migration (Regular -> Aggregator)
- **Differentiates** behavioral churn (paying but not attending) from financial churn (contract ended)
- **Explains** predictions to gym managers using plain-language SHAP reasons
- **Acts** through intervention playbooks assigned by risk tier and churn type
- **Validates** retroactively: tracks who actually churned, who improved, who worsened
- **Scores** daily via batch pipeline aligned with existing morning data refresh

### Scope

- **Branches:** 9 MVP branches [345, 181, 59, 233, 401, 166, 33, 6, 149]
- **Model scope:** REGULAR members only (Aggregators excluded from training and scoring)
- **Data:** 130M+ records across PostgreSQL/Azure
- **Inference:** Daily batch scoring (overnight Airflow, dashboard refreshes each morning)
- **Frontend:** Lovable-based gym manager dashboard with risk overview, member detail, SHAP explanations, playbook actions, and monthly outcome reports

### Approach Selected

**Approach A: dbt + Offline Scoring + Azure Functions API**

Rationale: Matches existing infrastructure (Azure PostgreSQL, Azure Functions), adds production rigor (dbt tests, data contracts, automated retraining) without over-engineering (no Feast, no Kubernetes). Demonstrates Nubank engineering values: idempotency, data contracts, security-first SQL.

Rejected alternatives:
- **Approach B** (Feast + MLflow + AKS): Over-engineered for 9 branches / ~30K-100K active members.
- **Approach C** (Pure SQL + pg_cron): No lineage, no data contracts, no automated testing.

---

## 2. System Architecture Overview

### High-Level Architecture

```
+-----------------------------------------------------------------------+
|                    SKYFIT CHURN PREDICTION SYSTEM                      |
|                    (REGULAR members only)                              |
+-----------------------------------------------------------------------+
|                                                                         |
|  DATA ENGINEERING (dbt)                                                |
|  BRONZE (core.*) --> SILVER (analytics.*) --> GOLD (ml.*)              |
|  EVO raw tables      Contract classification    Feature store (PIT)    |
|                      Spells by segment           Training samples      |
|                      Churn events                Predictions + SHAP    |
|                                                  Playbook assignment   |
|                                                  Outcome tracking      |
|                                                                         |
|  ML PIPELINE                                                           |
|  XGBoost Stacking (4 specialists + LogReg meta)                       |
|  Walk-forward temporal validation                                      |
|  Platt calibration + SHAP explainer                                   |
|                                                                         |
|  SCORING & SERVING                                                     |
|  Airflow DAG (daily 4AM) --> ml.churn_predictions                     |
|  Azure Functions API --> Lovable Dashboard                             |
|                                                                         |
|  MONITORING & OUTCOME TRACKING                                         |
|  Feature drift (PSI weekly)                                            |
|  Concept drift (actual vs predicted monthly)                           |
|  Retroactive outcome verification (30d after each prediction)          |
|  Manager-facing monthly outcome report                                 |
|  Circuit breakers (halt if data quality drops)                         |
+-----------------------------------------------------------------------+
```

### Key Design Decisions

1. **REGULAR members only** — Aggregators excluded from model (different economics, R$0 revenue, different churn drivers)
2. **`mv_member_kpi_base` is NOT the ML feature store** — it's a dashboard snapshot. ML uses point-in-time features.
3. **SHAP reasons pre-computed at scoring time** — stored as JSONB for fast API responses
4. **Playbook assignment is rule-based** — deterministic, explainable, no additional ML
5. **Retroactive outcome tracking** — every prediction is verified 30 days later to build manager trust and detect model drift

---

## 3. Data Engineering & Feature Store

### 3.1 Existing Data Layer (BUILT)

4 Materialized Views in `analytics` schema:

| MV | Purpose | Refresh Order |
|----|---------|--------------|
| `mv_contract_classified` | Each contract as REGULAR/AGREGADOR, with `data_efetiva_fim` | 1st |
| `mv_spells_v2` | Continuous periods per segment, gap > 30d = new spell | 2nd |
| `mv_churn_events` | Each spell outcome: CHURN / MIGRACAO / ATIVO / INDETERMINADO | 3rd |
| `mv_member_kpi_base` | 1 row per member, all KPIs consolidated (for dashboard) | 4th |

Business rules encoded:
- Aggregator classification: Level 1 (plan name) + Level 2 (gympass_id + entries cross-reference)
- "Diaria validada com sucesso" = AGREGADOR
- Churn = 30 days without contract in any segment
- Migration = segment change within <= 30 days (NOT churn)
- Aggregator revenue = R$0 (B2B pass-through not in database)
- Revenue source = `ltv.mv_receivables_normalized` (NOT `value_next_month` which is 100% NULL)

### 3.2 Point-in-Time Feature Store (TO BUILD)

#### Why `mv_member_kpi_base` Cannot Be Used for Training

| `mv_member_kpi_base` | `ml.training_samples` |
|---|---|
| 1 row per member (snapshot today) | N rows per member (one per reference_date) |
| Uses hardcoded cutoff date | Each sample has its own temporal window |
| `dias_sem_checkin` uses today | `days_since_last_checkin` uses reference_date |
| `total_churns` includes ALL churns | `total_previous_churns` only counts before reference |
| Good for dashboards | Good for ML training |

#### Multi-Horizon Training Samples

For each churn event, we generate 3 training samples:

- **spell_end - 30d** ("early warning") — actionable, 30 days to intervene
- **spell_end - 15d** ("mid warning") — still actionable, 15 days to intervene
- **spell_end** ("late warning") — model learns the maximum fatigue state

This teaches the model the "cooling off" gradient so it can detect disengagement early.

#### Training Sample Generation Rules

**Positive samples:** 3 per REGULAR churn event (spell_end, -15d, -30d)
**Negative samples:** Monthly snapshots of active REGULAR members (verified active 30d later)

**Exclusions:**
- AGREGADOR members (not in model scope)
- Migration events (not churn)
- Members < 30 days since registration (erratic newcomer behavior)
- Reference dates within 30 days of data cutoff (can't verify label)
- Members with zero check-ins ever (never engaged)

**Imbalance:** Preserve natural prevalence. Handle in model via `scale_pos_weight`.

#### Feature Store Schema: 26 Features

**Tenure (5):** tenure_days, current_spell_duration_days, contracts_in_current_spell, total_previous_spells, total_previous_churns

**Frequency (9):** checkins_7d/14d/30d/90d, days_since_last_checkin, checkin_trend, avg_weekly_checkins_90d, checkin_consistency, weekend_ratio

**Engagement (2):** peak_hour_ratio, visited_other_branch

**Recency (3):** days_until_contract_end, contract_expiring_30d, days_since_last_payment

**Financial (3):** avg_monthly_payment_90d, payment_regularity, has_open_receivable

**Seasonality (2):** month_of_year, is_resolution_signup

**Demographic (2):** idade, gender

**Segment (1):** had_segment_migration

Every feature has a documented leakage check. All use `WHERE date <= reference_date`.

### 3.3 Churn Type Differentiation

| Behavior | Pays? | Attends? | Label | Playbook |
|----------|-------|----------|-------|----------|
| Active | Yes | Yes | Not at risk | None |
| **Behavioral churn** | Yes | No (10+ days absent) | `BEHAVIORAL` | "Come back" campaign |
| **Financial churn** | No | No | `FINANCIAL` | Win-back offer |
| **Full churn** | No | No (both signals) | `FULL` | Emergency rescue |

Threshold: 10 consecutive days absent = behavioral churn signal.

### 3.4 Prediction Output: `ml.churn_predictions`

One row per active REGULAR member per day. Includes:
- `churn_probability` (calibrated 0-1)
- `risk_tier` (HIGH > 0.7, MEDIUM 0.4-0.7, LOW < 0.4)
- `churn_type` (BEHAVIORAL / FINANCIAL / FULL / NONE)
- `top_3_reasons` (JSONB, SHAP in plain language)
- `playbook_id` (rule-based assignment)
- `model_version` (for audit trail)

### 3.5 Outcome Tracking: `ml.churn_predictions_history`

Append-only table. Every daily prediction is copied here. 30 days later, a monitoring job fills `actual_churned` (TRUE/FALSE) and `outcome_category`:

| Outcome Category | Definition |
|-----------------|------------|
| `TRUE_POSITIVE` | Predicted HIGH/MEDIUM risk, member actually churned |
| `TRUE_NEGATIVE` | Predicted LOW risk, member stayed active |
| `FALSE_POSITIVE` | Predicted HIGH/MEDIUM risk, member stayed (possibly recovered by playbook) |
| `FALSE_NEGATIVE` | Predicted LOW risk, member churned (model missed it) |
| `RECOVERED` | Predicted HIGH/MEDIUM risk, playbook was executed, member stayed |
| `IMPROVED` | Was MEDIUM risk, moved to LOW risk in subsequent month |
| `WORSENED` | Was LOW/MEDIUM risk, moved to higher risk tier |

---

## 4. Model Architecture

### 4.1 Scope: REGULAR Members Only

```
TRAINING:  segmento = 'REGULAR' from ml.training_samples
SCORING:   All active REGULAR members daily
EXCLUDED:  AGREGADOR members (different economics, R$0 revenue)
FLAGGED:   Members who migrated to AGREGADOR shown as "MIGRADO" in dashboard
```

Dropping `segmento` feature (always REGULAR). Keeping `had_segment_migration` (behavioral signal).
Revised total: **26 features**.

### 4.2 Stacking Ensemble: 4 Specialists + Meta-Learner

```
ml.training_samples (REGULAR only)
            |
  +---------+---------+---------+
  |         |         |         |
  v         v         v         v
XGB_freq  XGB_fin  XGB_tenure XGB_context
(9 feat)  (3 feat) (7 feat)   (7 feat)
  |         |         |         |
  P(churn)  P(churn)  P(churn)  P(churn)
  |         |         |         |
  +----+----+----+----+         |
       |              |         |
       v              v         v
  L1: Logistic Regression (meta-learner)
  Inputs: 4 probabilities + 3 passthrough features
  Output: calibrated P(churn_in_30d)
```

**Specialist assignment:**

| Model | Features | Signal |
|-------|----------|--------|
| `XGB_freq` (9) | checkins_7d/14d/30d/90d, days_since_last_checkin, checkin_trend, avg_weekly_90d, checkin_consistency, weekend_ratio | Attendance decay patterns |
| `XGB_fin` (3) | avg_monthly_payment_90d, payment_regularity, has_open_receivable | Payment health (always populated for REGULAR) |
| `XGB_tenure` (7) | tenure_days, current_spell_duration, contracts_in_spell, total_previous_spells, total_previous_churns, days_until_contract_end, contract_expiring_30d | Lifecycle position + contract timing |
| `XGB_context` (7) | peak_hour_ratio, visited_other_branch, month_of_year, is_resolution_signup, idade, gender, had_segment_migration | Context, seasonality, demographics |

**Why LogReg as meta-learner:** Naturally calibrated outputs, prevents L1 overfitting on 4-7 inputs, coefficients show which specialist matters most.

**3 passthrough features to L1:** days_since_last_checkin, days_until_contract_end, checkin_trend (highest expected SHAP impact).

### 4.3 Training Strategy: Walk-Forward Temporal Validation

Never use random cross-validation for time-series churn data.

```
FOLD 1: Train [2025-03..2025-08] -> Validate [2025-09..2025-10]
FOLD 2: Train [2025-03..2025-10] -> Validate [2025-11..2025-12]
FOLD 3: Train [2025-03..2025-12] -> Validate [2026-01..2026-01]
Final:  Train [2025-03..2026-01] (all data)
```

Validation window auto-adapts: sized to ensure >= 200 churn events per fold.

L0 specialists trained on training set, scored on validation set.
L1 meta-learner trained on out-of-fold L0 predictions (prevents L0->L1 leakage).

### 4.4 Handling Imbalance

| Technique | Where | How |
|-----------|-------|-----|
| `scale_pos_weight` | L0 XGBoost | `count_negative / count_positive` (~10-19x) |
| Threshold tuning | Post-calibration | Optimize on validation set |
| Stratified sampling | Walk-forward folds | Each fold has representative churn rate |

**Not using SMOTE:** Synthetic gym members have unrealistic feature combinations.
**Not using Focal Loss:** At 10-20:1 with XGBoost, scale_pos_weight is simpler and equivalent.

### 4.5 Calibration

```
Raw L1 output --> Platt Scaling --> Calibrated P(churn)
                  (on validation)        |
                                    Risk Tier assignment
                                    HIGH > 0.70
                                    MEDIUM 0.40-0.70
                                    LOW < 0.40
```

Validation: reliability diagram (predicted probability vs observed churn rate in bins).

### 4.6 SHAP: Plain-Language Explanations

SHAP TreeExplainer runs at scoring time. Top 3 features by |SHAP value| are translated to plain Portuguese:

| Feature | Template |
|---------|----------|
| `days_since_last_checkin` | "Sem check-in ha {value} dias (media da academia: {avg} dias)" |
| `checkin_trend` | "Frequencia caiu {pct}% nas ultimas 2 semanas" |
| `days_until_contract_end` | "Contrato expira em {value} dias" |
| `contract_expiring_30d` | "Contrato expira nos proximos 30 dias" |
| `has_open_receivable` | "Possui parcela em aberto" |
| `checkins_last_30d` | "Apenas {value} check-ins nos ultimos 30 dias (media: {avg})" |
| `total_previous_churns` | "Ja cancelou {value} vez(es) antes" |
| `payment_regularity` | "Regularidade de pagamento: {pct}% (abaixo da media)" |
| `avg_monthly_payment_90d` | "Ticket medio mensal: R${value}" |
| `checkin_consistency` | "Frequencia irregular (variacao de {value} dias entre visitas)" |
| `tenure_days` | "Membro ha apenas {months} meses (periodo critico)" |
| `peak_hour_ratio` | "Treina {pct}% no horario de pico (possivel insatisfacao com lotacao)" |

### 4.7 Evaluation: Two Layers

**For the ML engineer (internal, never shown to managers):**

| Metric | Target | Purpose |
|--------|--------|---------|
| PR-AUC | > 0.45 | Primary: precision-recall for imbalanced data |
| Precision @ top 20% | > 0.50 | "Top 20% riskiest: are >50% real churners?" |
| Brier Score | < 0.10 | Calibration quality |
| ROC-AUC | > 0.80 | Literature comparison |

**For the gym manager (monthly outcome report):**

```
RESULTADO DO MES ANTERIOR
+---------------------------------------------------------+
| ALTO RISCO (19 alunos identificados)                    |
|   12 cancelaram (modelo acertou)                        |
|    5 foram recuperados apos contato                     |
|    2 continuam ativos sem contato                       |
|                                                          |
| MEDIO RISCO (43 alunos identificados)                   |
|   8 cancelaram (pioraram - modelo detectou tendencia)   |
|  28 melhoraram (sairam da zona de risco)                |
|   7 continuam em risco medio                            |
|                                                          |
| BAIXO RISCO (398 alunos)                                |
|   3 cancelaram (modelo nao previu)                      |
| 395 continuam ativos                                    |
+---------------------------------------------------------+
```

### 4.8 Outcome Tracking Loop

```
DAY 1:    Score all REGULAR members -> ml.churn_predictions
          Copy to ml.churn_predictions_history

DAY 1-30: Manager sees risk list, executes playbooks
          Playbook execution logged in ml.playbook_executions

DAY 31:   Monitoring job checks each prediction:
          - Did the member actually churn? (query mv_churn_events)
          - Was a playbook executed? (query playbook_executions)
          - Did risk tier change? (compare with latest score)
          -> UPDATE churn_predictions_history SET
             actual_churned, outcome_category, outcome_verified_at

MONTHLY:  Aggregate outcomes into manager-facing report
          Compare hit rates across months (detect drift)
          If hit rate drops below threshold -> trigger retraining
```

---

## 5. Deployment & MLOps

> **STATUS: PENDING — Next section to design**

Planned topics:
- Airflow DAG design (daily scoring pipeline)
- Shadow Mode deployment
- Feature Drift detection (PSI)
- Concept Drift detection (actual vs predicted)
- Circuit Breakers (data quality gates)
- CI/CD pipeline for automated retraining
- Upgrade path to event-driven / near-real-time

---

## 6. Trade-off Analysis

> **STATUS: PENDING**

Planned topics:
- Precision vs Recall in low-margin gym environment
- Cost of false positives (unnecessary discounts) vs false negatives (missed churns)
- When to retrain vs when to recalibrate

---

## 7. Frontend & Playbooks

> **STATUS: PENDING**

Planned topics:
- Gym Manager dashboard design (Lovable)
- Risk overview by tier with outcome tracking
- Member detail with SHAP explanations
- Playbook system (intervention recipes)
- Monthly outcome report view

---

## 8. Decisions Log

| # | Date | Decision | Rationale |
|---|------|----------|-----------|
| D1 | 2026-02-12 | Aggregator classification: 2-level hierarchy | Name-only misses members with regular plan names but aggregator IDs |
| D2 | 2026-02-12 | "Diaria validada com sucesso" = AGREGADOR | Daily pass validation without platform = aggregator access |
| D3 | 2026-02-12 | Revenue from `mv_receivables_normalized` | `value_next_month` is 100% NULL (P2) |
| D4 | 2026-02-12 | Aggregator revenue = R$0 | B2B pass-through not in database |
| D5 | 2026-02-12 | Create new MVs from scratch | `mv_regular_spells` doesn't segregate by segment |
| D6 | 2026-02-19 | Approach A: dbt + Offline Scoring + Azure Functions | Matches existing infra, avoids over-engineering |
| D7 | 2026-02-19 | Daily batch scoring (not real-time) | Aligns with existing morning pipeline cadence |
| D8 | 2026-02-19 | Multi-horizon training (spell_end -30d, -15d, 0d) | Model learns "cooling off" gradient for early detection |
| D9 | 2026-02-19 | Behavioral churn threshold = 10 days absent | Filters normal breaks, catches disengagement early |
| D10 | 2026-02-19 | Drop `uses_personal_trainer` feature | Cannot identify from available data |
| D11 | 2026-02-19 | No artificial balancing; use scale_pos_weight | Preserve natural prevalence |
| D12 | 2026-02-19 | Inertia filter: exclude < 30 days since registration | Newcomer behavior pollutes patterns |
| D13 | 2026-02-19 | **REGULAR members only** in churn model | Aggregators have different economics (R$0), different churn drivers |
| D14 | 2026-02-19 | Retroactive outcome tracking with outcome categories | Manager trust, model drift detection, playbook ROI measurement |
| D15 | 2026-02-19 | Manager sees outcomes by tier (churned/recovered/improved/worsened) | Plain-language feedback, no ML metrics shown to managers |
| D16 | 2026-02-19 | Walk-forward validation auto-adapts window size | Ensures >= 200 churn events per fold regardless of data volume |

---

## 9. Open Questions

| # | Question | Status | Impact |
|---|----------|--------|--------|
| Q1 | Peak hour definition: is 17:00-20:00 correct? | **OPEN** | Affects `peak_hour_ratio` feature |
| Q2 | How many active REGULAR members per branch? | **OPEN** | Scoring pipeline sizing |
| Q3 | Validation query results from existing MVs | **BLOCKING** | Must verify data layer before ML |
| Q4 | Azure Function endpoints — parameterized SQL done? | **OPEN** | API layer design |
| Q5 | What is the actual monthly churn count for REGULAR? | **BLOCKING** | Walk-forward window sizing |

---

## 10. Appendix: P1-P5 Issue Resolution

| Issue | Description | Resolution |
|-------|-------------|------------|
| **P1** | Aggregator classification incomplete | 2-level hierarchy: name + gympass_id/entries |
| **P2** | `value_next_month` 100% NULL | Revenue from `mv_receivables_normalized` |
| **P3** | Spells mix regular + aggregator | `mv_spells_v2` partitions by segment |
| **P4** | Branch 149 missing | Included in MVP branch list |
| **P5** | SQL injection in Azure Functions | Parameterized queries in all new endpoints |

---

## How to Resume in New Conversation

If context window is exhausted:

> "I'm continuing the SkyFit Churn Prediction System design.
> Read `docs/plans/2026-02-19-churn-prediction-system-design.md` for full context.
> Sections 1-4 are approved. Continue with Section 5: Deployment & MLOps.
> Key constraint: REGULAR members only. No aggregators in the model.
> Data validation results pending — check sql/validation/ for queries to run."
