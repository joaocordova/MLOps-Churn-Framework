# SkyFit Churn Prediction System — Design Document

**Date:** 2026-02-19
**Version:** 1.0
**Author:** ML Systems Architecture
**Status:** In Progress (Sections 1-2 approved, 3-6 pending)
**Target:** Senior/Staff ML Engineer technical case (Nubank-level rigor)

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Architecture Overview](#2-system-architecture-overview)
3. [Data Engineering & Feature Store](#3-data-engineering--feature-store)
4. [Model Architecture](#4-model-architecture) — PENDING
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

- **Predicts** which members will churn (30-day inactivity gap) with calibrated probabilities
- **Differentiates** real churn from segment migration (Regular <-> Aggregator)
- **Differentiates** behavioral churn (paying but not attending) from financial churn (contract ended)
- **Explains** predictions to gym managers using plain-language SHAP reasons
- **Acts** through intervention playbooks assigned by risk tier and churn type
- **Scores** daily via batch pipeline aligned with existing morning data refresh

### Scope

- **Branches:** 9 MVP branches [345, 181, 59, 233, 401, 166, 33, 6, 149]
- **Data:** 130M+ records across PostgreSQL/Azure
- **Inference:** Daily batch scoring (overnight Airflow, dashboard refreshes each morning)
- **Frontend:** Lovable-based gym manager dashboard with risk overview, member detail, SHAP explanations, and playbook actions

### Approach Selected

**Approach A: dbt + Offline Scoring + Azure Functions API**

Rationale: Matches existing infrastructure (Azure PostgreSQL, Azure Functions), adds production rigor (dbt tests, data contracts, automated retraining) without over-engineering (no Feast, no Kubernetes). Demonstrates Nubank engineering values: idempotency, data contracts, security-first SQL.

Rejected alternatives:
- **Approach B** (Feast + MLflow + AKS): Over-engineered for 9 branches / ~30K-100K active members. Would be "resume-driven development."
- **Approach C** (Pure SQL + pg_cron): No lineage, no data contracts, no automated testing. Doesn't demonstrate system-level thinking.

---

## 2. System Architecture Overview

### High-Level Architecture

```
+-----------------------------------------------------------------------+
|                    SKYFIT CHURN PREDICTION SYSTEM                      |
+-----------------------------------------------------------------------+
|                                                                         |
|  +--------------- DATA ENGINEERING (dbt) -----------------+           |
|  |                                                         |           |
|  |  BRONZE (core.*)          SILVER (analytics.*)         |           |
|  |  +----------------+     +------------------------+     |           |
|  |  | evo_members    |---->| mv_contract_classified |     |           |
|  |  | evo_memberships|---->| mv_spells_v2           |     |           |
|  |  | evo_entries    |---->| mv_churn_events        |     |           |
|  |  | evo_sales      |     +----------+-------------+     |           |
|  |  | receivables    |                |                    |           |
|  |  +----------------+                v                    |           |
|  |                         GOLD (ml.*)                     |           |
|  |                         +------------------------+      |           |
|  |                         | ml.feature_store       |<-PIT |           |
|  |                         |  (point-in-time)       | corr.|           |
|  |                         +----------+-------------+      |           |
|  +------------------------------|-----+--------------------+           |
|                                 |                                      |
|  +--------------- ML PIPELINE --|--------------------------+           |
|  |                              v                           |           |
|  |  +-------------+  +-------------------+  +------------+ |           |
|  |  | Training    |  | XGBoost Stacking  |  | Calibration| |           |
|  |  | (monthly)   |->| Ensemble          |->| (Platt)    | |           |
|  |  | Walk-forward|  | L0: 4 specialist  |  | + SHAP     | |           |
|  |  | validation  |  | L1: LogReg meta   |  | explainer  | |           |
|  |  +-------------+  +-------------------+  +-----+------+ |           |
|  +---------------------------------------------------+-----+           |
|                                                       |                |
|  +--------------- SCORING & SERVING -----------------+|----+           |
|  |                                                    v     |           |
|  |  +-------------+  +------------------------+            |           |
|  |  | Airflow DAG |  | ml.churn_predictions   |            |           |
|  |  | (daily 4AM) |->|   member_id            |            |           |
|  |  |             |  |   churn_probability     |            |           |
|  |  | 1.Refresh MV|  |   risk_tier (H/M/L)    |            |           |
|  |  | 2.Score all |  |   churn_type            |            |           |
|  |  | 3.Write pred|  |   top_3_reasons (SHAP)  |            |           |
|  |  | 4.Validate  |  |   playbook_id           |            |           |
|  |  +-------------+  +----------+--------------+            |           |
|  +------------------------------|---------------------------+           |
|                                 |                                      |
|  +------------- API LAYER ------|--------------------------+           |
|  |                              v                           |           |
|  |  Azure Functions (parameterized SQL -- P5 fixed)        |           |
|  |  +---------------------------------------------+       |           |
|  |  | GET /api/churn/branch/{id}                  |       |           |
|  |  | GET /api/churn/member/{id}                  |       |           |
|  |  | GET /api/churn/playbook/{tier}              |       |           |
|  |  | GET /api/monitoring/drift                   |       |           |
|  |  +---------------------+-----------------------+       |           |
|  +--------------------------|--------------------------+   |           |
|                              |                             |           |
|  +------------- FRONTEND ---|--------------------------+   |           |
|  |                           v                          |   |           |
|  |  Lovable Dashboard (Gym Manager Persona)            |   |           |
|  |  +---------+ +-------------+ +----------+ +-------+ |   |           |
|  |  | Risk    | | Member      | | "Why"    | |Playbook| |   |           |
|  |  | Overview| | Detail +    | | SHAP     | |Actions | |   |           |
|  |  | by Tier | | Timeline    | | (plain)  | |(recov.)| |   |           |
|  |  +---------+ +-------------+ +----------+ +-------+ |   |           |
|  +------------------------------------------------------+   |           |
|                                                              |           |
|  +------------- MONITORING & MLOps ----------------------+   |           |
|  |  Feature Drift: PSI on check-in frequency weekly       |   |           |
|  |  Concept Drift: Actual vs predicted churn (monthly)    |   |           |
|  |  Circuit Breaker: halt if NULL rate > 5% in features   |   |           |
|  |  Shadow Mode: new model scores alongside prod          |   |           |
|  +--------------------------------------------------------+   |           |
+-----------------------------------------------------------------------+
```

### Key Design Decisions

1. **`mv_member_kpi_base` is NOT the ML feature store** — it's a dashboard snapshot (current state). The ML feature store uses **point-in-time** features to prevent data leakage.
2. **SHAP reasons are pre-computed at scoring time** — stored in `ml.churn_predictions` as JSONB to keep Azure Functions fast.
3. **Playbook assignment is rule-based** on risk tier + churn type + top SHAP reason — deterministic and explainable.
4. **Daily batch aligns with existing morning pipelines** — sales, entries, plans, members already refresh each morning. Upgrade path to event-driven documented separately.

---

## 3. Data Engineering & Feature Store

### 3.1 Existing Data Layer (BUILT)

The following Materialized Views already exist in the `analytics` schema:

| MV | Purpose | Refresh Order |
|----|---------|--------------|
| `mv_contract_classified` | Each contract classified as REGULAR/AGREGADOR, with `data_efetiva_fim` | 1st |
| `mv_spells_v2` | Continuous periods per segment, gap > 30d = new spell | 2nd |
| `mv_churn_events` | Each spell outcome: CHURN / MIGRACAO / ATIVO / INDETERMINADO | 3rd |
| `mv_member_kpi_base` | 1 row per member, all KPIs consolidated (for dashboard) | 4th |

**Business rules encoded:**
- Aggregator classification: Level 1 (plan name) + Level 2 (gympass_id + entries cross-reference)
- "Diaria validada com sucesso" = AGREGADOR
- Churn = 30 days without contract in any segment
- Migration = segment change within <= 30 days (NOT churn)
- Aggregator revenue = R$0 (B2B pass-through not in database)
- Revenue source = `ltv.mv_receivables_normalized` (NOT `value_next_month` which is 100% NULL — P2)

### 3.2 Point-in-Time Feature Store (TO BUILD)

#### Why `mv_member_kpi_base` Cannot Be Used for Training

| `mv_member_kpi_base` | `ml.training_samples` |
|---|---|
| 1 row per member (snapshot today) | N rows per member (one per reference_date) |
| Uses `2026-02-10` hardcoded cutoff | Each sample has its own temporal window |
| `dias_sem_checkin` uses today | `days_since_last_checkin` uses reference_date |
| `total_churns` includes ALL churns | `total_previous_churns` only counts those BEFORE reference |
| Good for dashboards | Good for ML training |

**Note:** `mv_member_kpi_base` IS used for **inference** (daily scoring of current state), not for **training**.

#### Multi-Horizon Training Samples

For each churn event, we generate 3 training samples at different prediction horizons:

```
Timeline for a CHURN case:
------------------------------------------------------------->
|              |              |              |              |
spell_end-30d  spell_end-15d  spell_end     spell_end+30d
|              |              |              |
SAMPLE 1       SAMPLE 2       SAMPLE 3       CHURN
label=TRUE     label=TRUE     label=TRUE     CONFIRMED
"early         "mid           "late
 warning"       warning"       warning"
|              |              |
Actionable     Actionable     Too late
(30d to act)   (15d to act)   (already gone)
```

Rationale: Training only on `spell_end` teaches the model to recognize members who have already left. Earlier snapshots (spell_end - 30d, spell_end - 15d) teach the model the "cooling off" gradient, giving marketing time to intervene while the member is still attending but disengaging.

#### Training Sample Generation Rules

**Positive samples (churned members):**
- Source: `mv_churn_events` WHERE `evento = 'CHURN'`
- Generate 3 samples per event at: `spell_end`, `spell_end - 15`, `spell_end - 30`
- All features computed as of each respective `reference_date`
- `churned_in_30d = TRUE`

**Negative samples (active members):**
- Source: Active spells from `mv_spells_v2`
- Cadence: Monthly snapshots (1st of each month the spell was active)
- `churned_in_30d = FALSE` (verified: member was still active 30 days later)

**Exclusions:**
- Members with < 30 days since `register_date` at reference_date (erratic newcomer behavior)
- Reference dates within 30 days of data cutoff (can't verify 30-day forward label)
- Migration events (not churn — different behavior)
- Members with zero check-ins ever (never engaged — different problem)

**Imbalance handling:**
- Do NOT artificially balance during sample generation
- Preserve natural prevalence (~5-10% churn rate)
- Handle in the MODEL (scale_pos_weight in XGBoost)
- Evaluate with Precision-Recall AUC, not ROC AUC

#### Feature Store Schema: `ml.training_samples`

**Tenure Features (5):**

| Feature | Type | Logic | Leakage Check |
|---------|------|-------|---------------|
| `tenure_days` | INT | `reference_date - register_date` | Safe: register_date is past |
| `current_spell_duration_days` | INT | Days in current spell as of reference_date | Safe: uses spell_start only |
| `contracts_in_current_spell` | INT | Contracts with `start_date < reference_date` in active spell | Safe |
| `total_previous_spells` | INT | Completed spells before reference_date | Safe |
| `total_previous_churns` | INT | Churn events with `churn_confirmed_date < reference_date` | Safe |

**Frequency Features (9):**

| Feature | Type | Logic | Leakage Check |
|---------|------|-------|---------------|
| `checkins_last_7d` | INT | Check-ins in [ref-7, ref] | Safe |
| `checkins_last_14d` | INT | Check-ins in [ref-14, ref] | Safe |
| `checkins_last_30d` | INT | Check-ins in [ref-30, ref] | Safe |
| `checkins_last_90d` | INT | Check-ins in [ref-90, ref] | Safe |
| `days_since_last_checkin` | INT | `reference_date - MAX(entry_date) WHERE entry_date <= reference_date` | Safe |
| `checkin_trend` | FLOAT | `checkins_last_14d / checkins_prior_14d` ratio (acceleration) | Safe |
| `avg_weekly_checkins_90d` | FLOAT | `checkins_last_90d / (90/7)` | Safe |
| `checkin_consistency` | FLOAT | StdDev of days-between-checkins in last 90d (lower = more regular) | Safe |
| `weekend_ratio` | FLOAT | % of check-ins on Sat/Sun in last 90d | Safe |

**Recency Features (3):**

| Feature | Type | Logic | Leakage Check |
|---------|------|-------|---------------|
| `days_until_contract_end` | INT | Active contract `end_date - reference_date` | Safe: end_date known at signing |
| `contract_expiring_30d` | BOOL | Contract ends within 30 days of reference | Safe |
| `days_since_last_payment` | INT | `reference_date - MAX(receiving_date) WHERE receiving_date <= ref` | Safe |

**Financial Features (3):**

| Feature | Type | Logic | Leakage Check |
|---------|------|-------|---------------|
| `avg_monthly_payment_90d` | FLOAT | Avg payment in [ref-90, ref] from receivables | Safe |
| `payment_regularity` | FLOAT | % of expected payments received in last 90d | Safe |
| `has_open_receivable` | BOOL | Unpaid amount as of reference_date | Safe |

**Engagement Features (2):**

| Feature | Type | Logic | Leakage Check |
|---------|------|-------|---------------|
| `peak_hour_ratio` | FLOAT | % of check-ins during 17:00-20:00 in last 90d | Safe |
| `visited_other_branch` | BOOL | Any entry at branch_id != home branch before ref | Safe |

**Seasonality Features (2):**

| Feature | Type | Logic | Leakage Check |
|---------|------|-------|---------------|
| `month_of_year` | INT (1-12) | Month of reference_date | Safe |
| `is_resolution_signup` | BOOL | `register_date` in January or February | Safe |

**Demographic Features (2):**

| Feature | Type | Logic | Leakage Check |
|---------|------|-------|---------------|
| `idade` | INT | Age at reference_date | Safe |
| `gender` | TEXT | From registration | Safe |

**Segment Features (2):**

| Feature | Type | Logic | Leakage Check |
|---------|------|-------|---------------|
| `segmento` | TEXT | REGULAR/AGREGADOR at reference_date | Safe |
| `had_segment_migration` | BOOL | Any migration event before reference_date | Safe |

**Target & Metadata:**

| Column | Type | Purpose |
|--------|------|---------|
| `sample_id` | PK | `{member_id}_{reference_date}` |
| `member_id` | FK | Member identifier |
| `reference_date` | DATE | The "as of" date for all features |
| `branch_id` | INT | Branch at reference_date |
| `prediction_horizon` | TEXT | `30d_before` / `15d_before` / `at_spell_end` |
| `days_to_event` | INT | 30, 15, or 0 |
| `churned_in_30d` | BOOL | Target label (computed retrospectively) |
| `label_type` | TEXT | `CHURN` / `ACTIVE` / `MIGRATION` (for filtering) |

**Total: 28 features + 8 metadata/target columns**

### 3.3 Churn Type Differentiation

A member can be in four states:

| Behavior | Pays? | Attends? | Label | Playbook Action |
|----------|-------|----------|-------|----------------|
| Active | Yes | Yes | Not at risk | None |
| **Behavioral churn** | Yes | **No** (10+ days absent) | `BEHAVIORAL` | "Come back" — they're still paying |
| **Financial churn** | **No** | No | `FINANCIAL` | Win-back offer — fully disengaged |
| Passive payer | Yes | Sporadic | `AT_RISK` | Engagement nudge |

**How this maps to data:**
- **Behavioral churn**: Active contract (`mv_contract_classified`) BUT `days_since_last_checkin > 10`
- **Financial churn**: 30-day gap in contracts (`mv_churn_events` where evento = 'CHURN')

The 10-day threshold was set based on operational experience: long enough to filter normal breaks (weekends, short trips) but short enough to intervene before the member mentally disengages.

### 3.4 Prediction Output Table: `ml.churn_predictions`

| Column | Type | Purpose |
|--------|------|---------|
| `prediction_id` | PK | Auto-increment |
| `member_id` | FK | Member identifier |
| `scored_at` | TIMESTAMP | When the score was computed |
| `churn_probability` | FLOAT | Calibrated probability (0.0 to 1.0) |
| `risk_tier` | TEXT | `HIGH` (>0.7) / `MEDIUM` (0.4-0.7) / `LOW` (<0.4) |
| `churn_type` | TEXT | `BEHAVIORAL` / `FINANCIAL` / `FULL` (both signals) |
| `top_3_reasons` | JSONB | SHAP explanations in plain language |
| `playbook_id` | FK | Which intervention playbook to apply |
| `days_until_contract_end` | INT | Urgency context for manager |
| `last_checkin_date` | DATE | Quick reference for manager |
| `model_version` | TEXT | Which model produced this score |

### 3.5 dbt Migration Plan

The existing Materialized Views will be migrated to dbt models for:
- **Lineage tracking**: Automatic DAG of dependencies
- **Data contracts**: Schema tests, not_null, accepted_values
- **Idempotency**: dbt models are idempotent by design
- **Documentation**: Auto-generated from schema YAML

Medallion mapping:
- **Bronze (source)**: `core.evo_members`, `core.evo_member_memberships`, `core.evo_entries`, `core.evo_sales`, `ltv.mv_receivables_normalized`
- **Silver (staging)**: `stg_contract_classified`, `stg_spells`, `stg_churn_events`
- **Gold (marts)**: `mart_member_kpi` (dashboard), `mart_training_samples` (ML), `mart_churn_predictions` (serving)

---

## 4. Model Architecture

> **STATUS: PENDING — Next section to design**

Planned topics:
- XGBoost Stacking Ensemble (L0 specialists + L1 meta-learner)
- Handling imbalance (scale_pos_weight vs SMOTE vs Focal Loss)
- Calibration (Platt scaling / Isotonic regression)
- Walk-forward temporal validation
- Why stacking handles SkyFit's feature sparsity

---

## 5. Deployment & MLOps

> **STATUS: PENDING**

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
- SHAP for gym manager explanations
- Business metric vs ML metric alignment
- Cost of false positives (unnecessary discounts) vs false negatives (missed churns)

---

## 7. Frontend & Playbooks

> **STATUS: PENDING**

Planned topics:
- Gym Manager dashboard design (Lovable)
- Risk overview by tier
- Member detail with timeline
- SHAP explanations in plain language
- Playbook system (intervention recipes by churn_type + risk_tier)

---

## 8. Decisions Log

| # | Date | Decision | Rationale | Impact |
|---|------|----------|-----------|--------|
| D1 | 2026-02-12 | Aggregator classification: 2-level hierarchy | Name-only misses members with regular plan names but aggregator IDs + entries | Accurate segment split for spell/churn logic |
| D2 | 2026-02-12 | "Diaria validada com sucesso" = AGREGADOR | User confirmed: daily pass validation without platform = aggregator access | Catches ~117 entries otherwise missed |
| D3 | 2026-02-12 | Revenue from `mv_receivables_normalized` | `value_next_month` is 100% NULL (P2) | Reliable revenue for financial features |
| D4 | 2026-02-12 | Aggregator revenue = R$0 | B2B pass-through not in database (user confirmed) | Financial features only apply to REGULAR members |
| D5 | 2026-02-12 | Create new MVs from scratch (not adapt existing) | `mv_regular_spells` doesn't segregate by segment, covers wrong branches | Clean foundation |
| D6 | 2026-02-19 | Approach A: dbt + Offline Scoring + Azure Functions | Matches existing infra, avoids over-engineering, demonstrates Nubank values | Scope defined |
| D7 | 2026-02-19 | Daily batch scoring (not real-time) | Aligns with existing morning pipeline cadence; managers plan daily | Lower cost, sufficient for use case |
| D8 | 2026-02-19 | Multi-horizon training samples (spell_end - 30d, -15d, 0d) | Model must detect "cooling off" early enough for marketing to act | 3x more positive samples; captures degradation gradient |
| D9 | 2026-02-19 | Behavioral churn threshold = 10 consecutive days absent | Operational experience: long enough to filter normal breaks, short enough to intervene | Drives separate playbook for paying-but-absent members |
| D10 | 2026-02-19 | Drop `uses_personal_trainer` feature | Cannot identify PT sessions from available tables (`evo_entries` doesn't distinguish) | 27 features instead of 28; data integrity over guessing |
| D11 | 2026-02-19 | Negative sampling: monthly snapshots, no artificial balancing | Preserve natural churn prevalence; handle imbalance in model, not data | Avoids survivorship bias; uses scale_pos_weight |
| D12 | 2026-02-19 | Inertia filter: exclude members < 30 days since registration | Newcomer behavior is erratic and pollutes mature churn patterns | Cleaner training signal |

---

## 9. Open Questions

| # | Question | Status | Impact |
|---|----------|--------|--------|
| Q1 | Peak hour definition: is 17:00-20:00 correct for these branches? | **OPEN** | Affects `peak_hour_ratio` feature |
| Q2 | How many active members per branch (approximate)? | **OPEN** | Affects scoring pipeline resource sizing |
| Q3 | Validation query results from `01_validacao_standalone.sql` | **OPEN** | Need to verify MV logic on real data before building ML layer |
| Q4 | Are the Azure Function endpoints already updated with parameterized SQL (P5)? | **OPEN** | Blocks API layer design |

---

## 10. Appendix: P1-P5 Issue Resolution

| Issue | Description | Resolution in This System |
|-------|-------------|--------------------------|
| **P1** | Aggregator classification incomplete (name-only) | 2-level hierarchy: name + gympass_id/entries cross-reference |
| **P2** | `value_next_month` 100% NULL | Revenue from `mv_receivables_normalized`; never reference `value_next_month` |
| **P3** | Spells mix regular + aggregator | `mv_spells_v2` partitions by segment; migration events tracked separately |
| **P4** | Branch 149 missing from default array | Included in MVP branch list: `{345,181,59,233,401,166,33,6,149}` |
| **P5** | SQL injection in Azure Functions (f-strings) | All new endpoints use parameterized queries; existing endpoints flagged for remediation |
