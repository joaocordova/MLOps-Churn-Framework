# Upgrade Path: Daily Batch -> Event-Driven Architecture

**Status:** Documented for future implementation
**Current:** Daily batch scoring via Airflow (4AM)
**Target:** Hybrid — daily batch + event-triggered re-scoring

---

## Why Daily Batch Is Sufficient Today

1. Gym managers plan their day in the morning — they don't need real-time risk scores
2. Existing data pipelines (sales, entries, plans, members) already run each morning
3. A member who checks in today has their score updated the next morning
4. Operational simplicity: one Airflow DAG, one scoring job, one write to DB

## When to Upgrade

Upgrade to event-driven when ANY of these become true:
- SkyFit expands beyond 50 branches (scoring volume makes batch window tight)
- Business requires same-day intervention (e.g., member checks in after 20-day absence -> immediate push notification)
- Integration with real-time marketing tools (e.g., trigger SMS within 1 hour of risk score change)

## What Changes

### Current Architecture (Daily Batch)
```
Morning pipeline refreshes core.* tables
    -> Airflow DAG (4AM)
        -> Refresh analytics.* MVs
        -> Score all active members
        -> Write to ml.churn_predictions
    -> Dashboard shows yesterday's scores
```

### Target Architecture (Hybrid Event-Driven)
```
Morning pipeline refreshes core.* tables
    -> Airflow DAG (4AM) [UNCHANGED]
        -> Full batch scoring (baseline)

PLUS:

Event stream (Azure Event Hubs / Kafka)
    -> Trigger on: new check-in, payment received, contract change
    -> Lightweight re-scoring service (Azure Functions or AKS)
        -> Re-compute ONLY changed features for affected member
        -> Update ml.churn_predictions (upsert)
        -> If risk_tier changed: trigger immediate playbook action
```

### Infrastructure Additions Required

| Component | Purpose | Estimated Cost |
|-----------|---------|---------------|
| Azure Event Hubs (Basic) | Event stream from EVO system | ~$11/month |
| Azure Functions (Consumption) | Event processor + re-scorer | ~$5-20/month |
| Redis Cache (Basic) | Feature cache for fast re-scoring | ~$15/month |

### Code Changes Required

1. **Event Producer:** Modify EVO sync pipeline to emit events to Event Hubs when:
   - New entry in `core.evo_entries` (check-in)
   - New/updated row in `core.evo_member_memberships` (contract change)
   - New payment in `ltv.mv_receivables_normalized`

2. **Event Consumer:** New Azure Function that:
   - Receives event
   - Queries current features for affected member from `mv_member_kpi_base`
   - Runs model inference (load model from blob storage)
   - Upserts `ml.churn_predictions`
   - If risk_tier changed: enqueue playbook action

3. **Feature Cache:** Redis stores the last-computed features per member to avoid hitting PostgreSQL on every event.

4. **Model Serving:** Switch from batch file-based scoring to model-as-a-service:
   - Load model once into Azure Function warm instance
   - Use ONNX runtime for fast inference (~2ms per prediction)

### Migration Strategy

1. Keep daily batch running (unchanged)
2. Deploy event consumer in **shadow mode** (processes events, writes to separate table, does NOT trigger playbooks)
3. Compare event-driven scores vs next-morning batch scores for 30 days
4. If consistency > 95%, enable event-driven playbook triggers
5. Eventually, batch becomes a "reconciliation" job (catches anything events missed)

### What Does NOT Change

- Training pipeline (still monthly retrain on historical data)
- Feature store schema (same features, just computed more frequently)
- Dashboard (still shows latest score, whether from batch or event)
- Playbook definitions (same rules, just triggered faster)
- Monitoring (same drift detection, applied to both pipelines)
