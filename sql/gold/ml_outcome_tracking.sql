-- ============================================================================
-- SKYFIT ML — OUTCOME TRACKING & MANAGER REPORTS
-- ============================================================================
-- Purpose: Track prediction outcomes retroactively (30 days after scoring)
--          and generate manager-facing monthly outcome reports.
--
-- This is the "feedback loop" that builds manager trust and detects model drift.
--
-- Data de criacao: 2026-02-19
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS ml;

-- ============================================================================
-- PLAYBOOK EXECUTION LOG
-- ============================================================================
-- Tracks when a manager acts on a prediction (executes a playbook step).
-- Used to distinguish "recovered" from "false positive" in outcome analysis.
-- ============================================================================

DROP TABLE IF EXISTS ml.playbook_executions CASCADE;

CREATE TABLE ml.playbook_executions (
    execution_id        BIGSERIAL PRIMARY KEY,
    member_id           BIGINT NOT NULL,
    branch_id           INT NOT NULL,
    playbook_id         TEXT NOT NULL,
    step_number         INT NOT NULL,
    action_type         TEXT NOT NULL,  -- 'whatsapp', 'email', 'call', 'offer', 'push'
    executed_at         TIMESTAMP NOT NULL DEFAULT NOW(),
    executed_by         TEXT,           -- manager name or 'system' for automated
    notes               TEXT,           -- manager can add context
    prediction_date     DATE NOT NULL   -- links back to the prediction that triggered this
);

CREATE INDEX IF NOT EXISTS idx_pe_member_date
ON ml.playbook_executions (member_id, prediction_date);

CREATE INDEX IF NOT EXISTS idx_pe_branch_date
ON ml.playbook_executions (branch_id, executed_at);


-- ============================================================================
-- OUTCOME VERIFICATION PROCEDURE
-- ============================================================================
-- Run daily for predictions that are 30+ days old and not yet verified.
-- Checks mv_churn_events and current member status to determine outcome.
-- ============================================================================

-- This query updates predictions_history with actual outcomes.
-- Run as part of the daily Airflow DAG, AFTER mv refreshes.

/*
-- OUTCOME VERIFICATION QUERY
-- Run for predictions from 30-35 days ago (batch window)
WITH predictions_to_verify AS (
    SELECT
        ph.prediction_id,
        ph.member_id,
        ph.branch_id,
        ph.score_date,
        ph.risk_tier,
        ph.churn_probability,
        ph.model_version
    FROM ml.churn_predictions_history ph
    WHERE ph.actual_churned IS NULL                    -- not yet verified
      AND ph.score_date <= (CURRENT_DATE - 30)         -- 30+ days have passed
      AND ph.score_date >= (CURRENT_DATE - 35)         -- batch window (5 days)
),

-- Check if the member actually churned after the prediction
actual_outcomes AS (
    SELECT
        p.prediction_id,
        p.member_id,
        p.score_date,
        p.risk_tier,
        -- Did a churn event occur with churn_confirmed_date after score_date?
        CASE
            WHEN EXISTS (
                SELECT 1 FROM analytics.mv_churn_events ce
                WHERE ce.member_id = p.member_id
                  AND ce.segmento = 'REGULAR'
                  AND ce.evento = 'CHURN'
                  AND ce.churn_confirmed_date > p.score_date
                  AND ce.churn_confirmed_date <= (p.score_date + 60)  -- within 60 days
            ) THEN TRUE
            ELSE FALSE
        END AS actually_churned,
        -- Was a playbook executed for this member after this prediction?
        EXISTS (
            SELECT 1 FROM ml.playbook_executions pe
            WHERE pe.member_id = p.member_id
              AND pe.prediction_date = p.score_date
        ) AS playbook_was_executed,
        -- What is the member's current risk tier (latest prediction)?
        (
            SELECT cp.risk_tier
            FROM ml.churn_predictions cp
            WHERE cp.member_id = p.member_id
            ORDER BY cp.scored_at DESC
            LIMIT 1
        ) AS current_risk_tier
    FROM predictions_to_verify p
)

UPDATE ml.churn_predictions_history ph
SET
    actual_churned = ao.actually_churned,
    outcome_verified_at = CURRENT_DATE,
    outcome_category = CASE
        -- Predicted HIGH/MEDIUM risk, actually churned
        WHEN ao.actually_churned AND ph.risk_tier IN ('HIGH', 'MEDIUM')
        THEN 'TRUE_POSITIVE'

        -- Predicted LOW risk, stayed active
        WHEN NOT ao.actually_churned AND ph.risk_tier = 'LOW'
        THEN 'TRUE_NEGATIVE'

        -- Predicted HIGH/MEDIUM risk, did NOT churn, playbook was executed
        WHEN NOT ao.actually_churned AND ph.risk_tier IN ('HIGH', 'MEDIUM')
             AND ao.playbook_was_executed
        THEN 'RECOVERED'

        -- Predicted HIGH/MEDIUM risk, did NOT churn, no playbook executed
        WHEN NOT ao.actually_churned AND ph.risk_tier IN ('HIGH', 'MEDIUM')
             AND NOT ao.playbook_was_executed
        THEN 'FALSE_POSITIVE'

        -- Predicted LOW risk, actually churned (model missed it)
        WHEN ao.actually_churned AND ph.risk_tier = 'LOW'
        THEN 'FALSE_NEGATIVE'

        ELSE 'UNKNOWN'
    END,
    -- Track tier movement
    tier_movement = CASE
        WHEN ao.current_risk_tier IS NULL THEN 'CHURNED'
        WHEN ao.current_risk_tier = ph.risk_tier THEN 'STABLE'
        WHEN ao.current_risk_tier = 'LOW' AND ph.risk_tier IN ('HIGH', 'MEDIUM') THEN 'IMPROVED'
        WHEN ao.current_risk_tier IN ('HIGH', 'MEDIUM') AND ph.risk_tier = 'LOW' THEN 'WORSENED'
        WHEN ao.current_risk_tier = 'HIGH' AND ph.risk_tier = 'MEDIUM' THEN 'WORSENED'
        WHEN ao.current_risk_tier = 'MEDIUM' AND ph.risk_tier = 'HIGH' THEN 'IMPROVED'
        ELSE 'STABLE'
    END
FROM actual_outcomes ao
WHERE ph.prediction_id = ao.prediction_id;
*/


-- ============================================================================
-- MANAGER MONTHLY OUTCOME REPORT QUERY
-- ============================================================================
-- This query generates the monthly outcome report shown to gym managers.
-- Run for a specific branch and month.
-- ============================================================================

/*
-- MONTHLY OUTCOME REPORT
-- Parameters: {branch_id}, {report_month} (e.g., '2026-01-01')
WITH month_predictions AS (
    SELECT
        ph.member_id,
        ph.risk_tier,
        ph.churn_probability,
        ph.actual_churned,
        ph.outcome_category,
        ph.tier_movement
    FROM ml.churn_predictions_history ph
    WHERE ph.branch_id = {branch_id}
      AND ph.score_date >= {report_month}::DATE
      AND ph.score_date < ({report_month}::DATE + INTERVAL '1 month')
      AND ph.outcome_verified_at IS NOT NULL
    -- Use the FIRST prediction of the month for each member (avoid duplicates)
    AND ph.prediction_id = (
        SELECT MIN(ph2.prediction_id)
        FROM ml.churn_predictions_history ph2
        WHERE ph2.member_id = ph.member_id
          AND ph2.branch_id = ph.branch_id
          AND ph2.score_date >= {report_month}::DATE
          AND ph2.score_date < ({report_month}::DATE + INTERVAL '1 month')
    )
)

SELECT
    risk_tier,
    COUNT(*) AS total_members,
    -- How many actually churned
    COUNT(*) FILTER (WHERE actual_churned = TRUE) AS churned,
    -- How many were recovered (playbook executed, didn't churn)
    COUNT(*) FILTER (WHERE outcome_category = 'RECOVERED') AS recovered,
    -- How many stayed without intervention
    COUNT(*) FILTER (WHERE outcome_category = 'FALSE_POSITIVE') AS stayed_no_contact,
    COUNT(*) FILTER (WHERE outcome_category = 'TRUE_NEGATIVE') AS stayed_low_risk,
    -- How many the model missed
    COUNT(*) FILTER (WHERE outcome_category = 'FALSE_NEGATIVE') AS missed_churns,
    -- Tier movement
    COUNT(*) FILTER (WHERE tier_movement = 'IMPROVED') AS improved,
    COUNT(*) FILTER (WHERE tier_movement = 'WORSENED') AS worsened,
    COUNT(*) FILTER (WHERE tier_movement = 'STABLE') AS stable,
    -- Hit rate for this tier
    CASE
        WHEN COUNT(*) > 0
        THEN ROUND(
            COUNT(*) FILTER (WHERE actual_churned = TRUE OR outcome_category = 'RECOVERED')::NUMERIC
            / COUNT(*) * 100,
            1
        )
        ELSE 0
    END AS action_accuracy_pct
FROM month_predictions
GROUP BY risk_tier
ORDER BY
    CASE risk_tier WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 WHEN 'LOW' THEN 3 END;
*/


-- ============================================================================
-- Add outcome columns to predictions_history (if table already exists)
-- ============================================================================
-- Run this ALTER only if ml.churn_predictions_history was created from
-- ml_churn_predictions.sql BEFORE this file.
-- ============================================================================

/*
ALTER TABLE ml.churn_predictions_history
    ADD COLUMN IF NOT EXISTS outcome_category TEXT,
    ADD COLUMN IF NOT EXISTS tier_movement TEXT;

CREATE INDEX IF NOT EXISTS idx_pred_hist_outcome
ON ml.churn_predictions_history (outcome_category)
WHERE outcome_category IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_pred_hist_verified
ON ml.churn_predictions_history (outcome_verified_at)
WHERE outcome_verified_at IS NOT NULL;
*/
