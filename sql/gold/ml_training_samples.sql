-- ============================================================================
-- SKYFIT ML — FEATURE STORE: POINT-IN-TIME TRAINING SAMPLES
-- ============================================================================
-- Purpose: Generate training data for churn prediction with strict
--          point-in-time correctness (no data leakage).
--
-- SCOPE: REGULAR members ONLY. Aggregators excluded from model.
--
-- Each row = one member at one reference_date, with features computed
-- using ONLY data available at that reference_date.
--
-- Positive samples: 3 per REGULAR churn event (spell_end, spell_end-15d, spell_end-30d)
-- Negative samples: monthly snapshots of active REGULAR members
--
-- Anti-leakage rules:
--   - All features use WHERE date <= reference_date
--   - Target (churned_in_30d) uses dates AFTER reference_date
--   - total_previous_churns uses churn_confirmed_date < reference_date
--   - Negative samples only where member stayed 30+ more days (verified)
--
-- V1 (D19): Extended to 2024+ for broader temporal coverage.
--   Training starts 2024-03-01 (after inertia period).
--   Test period covers 2025-H2 (Jul-Dec 2025).
--
-- D17: Monthly contracts auto-renew every ~30 days.
--   contract_expiring_30d is always True for active monthly members.
--   days_since_last_payment is the primary financial signal.
--
-- D18: Non-payment -> default -> turnstile blocked.
--   is_defaulter = has_open_receivable AND days_since_last_payment > 30.
--   Derived in Python (data_loader.py) from SQL features.
--
-- Data de criacao: 2026-02-19
-- Atualizado: 2026-02-19 (V1 -- D17, D18, D19)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS ml;

DROP TABLE IF EXISTS ml.training_samples CASCADE;

-- ============================================================================
-- STEP 1: Generate reference dates for all training samples
-- ============================================================================
-- This CTE produces (member_id, reference_date, churned_in_30d, label_type,
-- prediction_horizon) for every sample we want to generate.
-- ============================================================================

CREATE TABLE ml.training_samples AS
WITH
-- -----------------------------------------------------------------------
-- 1A: POSITIVE SAMPLES — churned members at 3 horizons
-- -----------------------------------------------------------------------
positive_samples AS (
    SELECT
        ce.member_id,
        ce.branch_id,
        ce.segmento,
        -- Horizon 1: at spell_end (moment of last activity)
        ce.spell_end                          AS reference_date,
        'at_spell_end'                        AS prediction_horizon,
        0                                     AS days_to_event,
        TRUE                                  AS churned_in_30d,
        'CHURN'                               AS label_type,
        ce.spell_start,
        ce.spell_end AS original_spell_end

    FROM analytics.mv_churn_events ce
    WHERE ce.evento = 'CHURN'
      AND ce.segmento = 'REGULAR'             -- REGULAR only (D13)
      -- Ensure spell_end - 30d still has enough history
      AND ce.spell_end >= '2024-03-01'::DATE  -- V1 (D19): extended to 2024

    UNION ALL

    SELECT
        ce.member_id,
        ce.branch_id,
        ce.segmento,
        -- Horizon 2: 15 days before spell_end
        (ce.spell_end - INTERVAL '15 days')::DATE AS reference_date,
        '15d_before'                          AS prediction_horizon,
        15                                    AS days_to_event,
        TRUE                                  AS churned_in_30d,
        'CHURN'                               AS label_type,
        ce.spell_start,
        ce.spell_end AS original_spell_end

    FROM analytics.mv_churn_events ce
    WHERE ce.evento = 'CHURN'
      AND ce.segmento = 'REGULAR'             -- REGULAR only (D13)
      AND ce.spell_end >= '2024-03-01'::DATE  -- V1 (D19): extended to 2024
      -- Ensure reference_date is AFTER spell_start (member was active)
      AND (ce.spell_end - INTERVAL '15 days')::DATE >= ce.spell_start

    UNION ALL

    SELECT
        ce.member_id,
        ce.branch_id,
        ce.segmento,
        -- Horizon 3: 30 days before spell_end
        (ce.spell_end - INTERVAL '30 days')::DATE AS reference_date,
        '30d_before'                          AS prediction_horizon,
        30                                    AS days_to_event,
        TRUE                                  AS churned_in_30d,
        'CHURN'                               AS label_type,
        ce.spell_start,
        ce.spell_end AS original_spell_end

    FROM analytics.mv_churn_events ce
    WHERE ce.evento = 'CHURN'
      AND ce.segmento = 'REGULAR'             -- REGULAR only (D13)
      AND ce.spell_end >= '2024-03-01'::DATE  -- V1 (D19): extended to 2024
      -- Ensure reference_date is AFTER spell_start (member was active)
      AND (ce.spell_end - INTERVAL '30 days')::DATE >= ce.spell_start
),

-- -----------------------------------------------------------------------
-- 1B: NEGATIVE SAMPLES — active members, monthly snapshots
-- -----------------------------------------------------------------------
-- For each active spell, take 1st-of-month snapshots where the member
-- was active AND remained active for 30+ more days after.
-- -----------------------------------------------------------------------
active_months AS (
    SELECT generate_series(
        '2024-03-01'::DATE,  -- V1 (D19): extended to 2024
        -- Stop 30 days before data cutoff to ensure verifiable labels
        ('2026-02-10'::DATE - INTERVAL '30 days')::DATE,
        '1 month'::INTERVAL
    )::DATE AS snapshot_date
),

negative_samples AS (
    SELECT
        s.member_id,
        s.branch_id,
        s.segmento,
        am.snapshot_date                      AS reference_date,
        'active_snapshot'                     AS prediction_horizon,
        NULL::INT                             AS days_to_event,
        FALSE                                 AS churned_in_30d,
        'ACTIVE'                              AS label_type,
        s.spell_start,
        s.spell_end AS original_spell_end

    FROM analytics.mv_spells_v2 s
    CROSS JOIN active_months am
    WHERE
        -- REGULAR only (D13)
        s.segmento = 'REGULAR'
        -- Spell was active at snapshot_date
        AND s.spell_start <= am.snapshot_date
        AND s.spell_end >= am.snapshot_date
        -- Member stayed active for 30+ more days (verifiable label)
        AND s.spell_end >= (am.snapshot_date + INTERVAL '30 days')::DATE
        -- Inertia filter: member registered 30+ days before snapshot
        -- (avoid erratic newcomer behavior)
        AND am.snapshot_date >= (
            SELECT (m.register_date::DATE + INTERVAL '30 days')::DATE
            FROM core.evo_members m
            WHERE m.member_id = s.member_id
        )
),

-- -----------------------------------------------------------------------
-- 2: UNION all samples
-- -----------------------------------------------------------------------
all_samples AS (
    SELECT * FROM positive_samples
    UNION ALL
    SELECT * FROM negative_samples
),

-- -----------------------------------------------------------------------
-- 3: COMPUTE FEATURES for each (member_id, reference_date)
-- -----------------------------------------------------------------------
-- Each feature uses ONLY data available at reference_date.
-- -----------------------------------------------------------------------

-- 3A: Tenure features
tenure_features AS (
    SELECT
        s.member_id,
        s.reference_date,
        -- tenure_days: days since registration
        (s.reference_date - m.register_date::DATE) AS tenure_days,
        -- current_spell_duration_days
        (s.reference_date - s.spell_start) AS current_spell_duration_days,
        -- contracts_in_current_spell: contracts that started before reference
        (
            SELECT COUNT(*)
            FROM analytics.mv_contract_classified cc
            WHERE cc.member_id = s.member_id
              AND cc.segmento = s.segmento
              AND cc.start_date <= s.reference_date
              AND cc.data_efetiva_fim >= s.spell_start
        ) AS contracts_in_current_spell,
        -- total_previous_spells
        (
            SELECT COUNT(*)
            FROM analytics.mv_spells_v2 sp
            WHERE sp.member_id = s.member_id
              AND sp.spell_end < s.spell_start
        ) AS total_previous_spells,
        -- total_previous_churns (only confirmed BEFORE reference)
        (
            SELECT COUNT(*)
            FROM analytics.mv_churn_events ce
            WHERE ce.member_id = s.member_id
              AND ce.evento = 'CHURN'
              AND ce.churn_confirmed_date < s.reference_date
        ) AS total_previous_churns

    FROM all_samples s
    JOIN core.evo_members m ON s.member_id = m.member_id
),

-- 3B: Frequency features
frequency_features AS (
    SELECT
        s.member_id,
        s.reference_date,
        -- checkins in various windows
        COUNT(*) FILTER (
            WHERE e.entry_date::DATE BETWEEN (s.reference_date - 7) AND s.reference_date
        ) AS checkins_last_7d,
        COUNT(*) FILTER (
            WHERE e.entry_date::DATE BETWEEN (s.reference_date - 14) AND s.reference_date
        ) AS checkins_last_14d,
        COUNT(*) FILTER (
            WHERE e.entry_date::DATE BETWEEN (s.reference_date - 30) AND s.reference_date
        ) AS checkins_last_30d,
        COUNT(*) FILTER (
            WHERE e.entry_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
        ) AS checkins_last_90d,
        -- days_since_last_checkin
        CASE
            WHEN MAX(e.entry_date::DATE) FILTER (
                WHERE e.entry_date::DATE <= s.reference_date
            ) IS NOT NULL
            THEN s.reference_date - MAX(e.entry_date::DATE) FILTER (
                WHERE e.entry_date::DATE <= s.reference_date
            )
            ELSE NULL
        END AS days_since_last_checkin,
        -- checkin_trend: ratio of last 14d vs prior 14d
        CASE
            WHEN COUNT(*) FILTER (
                WHERE e.entry_date::DATE BETWEEN (s.reference_date - 28) AND (s.reference_date - 15)
            ) > 0
            THEN COUNT(*) FILTER (
                WHERE e.entry_date::DATE BETWEEN (s.reference_date - 14) AND s.reference_date
            )::NUMERIC / COUNT(*) FILTER (
                WHERE e.entry_date::DATE BETWEEN (s.reference_date - 28) AND (s.reference_date - 15)
            )
            ELSE NULL
        END AS checkin_trend,
        -- avg_weekly_checkins_90d
        CASE
            WHEN COUNT(*) FILTER (
                WHERE e.entry_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
            ) > 0
            THEN ROUND(
                COUNT(*) FILTER (
                    WHERE e.entry_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
                )::NUMERIC / (90.0 / 7.0),
                2
            )
            ELSE 0
        END AS avg_weekly_checkins_90d,
        -- weekend_ratio: % of checkins on Sat/Sun in last 90d
        CASE
            WHEN COUNT(*) FILTER (
                WHERE e.entry_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
            ) > 0
            THEN ROUND(
                COUNT(*) FILTER (
                    WHERE e.entry_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
                      AND EXTRACT(DOW FROM e.entry_date) IN (0, 6)
                )::NUMERIC / NULLIF(COUNT(*) FILTER (
                    WHERE e.entry_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
                ), 0),
                3
            )
            ELSE NULL
        END AS weekend_ratio,
        -- peak_hour_ratio: % of checkins during 17:00-20:00 in last 90d
        CASE
            WHEN COUNT(*) FILTER (
                WHERE e.entry_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
            ) > 0
            THEN ROUND(
                COUNT(*) FILTER (
                    WHERE e.entry_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
                      AND EXTRACT(HOUR FROM e.entry_date) BETWEEN 17 AND 19
                )::NUMERIC / NULLIF(COUNT(*) FILTER (
                    WHERE e.entry_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
                ), 0),
                3
            )
            ELSE NULL
        END AS peak_hour_ratio,
        -- visited_other_branch
        BOOL_OR(
            e.branch_id != s.branch_id
            AND e.entry_date::DATE <= s.reference_date
        ) AS visited_other_branch

    FROM all_samples s
    LEFT JOIN core.evo_entries e
        ON e.member_id = s.member_id
       AND e.entry_type = 'Controle de acesso'
       AND e.entry_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
    GROUP BY s.member_id, s.reference_date, s.branch_id
),

-- 3C: Checkin consistency (stddev of days between checkins, last 90d)
checkin_gaps AS (
    SELECT
        s.member_id,
        s.reference_date,
        e.entry_date::DATE AS checkin_date,
        LAG(e.entry_date::DATE) OVER (
            PARTITION BY s.member_id, s.reference_date
            ORDER BY e.entry_date::DATE
        ) AS prev_checkin_date
    FROM all_samples s
    JOIN core.evo_entries e
        ON e.member_id = s.member_id
       AND e.entry_type = 'Controle de acesso'
       AND e.entry_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
),

consistency_features AS (
    SELECT
        member_id,
        reference_date,
        ROUND(STDDEV(checkin_date - prev_checkin_date)::NUMERIC, 2) AS checkin_consistency
    FROM checkin_gaps
    WHERE prev_checkin_date IS NOT NULL
    GROUP BY member_id, reference_date
),

-- 3D: Recency features (contract and payment)
recency_features AS (
    SELECT
        s.member_id,
        s.reference_date,
        -- days_until_contract_end: how many days until current contract expires
        (
            SELECT MIN(cc.end_date - s.reference_date)
            FROM analytics.mv_contract_classified cc
            WHERE cc.member_id = s.member_id
              AND cc.start_date <= s.reference_date
              AND cc.end_date >= s.reference_date
        ) AS days_until_contract_end,
        -- contract_expiring_30d
        EXISTS (
            SELECT 1
            FROM analytics.mv_contract_classified cc
            WHERE cc.member_id = s.member_id
              AND cc.start_date <= s.reference_date
              AND cc.end_date >= s.reference_date
              AND cc.end_date <= (s.reference_date + 30)
        ) AS contract_expiring_30d,
        -- days_since_last_payment
        (
            SELECT s.reference_date - MAX(r.receiving_date::DATE)
            FROM ltv.mv_receivables_normalized r
            WHERE r.member_id = s.member_id
              AND r.receiving_date::DATE <= s.reference_date
              AND r.status_conciliado = 'RECEBIDO'
        ) AS days_since_last_payment

    FROM all_samples s
),

-- 3E: Financial features (revenue from receivables, regular only)
financial_features AS (
    SELECT
        s.member_id,
        s.reference_date,
        -- avg_monthly_payment_90d
        ROUND(
            COALESCE(SUM(r.amount_paid::NUMERIC) FILTER (
                WHERE r.receiving_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
                  AND r.status_conciliado = 'RECEBIDO'
            ), 0) / 3.0,  -- 90 days ~ 3 months
            2
        ) AS avg_monthly_payment_90d,
        -- payment_regularity: ratio of paid vs expected in last 90d
        CASE
            WHEN COUNT(*) FILTER (
                WHERE r.reference_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
            ) > 0
            THEN ROUND(
                COUNT(*) FILTER (
                    WHERE r.reference_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
                      AND r.status_conciliado = 'RECEBIDO'
                )::NUMERIC / NULLIF(COUNT(*) FILTER (
                    WHERE r.reference_date::DATE BETWEEN (s.reference_date - 90) AND s.reference_date
                ), 0),
                3
            )
            ELSE NULL
        END AS payment_regularity,
        -- has_open_receivable
        EXISTS (
            SELECT 1
            FROM ltv.mv_receivables_normalized r2
            WHERE r2.member_id = s.member_id
              AND r2.status_conciliado = 'EM ABERTO'
              AND r2.reference_date::DATE <= s.reference_date
        ) AS has_open_receivable

    FROM all_samples s
    LEFT JOIN ltv.mv_receivables_normalized r
        ON r.member_id = s.member_id
       AND r.branch_id = s.branch_id
    GROUP BY s.member_id, s.reference_date
),

-- 3F: Seasonality + demographic features
demo_features AS (
    SELECT
        s.member_id,
        s.reference_date,
        EXTRACT(MONTH FROM s.reference_date)::INT AS month_of_year,
        EXTRACT(MONTH FROM m.register_date) IN (1, 2) AS is_resolution_signup,
        EXTRACT(YEAR FROM age(s.reference_date, m.birth_date::DATE))::INT AS idade,
        m.gender,
        s.segmento,
        -- had_segment_migration before reference
        EXISTS (
            SELECT 1
            FROM analytics.mv_churn_events ce
            WHERE ce.member_id = s.member_id
              AND ce.evento = 'MIGRACAO'
              AND ce.spell_end < s.reference_date
        ) AS had_segment_migration

    FROM all_samples s
    JOIN core.evo_members m ON s.member_id = m.member_id
)

-- -----------------------------------------------------------------------
-- FINAL ASSEMBLY: Join all feature groups
-- -----------------------------------------------------------------------
SELECT
    -- Sample metadata
    (s.member_id || '_' || s.reference_date::TEXT) AS sample_id,
    s.member_id,
    s.reference_date,
    s.branch_id,
    s.prediction_horizon,
    s.days_to_event,

    -- Tenure features
    t.tenure_days,
    t.current_spell_duration_days,
    t.contracts_in_current_spell,
    t.total_previous_spells,
    t.total_previous_churns,

    -- Frequency features
    COALESCE(f.checkins_last_7d, 0)           AS checkins_last_7d,
    COALESCE(f.checkins_last_14d, 0)          AS checkins_last_14d,
    COALESCE(f.checkins_last_30d, 0)          AS checkins_last_30d,
    COALESCE(f.checkins_last_90d, 0)          AS checkins_last_90d,
    f.days_since_last_checkin,
    f.checkin_trend,
    COALESCE(f.avg_weekly_checkins_90d, 0)    AS avg_weekly_checkins_90d,
    c.checkin_consistency,
    f.weekend_ratio,

    -- Engagement features
    COALESCE(f.peak_hour_ratio, 0)            AS peak_hour_ratio,
    COALESCE(f.visited_other_branch, FALSE)   AS visited_other_branch,

    -- Recency features
    r.days_until_contract_end,
    COALESCE(r.contract_expiring_30d, FALSE)  AS contract_expiring_30d,
    r.days_since_last_payment,

    -- Financial features
    COALESCE(fin.avg_monthly_payment_90d, 0)  AS avg_monthly_payment_90d,
    fin.payment_regularity,
    COALESCE(fin.has_open_receivable, FALSE)  AS has_open_receivable,

    -- Seasonality features
    d.month_of_year,
    COALESCE(d.is_resolution_signup, FALSE)   AS is_resolution_signup,

    -- Demographic features
    d.idade,
    d.gender,

    -- Segment features
    d.segmento,
    COALESCE(d.had_segment_migration, FALSE)  AS had_segment_migration,

    -- Target
    s.churned_in_30d,
    s.label_type

FROM all_samples s
LEFT JOIN tenure_features t
    ON s.member_id = t.member_id AND s.reference_date = t.reference_date
LEFT JOIN frequency_features f
    ON s.member_id = f.member_id AND s.reference_date = f.reference_date
LEFT JOIN consistency_features c
    ON s.member_id = c.member_id AND s.reference_date = c.reference_date
LEFT JOIN recency_features r
    ON s.member_id = r.member_id AND s.reference_date = r.reference_date
LEFT JOIN financial_features fin
    ON s.member_id = fin.member_id AND s.reference_date = fin.reference_date
LEFT JOIN demo_features d
    ON s.member_id = d.member_id AND s.reference_date = d.reference_date;

-- ============================================================================
-- INDEXES
-- ============================================================================

CREATE UNIQUE INDEX IF NOT EXISTS idx_ts_pk
ON ml.training_samples (sample_id);

CREATE INDEX IF NOT EXISTS idx_ts_member
ON ml.training_samples (member_id);

CREATE INDEX IF NOT EXISTS idx_ts_label
ON ml.training_samples (churned_in_30d, label_type);

CREATE INDEX IF NOT EXISTS idx_ts_reference_date
ON ml.training_samples (reference_date);

CREATE INDEX IF NOT EXISTS idx_ts_branch
ON ml.training_samples (branch_id);

-- ============================================================================
-- VALIDATION QUERIES
-- ============================================================================

/*
-- VAL_FS_1: Sample distribution
SELECT
    label_type,
    prediction_horizon,
    COUNT(*) AS total_samples,
    COUNT(DISTINCT member_id) AS unique_members,
    ROUND(AVG(CASE WHEN churned_in_30d THEN 1 ELSE 0 END)::NUMERIC * 100, 2) AS churn_rate_pct
FROM ml.training_samples
GROUP BY label_type, prediction_horizon
ORDER BY label_type, prediction_horizon;

-- VAL_FS_2: Feature completeness (NULL rates)
SELECT
    'tenure_days' AS feature, ROUND(COUNT(*) FILTER (WHERE tenure_days IS NULL)::NUMERIC / COUNT(*) * 100, 2) AS null_pct
FROM ml.training_samples
UNION ALL
SELECT 'days_since_last_checkin', ROUND(COUNT(*) FILTER (WHERE days_since_last_checkin IS NULL)::NUMERIC / COUNT(*) * 100, 2)
FROM ml.training_samples
UNION ALL
SELECT 'checkin_trend', ROUND(COUNT(*) FILTER (WHERE checkin_trend IS NULL)::NUMERIC / COUNT(*) * 100, 2)
FROM ml.training_samples
UNION ALL
SELECT 'days_until_contract_end', ROUND(COUNT(*) FILTER (WHERE days_until_contract_end IS NULL)::NUMERIC / COUNT(*) * 100, 2)
FROM ml.training_samples
UNION ALL
SELECT 'payment_regularity', ROUND(COUNT(*) FILTER (WHERE payment_regularity IS NULL)::NUMERIC / COUNT(*) * 100, 2)
FROM ml.training_samples
UNION ALL
SELECT 'checkin_consistency', ROUND(COUNT(*) FILTER (WHERE checkin_consistency IS NULL)::NUMERIC / COUNT(*) * 100, 2)
FROM ml.training_samples;

-- VAL_FS_3: Leakage check — churned members should NOT have high recent activity
-- at spell_end. If avg checkins_last_7d for churned members at spell_end is
-- suspiciously high, we may have a leakage issue.
SELECT
    prediction_horizon,
    churned_in_30d,
    ROUND(AVG(checkins_last_7d), 2) AS avg_checkins_7d,
    ROUND(AVG(checkins_last_30d), 2) AS avg_checkins_30d,
    ROUND(AVG(days_since_last_checkin), 1) AS avg_days_inactive
FROM ml.training_samples
WHERE label_type IN ('CHURN', 'ACTIVE')
GROUP BY prediction_horizon, churned_in_30d
ORDER BY prediction_horizon, churned_in_30d;
-- EXPECTED: churned members at spell_end should have LOW checkins and HIGH days_inactive.
-- If churned members at 30d_before still have high checkins, the "cooling off" pattern is captured.

-- VAL_FS_4: Temporal integrity — reference_date should NEVER be after data cutoff
SELECT COUNT(*) AS violations
FROM ml.training_samples
WHERE reference_date > '2026-02-10'::DATE;
-- EXPECTED: 0
*/
