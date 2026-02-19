-- ============================================================================
-- SKYFIT ML — DATA VALIDATION FOR ML READINESS
-- ============================================================================
-- Run AFTER 00_architecture_complete.sql and 01_validacao_standalone.sql pass.
-- These queries validate that the data is clean enough for ML training.
--
-- BLOCKING: Do NOT proceed with model training until all checks pass.
--
-- Data de criacao: 2026-02-19
-- ============================================================================


-- ============================================================================
-- ML_VAL_01: REGULAR CHURN VOLUME PER MONTH
-- ============================================================================
-- PURPOSE: Determine walk-forward validation window size.
--          Need >= 200 churn events per validation fold.
-- WHAT TO REPORT: The full output (all rows).
-- ============================================================================

SELECT
    churn_confirmed_mes,
    COUNT(*)                            AS churns_regular,
    COUNT(DISTINCT member_id)           AS membros_unicos
FROM analytics.mv_churn_events
WHERE evento = 'CHURN'
  AND segmento = 'REGULAR'
  AND churn_confirmed_mes >= '2025-01-01'
  AND churn_confirmed_mes <= '2026-02-01'
GROUP BY churn_confirmed_mes
ORDER BY churn_confirmed_mes;


-- ============================================================================
-- ML_VAL_02: ACTIVE REGULAR MEMBERS PER BRANCH (scoring volume)
-- ============================================================================
-- PURPOSE: Size the daily scoring pipeline.
-- WHAT TO REPORT: The full output.
-- ============================================================================

SELECT
    branch_id,
    branch_name,
    COUNT(*)                            AS active_regular_members,
    ROUND(AVG(dias_sem_checkin), 1)     AS avg_days_since_checkin,
    ROUND(AVG(avg_checkins_per_week), 2) AS avg_weekly_frequency
FROM analytics.mv_member_kpi_base
WHERE is_active = TRUE
  AND segmento_atual = 'REGULAR'
GROUP BY branch_id, branch_name
ORDER BY branch_id;


-- ============================================================================
-- ML_VAL_03: FEATURE AVAILABILITY — CHECK-INS
-- ============================================================================
-- PURPOSE: Verify that entries data is populated enough for frequency features.
--          If a branch has very few entries, frequency features will be sparse.
-- WHAT TO REPORT: Any branch with < 1000 entries in 2025 is concerning.
-- ============================================================================

SELECT
    e.branch_id,
    COUNT(*)                                        AS total_entries_2025,
    COUNT(DISTINCT e.member_id)                     AS members_with_entries,
    COUNT(DISTINCT e.entry_date::DATE)              AS distinct_days,
    MIN(e.entry_date::DATE)                         AS earliest_entry,
    MAX(e.entry_date::DATE)                         AS latest_entry,
    -- Check for gaps: are there days with zero entries?
    COUNT(DISTINCT e.entry_date::DATE)::NUMERIC /
        GREATEST((MAX(e.entry_date::DATE) - MIN(e.entry_date::DATE)), 1) * 100
                                                    AS coverage_pct
FROM core.evo_entries e
WHERE e.branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[])
  AND e.entry_date >= '2025-01-01'
  AND e.entry_type = 'Controle de acesso'
GROUP BY e.branch_id
ORDER BY e.branch_id;


-- ============================================================================
-- ML_VAL_04: FEATURE AVAILABILITY — RECEIVABLES (REGULAR only)
-- ============================================================================
-- PURPOSE: Verify that payment data is populated for financial features.
--          Since aggregator = R$0, we only care about REGULAR members.
-- WHAT TO REPORT: Full output. If many REGULAR members have zero receivables,
--                 financial features will be weak.
-- ============================================================================

WITH regular_active AS (
    SELECT member_id, branch_id
    FROM analytics.mv_member_kpi_base
    WHERE segmento_atual = 'REGULAR'
      AND is_active = TRUE
)
SELECT
    ra.branch_id,
    COUNT(DISTINCT ra.member_id)                    AS active_regular,
    COUNT(DISTINCT r.member_id)                     AS with_receivables,
    ROUND(
        COUNT(DISTINCT r.member_id)::NUMERIC /
        NULLIF(COUNT(DISTINCT ra.member_id), 0) * 100, 1
    )                                               AS receivables_coverage_pct,
    ROUND(AVG(r.amount_paid::NUMERIC) FILTER (WHERE r.status_conciliado = 'RECEBIDO'), 2)
                                                    AS avg_payment_amount,
    COUNT(*) FILTER (WHERE r.status_conciliado = 'EM ABERTO')
                                                    AS open_receivables_count
FROM regular_active ra
LEFT JOIN ltv.mv_receivables_normalized r
    ON ra.member_id = r.member_id
   AND r.reference_date >= '2025-01-01'
GROUP BY ra.branch_id
ORDER BY ra.branch_id;


-- ============================================================================
-- ML_VAL_05: NULL RATE IN KEY FEATURES (from mv_member_kpi_base)
-- ============================================================================
-- PURPOSE: If critical features have high NULL rates, the model will struggle.
--          Acceptable: < 5% NULL for most features.
--          Exception: dias_sem_checkin can be NULL for members who never checked in.
-- WHAT TO REPORT: Full output. Flag any feature with > 10% NULL.
-- ============================================================================

SELECT
    COUNT(*) AS total_regular_members,
    -- Frequency
    ROUND(COUNT(*) FILTER (WHERE last_checkin IS NULL)::NUMERIC / COUNT(*) * 100, 2)
        AS pct_null_last_checkin,
    ROUND(COUNT(*) FILTER (WHERE dias_sem_checkin IS NULL)::NUMERIC / COUNT(*) * 100, 2)
        AS pct_null_dias_sem_checkin,
    ROUND(COUNT(*) FILTER (WHERE avg_checkins_per_week IS NULL OR avg_checkins_per_week = 0)::NUMERIC / COUNT(*) * 100, 2)
        AS pct_zero_or_null_frequency,
    -- Financial
    ROUND(COUNT(*) FILTER (WHERE receita_total_paga IS NULL OR receita_total_paga = 0)::NUMERIC / COUNT(*) * 100, 2)
        AS pct_zero_or_null_revenue,
    ROUND(COUNT(*) FILTER (WHERE ticket_medio_mensal IS NULL OR ticket_medio_mensal = 0)::NUMERIC / COUNT(*) * 100, 2)
        AS pct_zero_or_null_ticket,
    -- Demographic
    ROUND(COUNT(*) FILTER (WHERE idade IS NULL)::NUMERIC / COUNT(*) * 100, 2)
        AS pct_null_age,
    ROUND(COUNT(*) FILTER (WHERE gender IS NULL)::NUMERIC / COUNT(*) * 100, 2)
        AS pct_null_gender,
    -- Tenure
    ROUND(COUNT(*) FILTER (WHERE register_date IS NULL)::NUMERIC / COUNT(*) * 100, 2)
        AS pct_null_register_date,
    ROUND(COUNT(*) FILTER (WHERE first_spell_start IS NULL)::NUMERIC / COUNT(*) * 100, 2)
        AS pct_null_first_spell
FROM analytics.mv_member_kpi_base
WHERE segmento_atual = 'REGULAR';


-- ============================================================================
-- ML_VAL_06: CHURN RATE SANITY CHECK — REGULAR ONLY
-- ============================================================================
-- PURPOSE: Verify churn rate is in reasonable range (3-15% monthly for gyms).
--          If > 20%, data quality issue or classification problem.
--          If < 2%, possible under-detection.
-- WHAT TO REPORT: Full output. Flag any month outside 2-20% range.
-- ============================================================================

WITH meses AS (
    SELECT generate_series('2025-01-01'::DATE, '2026-01-01'::DATE, '1 month'::INTERVAL)::DATE AS mes
),
ativos_inicio AS (
    SELECT
        m.mes,
        s.branch_id,
        COUNT(DISTINCT s.member_id) AS ativos
    FROM meses m
    CROSS JOIN analytics.mv_spells_v2 s
    WHERE s.segmento = 'REGULAR'
      AND s.spell_start < m.mes
      AND s.spell_end >= m.mes
    GROUP BY m.mes, s.branch_id
),
churns_mes AS (
    SELECT
        churn_confirmed_mes AS mes,
        branch_id,
        COUNT(*) AS churns
    FROM analytics.mv_churn_events
    WHERE evento = 'CHURN'
      AND segmento = 'REGULAR'
      AND churn_confirmed_mes >= '2025-01-01'
    GROUP BY churn_confirmed_mes, branch_id
)
SELECT
    a.mes,
    a.branch_id,
    a.ativos                                        AS active_start_of_month,
    COALESCE(c.churns, 0)                           AS churns,
    CASE
        WHEN a.ativos > 0
        THEN ROUND(COALESCE(c.churns, 0)::NUMERIC / a.ativos * 100, 2)
        ELSE 0
    END                                             AS churn_rate_pct,
    CASE
        WHEN a.ativos > 0 AND ROUND(COALESCE(c.churns, 0)::NUMERIC / a.ativos * 100, 2) > 20
        THEN 'HIGH — investigate'
        WHEN a.ativos > 0 AND ROUND(COALESCE(c.churns, 0)::NUMERIC / a.ativos * 100, 2) < 2
        THEN 'LOW — possible under-detection'
        ELSE 'OK'
    END                                             AS flag
FROM ativos_inicio a
LEFT JOIN churns_mes c ON a.mes = c.mes AND a.branch_id = c.branch_id
ORDER BY a.branch_id, a.mes;


-- ============================================================================
-- ML_VAL_07: ENTRY_TYPE DISTRIBUTION
-- ============================================================================
-- PURPOSE: Verify that 'Controle de acesso' is the correct filter for check-ins.
--          If there are other entry_type values that represent real visits,
--          we may be undercounting frequency.
-- WHAT TO REPORT: Full output. List all entry_types with counts.
-- ============================================================================

SELECT
    entry_type,
    COUNT(*)                            AS total_entries,
    COUNT(DISTINCT member_id)           AS unique_members,
    MIN(entry_date::DATE)               AS earliest,
    MAX(entry_date::DATE)               AS latest
FROM core.evo_entries
WHERE branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[])
  AND entry_date >= '2025-01-01'
GROUP BY entry_type
ORDER BY total_entries DESC;


-- ============================================================================
-- ML_VAL_08: BLOCK_REASON PATTERNS FOR AGGREGATOR CLASSIFICATION
-- ============================================================================
-- PURPOSE: Verify that the aggregator classification via block_reason is
--          catching the right patterns. Are there new patterns we're missing?
-- WHAT TO REPORT: Full output. Look for any new aggregator-related patterns
--                 not covered by our regex.
-- ============================================================================

SELECT
    block_reason,
    COUNT(*)                            AS occurrences,
    COUNT(DISTINCT member_id)           AS unique_members
FROM core.evo_entries
WHERE branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[])
  AND entry_date >= '2025-01-01'
  AND block_reason IS NOT NULL
GROUP BY block_reason
ORDER BY occurrences DESC
LIMIT 30;


-- ============================================================================
-- ML_VAL_09: MEMBERSHIP_NAME PATTERNS FOR AGGREGATOR CLASSIFICATION
-- ============================================================================
-- PURPOSE: Are there new plan names that contain aggregator keywords we miss?
--          Or regular plans that our regex falsely classifies as aggregator?
-- WHAT TO REPORT: Full output. Look for plans that seem misclassified.
-- ============================================================================

SELECT
    membership_name,
    segmento,
    COUNT(*)                            AS contracts,
    COUNT(DISTINCT member_id)           AS unique_members,
    COUNT(*) FILTER (WHERE membership_status = 'active') AS active_contracts
FROM analytics.mv_contract_classified
GROUP BY membership_name, segmento
ORDER BY contracts DESC
LIMIT 40;


-- ============================================================================
-- ML_VAL_10: TEMPORAL COVERAGE — GAPS IN DATA
-- ============================================================================
-- PURPOSE: Check if there are days with zero entries (data pipeline failures).
--          Gaps in entry data would create false "inactivity" signals for the model.
-- WHAT TO REPORT: Any date with zero entries across all branches.
-- ============================================================================

WITH date_range AS (
    SELECT generate_series('2025-01-01'::DATE, '2026-02-10'::DATE, '1 day'::INTERVAL)::DATE AS dt
),
daily_entries AS (
    SELECT
        entry_date::DATE AS dt,
        COUNT(*) AS entries
    FROM core.evo_entries
    WHERE branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[])
      AND entry_date >= '2025-01-01'
      AND entry_type = 'Controle de acesso'
    GROUP BY entry_date::DATE
)
SELECT
    dr.dt,
    EXTRACT(DOW FROM dr.dt) AS day_of_week,  -- 0=Sun, 6=Sat
    COALESCE(de.entries, 0) AS entries,
    CASE
        WHEN COALESCE(de.entries, 0) = 0 AND EXTRACT(DOW FROM dr.dt) NOT IN (0)
        THEN 'MISSING DATA — not a Sunday'
        WHEN COALESCE(de.entries, 0) = 0 AND EXTRACT(DOW FROM dr.dt) = 0
        THEN 'Zero entries (Sunday — may be normal)'
        WHEN COALESCE(de.entries, 0) < 50
        THEN 'LOW — investigate'
        ELSE 'OK'
    END AS flag
FROM date_range dr
LEFT JOIN daily_entries de ON dr.dt = de.dt
WHERE COALESCE(de.entries, 0) < 50  -- Only show problematic days
ORDER BY dr.dt;


-- ============================================================================
-- ML_VAL_11: BEHAVIORAL CHURN CANDIDATES (10+ days absent, active contract)
-- ============================================================================
-- PURPOSE: Validate the 10-day behavioral churn threshold.
--          How many active REGULAR members have been absent 10+ days?
--          This becomes the BEHAVIORAL churn type in predictions.
-- WHAT TO REPORT: Summary counts + sample of 10 members for manual check.
-- ============================================================================

-- Summary
SELECT
    CASE
        WHEN dias_sem_checkin IS NULL THEN 'Never checked in'
        WHEN dias_sem_checkin <= 3 THEN '0-3 days (active)'
        WHEN dias_sem_checkin <= 7 THEN '4-7 days (normal gap)'
        WHEN dias_sem_checkin <= 10 THEN '8-10 days (watch)'
        WHEN dias_sem_checkin <= 15 THEN '11-15 days (BEHAVIORAL risk)'
        WHEN dias_sem_checkin <= 30 THEN '16-30 days (HIGH BEHAVIORAL risk)'
        ELSE '30+ days (likely already churning)'
    END AS absence_bucket,
    COUNT(*) AS members,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 1) AS pct
FROM analytics.mv_member_kpi_base
WHERE is_active = TRUE
  AND segmento_atual = 'REGULAR'
GROUP BY
    CASE
        WHEN dias_sem_checkin IS NULL THEN 'Never checked in'
        WHEN dias_sem_checkin <= 3 THEN '0-3 days (active)'
        WHEN dias_sem_checkin <= 7 THEN '4-7 days (normal gap)'
        WHEN dias_sem_checkin <= 10 THEN '8-10 days (watch)'
        WHEN dias_sem_checkin <= 15 THEN '11-15 days (BEHAVIORAL risk)'
        WHEN dias_sem_checkin <= 30 THEN '16-30 days (HIGH BEHAVIORAL risk)'
        ELSE '30+ days (likely already churning)'
    END
ORDER BY
    CASE
        WHEN dias_sem_checkin IS NULL THEN 0
        WHEN dias_sem_checkin <= 3 THEN 1
        WHEN dias_sem_checkin <= 7 THEN 2
        WHEN dias_sem_checkin <= 10 THEN 3
        WHEN dias_sem_checkin <= 15 THEN 4
        WHEN dias_sem_checkin <= 30 THEN 5
        ELSE 6
    END;


-- ============================================================================
-- ML_VAL_12: PEAK HOUR DISTRIBUTION
-- ============================================================================
-- PURPOSE: Validate that 17:00-20:00 is the correct peak hour definition.
--          If peak is actually 06:00-08:00 for some branches, we need to adjust.
-- WHAT TO REPORT: Hourly distribution. Identify peak hours per branch.
-- ============================================================================

SELECT
    e.branch_id,
    EXTRACT(HOUR FROM e.entry_date)::INT AS hour_of_day,
    COUNT(*) AS entries
FROM core.evo_entries e
WHERE e.branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[])
  AND e.entry_date >= '2025-01-01'
  AND e.entry_type = 'Controle de acesso'
GROUP BY e.branch_id, EXTRACT(HOUR FROM e.entry_date)::INT
ORDER BY e.branch_id, entries DESC;


-- ============================================================================
-- ML_VAL_13: MIGRATION EVENTS — REGULAR TO AGREGADOR
-- ============================================================================
-- PURPOSE: Understand migration patterns. These members are EXCLUDED from
--          churn training but should be flagged as "MIGRADO" in the dashboard.
-- WHAT TO REPORT: Volume and direction of migrations.
-- ============================================================================

SELECT
    segmento AS from_segment,
    next_spell_segmento AS to_segment,
    COUNT(*) AS migrations,
    ROUND(AVG(gap_any_days), 1) AS avg_gap_days,
    MIN(spell_end) AS earliest_migration,
    MAX(spell_end) AS latest_migration
FROM analytics.mv_churn_events
WHERE evento = 'MIGRACAO'
GROUP BY segmento, next_spell_segmento
ORDER BY migrations DESC;


-- ============================================================================
-- ML_VAL_14: SAMPLE SIZE ESTIMATION
-- ============================================================================
-- PURPOSE: Estimate how many training samples we'll generate.
--          Positive: 3x REGULAR churns (3 horizons)
--          Negative: ~monthly snapshots of active REGULAR members
-- WHAT TO REPORT: Estimated sample counts and churn rate.
-- ============================================================================

WITH churn_count AS (
    SELECT COUNT(*) AS n
    FROM analytics.mv_churn_events
    WHERE evento = 'CHURN'
      AND segmento = 'REGULAR'
      AND churn_confirmed_mes >= '2025-03-01'  -- after inertia period
),
active_months AS (
    SELECT
        COUNT(DISTINCT (s.member_id, DATE_TRUNC('month', dd)::DATE)) AS active_member_months
    FROM analytics.mv_spells_v2 s
    CROSS JOIN generate_series('2025-03-01'::DATE, '2025-12-01'::DATE, '1 month'::INTERVAL) dd
    WHERE s.segmento = 'REGULAR'
      AND s.spell_start <= dd::DATE
      AND s.spell_end >= (dd::DATE + 30)  -- must be active 30d later for label verification
)
SELECT
    cc.n AS regular_churn_events,
    cc.n * 3 AS positive_samples_3_horizons,
    am.active_member_months AS negative_samples_approx,
    cc.n * 3 + am.active_member_months AS total_samples_approx,
    ROUND(
        (cc.n * 3)::NUMERIC / NULLIF(cc.n * 3 + am.active_member_months, 0) * 100,
        2
    ) AS estimated_churn_rate_pct
FROM churn_count cc, active_months am;


-- ============================================================================
-- ML_VAL_15: DATA FRESHNESS
-- ============================================================================
-- PURPOSE: Verify data is recent enough for training.
--          If max entry_date is weeks old, the pipeline may be broken.
-- WHAT TO REPORT: Full output. Flag if any table is > 3 days stale.
-- ============================================================================

SELECT 'evo_entries' AS table_name,
    MAX(entry_date)::DATE AS max_date,
    CURRENT_DATE - MAX(entry_date)::DATE AS days_stale
FROM core.evo_entries
WHERE branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[])

UNION ALL

SELECT 'evo_member_memberships',
    MAX(GREATEST(start_date, COALESCE(cancel_date::DATE, start_date)))::DATE,
    CURRENT_DATE - MAX(GREATEST(start_date, COALESCE(cancel_date::DATE, start_date)))::DATE
FROM core.evo_member_memberships mm
JOIN core.evo_members m ON mm.member_id = m.member_id
WHERE m.branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[])

UNION ALL

SELECT 'mv_receivables_normalized',
    MAX(reference_date)::DATE,
    CURRENT_DATE - MAX(reference_date)::DATE
FROM ltv.mv_receivables_normalized
WHERE branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[])

ORDER BY table_name;
