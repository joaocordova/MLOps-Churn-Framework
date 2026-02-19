-- ============================================================================
-- SKYFIT ANALYTICS — SCRIPT DE VALIDAÇÃO STANDALONE
-- ============================================================================
-- Executar APÓS rodar 00_architecture_complete.sql
-- Cada query é independente — execute uma por vez e confira resultados
-- Data de corte: 2026-02-10
-- ============================================================================


-- ============================================================================
--  VAL_1A: DISTRIBUIÇÃO DE SEGMENTOS POR BRANCH
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ REGULAR deve ser > AGREGADOR na maioria das branches
--   ✓ Não devem existir contratos sem segmento
--   ✓ Soma de contratos deve bater com total na core.evo_member_memberships
-- ============================================================================

-- 1A.1: Resumo por branch e segmento
SELECT
    branch_id,
    branch_name,
    segmento,
    COUNT(*)                                          AS total_contratos,
    COUNT(DISTINCT member_id)                         AS membros_unicos,
    COUNT(*) FILTER (WHERE membership_status = 'active')   AS ativos,
    COUNT(*) FILTER (WHERE membership_status = 'canceled') AS cancelados,
    COUNT(*) FILTER (WHERE membership_status = 'expired')  AS expirados
FROM analytics.mv_contract_classified
GROUP BY branch_id, branch_name, segmento
ORDER BY branch_id, segmento;

-- 1A.2: AUDITORIA — total deve bater com fonte
SELECT
    'mv_contract_classified' AS tabela,
    COUNT(*) AS total_mv
FROM analytics.mv_contract_classified
UNION ALL
SELECT
    'core.evo_member_memberships (branches MVP)' AS tabela,
    COUNT(*) AS total_fonte
FROM core.evo_member_memberships mm
JOIN core.evo_members m ON mm.member_id = m.member_id
WHERE m.branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[]);


-- ============================================================================
--  VAL_1B: CONTRATOS CLASSIFICADOS PELO NÍVEL 2 (entries)
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ Contratos com nome REGULAR mas classificados como AGREGADOR
--   ✓ Se a lista for grande, a regex do nível 1 pode estar incompleta
--   ✓ Verificar se faz sentido (membro com gympass_id + entries de diária)
-- ============================================================================

SELECT
    cc.member_membership_id,
    cc.member_id,
    cc.branch_name,
    cc.membership_name,
    cc.segmento,
    cc.start_date,
    cc.end_date,
    cc.data_efetiva_fim,
    cc.gympass_id IS NOT NULL AS tem_gympass,
    cc.code_totalpass IS NOT NULL AS tem_totalpass
FROM analytics.mv_contract_classified cc
WHERE cc.segmento = 'AGREGADOR'
  AND NOT (LOWER(COALESCE(cc.membership_name, '')) ~ '(gympass|totalpass|wellhub)')
ORDER BY cc.member_id, cc.start_date;


-- ============================================================================
--  VAL_1C: MEMBROS COM TRANSIÇÃO DE SEGMENTO
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ A timeline faz sentido (regular primeiro, depois agregador, ou vice-versa)
--   ✓ Datas são coerentes (não tem sobreposição entre segmentos)
-- ============================================================================

SELECT
    cc.member_id,
    cc.branch_name,
    cc.membership_name,
    cc.segmento,
    cc.start_date,
    cc.data_efetiva_fim,
    cc.membership_status
FROM analytics.mv_contract_classified cc
WHERE cc.member_id IN (
    SELECT member_id
    FROM analytics.mv_contract_classified
    GROUP BY member_id
    HAVING COUNT(DISTINCT segmento) > 1
)
ORDER BY cc.member_id, cc.start_date;


-- ============================================================================
--  VAL_1D: data_efetiva_fim ESTÁ CORRETA?
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ Query deve retornar 0 linhas (zero erros)
--   ✓ Se cancel_date < end_date → data_efetiva_fim = cancel_date
--   ✓ Se cancel_date IS NULL → data_efetiva_fim = end_date
-- ============================================================================

SELECT
    member_membership_id,
    membership_status,
    start_date,
    end_date,
    cancel_date,
    data_efetiva_fim,
    'ERRO' AS check_status
FROM analytics.mv_contract_classified
WHERE
    -- Caso 1: tem cancel_date antes do end_date mas data_efetiva_fim não é cancel_date
    (cancel_date IS NOT NULL AND cancel_date::DATE < end_date AND data_efetiva_fim != cancel_date::DATE)
    OR
    -- Caso 2: sem cancel_date (ou cancel_date >= end_date) mas data_efetiva_fim não é end_date
    ((cancel_date IS NULL OR cancel_date::DATE >= end_date) AND data_efetiva_fim != end_date);
-- ESPERADO: 0 linhas


-- ============================================================================
--  VAL_2A: RESUMO DE SPELLS
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ Duração média faz sentido (regular 3-8 meses, agregador 2-6 meses)
--   ✓ Contratos por spell > 1 indica renovações (bom sinal)
--   ✓ Spells ativos devem existir para branches ativas
-- ============================================================================

SELECT
    branch_id,
    branch_name,
    segmento,
    COUNT(*)                                          AS total_spells,
    COUNT(DISTINCT member_id)                         AS membros_unicos,
    ROUND(AVG(duration_months), 1)                    AS duracao_media_meses,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_months)
                                                      AS mediana_meses,
    ROUND(AVG(contracts_in_spell), 1)                 AS media_contratos_spell,
    COUNT(*) FILTER (WHERE spell_status = 'active')   AS ativos,
    COUNT(*) FILTER (WHERE spell_status = 'ended')    AS encerrados
FROM analytics.mv_spells_v2
GROUP BY branch_id, branch_name, segmento
ORDER BY branch_id, segmento;


-- ============================================================================
--  VAL_2B: INTEGRIDADE — CONTRATOS × SPELLS
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ contratos_na_mv1 = contratos_nos_spells
--   ✓ Se divergir, há contratos "órfãos" que não entraram em nenhum spell
-- ============================================================================

WITH total_contratos AS (
    SELECT COUNT(*) AS n FROM analytics.mv_contract_classified
),
total_em_spells AS (
    SELECT SUM(contracts_in_spell) AS n FROM analytics.mv_spells_v2
)
SELECT
    tc.n AS contratos_na_mv1,
    ts.n AS contratos_nos_spells,
    tc.n - ts.n AS diferenca,
    CASE WHEN tc.n = ts.n THEN ' OK' ELSE ' DIVERGÊNCIA' END AS status
FROM total_contratos tc, total_em_spells ts;


-- ============================================================================
--  VAL_2C: NÃO DEVEM EXISTIR SPELLS SOBREPOSTOS
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ Deve retornar 0 linhas
--   ✓ Sobreposição indica bug no algoritmo de spell
-- ============================================================================

SELECT
    a.member_id,
    a.segmento,
    a.spell_num AS spell_a, a.spell_start AS start_a, a.spell_end AS end_a,
    b.spell_num AS spell_b, b.spell_start AS start_b, b.spell_end AS end_b
FROM analytics.mv_spells_v2 a
JOIN analytics.mv_spells_v2 b
    ON a.member_id = b.member_id
   AND a.segmento = b.segmento
   AND a.spell_num < b.spell_num
   AND a.spell_end >= b.spell_start;
-- ESPERADO: 0 linhas


-- ============================================================================
--  VAL_3A: DISTRIBUIÇÃO DE EVENTOS
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ CHURN não deve ser > 80% dos eventos (senão algo está errado)
--   ✓ ATIVO deve ter membros correspondentes aos spells ativos
--   ✓ MIGRACAO deve ter volume baixo (< 5% tipicamente)
--   ✓ INDETERMINADO só para spells encerrados há < 30 dias do corte
-- ============================================================================

SELECT
    segmento,
    evento,
    COUNT(*)                                          AS total,
    COUNT(DISTINCT member_id)                         AS membros,
    ROUND(AVG(duration_months), 1)                    AS duracao_media_spell,
    ROUND(AVG(gap_any_days) FILTER (WHERE gap_any_days IS NOT NULL), 1)
                                                      AS gap_medio_dias,
    MIN(spell_end)                                    AS spell_end_mais_antigo,
    MAX(spell_end)                                    AS spell_end_mais_recente
FROM analytics.mv_churn_events
GROUP BY segmento, evento
ORDER BY segmento, evento;


-- ============================================================================
--  VAL_3B: INTEGRIDADE — SPELLS × EVENTOS
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ Cada spell deve ter exatamente 1 evento
-- ============================================================================

WITH spell_count AS (
    SELECT COUNT(*) AS n FROM analytics.mv_spells_v2
),
event_count AS (
    SELECT COUNT(*) AS n FROM analytics.mv_churn_events
)
SELECT
    sc.n AS total_spells,
    ec.n AS total_eventos,
    sc.n - ec.n AS diferenca,
    CASE WHEN sc.n = ec.n THEN ' OK' ELSE ' DIVERGÊNCIA' END AS status
FROM spell_count sc, event_count ec;


-- ============================================================================
--  VAL_3C: CHURNS POR MÊS E SEGMENTO (2025+)
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ Tendência mensal faz sentido (jan tem mais churn pós-férias)
--   ✓ Não há meses com zero churns (se branch ativa)
-- ============================================================================

SELECT
    churn_confirmed_mes,
    segmento,
    COUNT(*)                                          AS churns,
    COUNT(DISTINCT member_id)                         AS membros,
    ROUND(AVG(duration_months), 1)                    AS permanencia_media
FROM analytics.mv_churn_events
WHERE evento = 'CHURN'
  AND churn_confirmed_mes >= '2025-01-01'
  AND churn_confirmed_mes <= '2026-02-01'
GROUP BY churn_confirmed_mes, segmento
ORDER BY churn_confirmed_mes, segmento;


-- ============================================================================
--  VAL_3D: MIGRAÇÕES — DIREÇÃO
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ Regular→Agregador ou Agregador→Regular?
--   ✓ Gap médio deve ser < 30 dias (por definição)
-- ============================================================================

SELECT
    segmento AS de_segmento,
    next_spell_segmento AS para_segmento,
    COUNT(*) AS migracoes,
    ROUND(AVG(gap_any_days), 1) AS gap_medio_dias,
    MIN(spell_end) AS primeira_migracao,
    MAX(spell_end) AS ultima_migracao
FROM analytics.mv_churn_events
WHERE evento = 'MIGRACAO'
GROUP BY segmento, next_spell_segmento
ORDER BY migracoes DESC;


-- ============================================================================
--  VAL_3E: CHURN RATE MENSAL POR BRANCH (KPI PRINCIPAL)
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ Churn rate entre 3-15% mensal é normal para academias
--   ✓ > 20% indica problema sério
--   ✓ Comparar entre branches para detectar outliers
-- ============================================================================

WITH meses AS (
    SELECT generate_series('2025-01-01'::DATE, '2026-01-01'::DATE, '1 month'::INTERVAL)::DATE AS mes
),
ativos_inicio AS (
    SELECT
        m.mes,
        s.branch_id,
        s.branch_name,
        s.segmento,
        COUNT(DISTINCT s.member_id) AS ativos
    FROM meses m
    CROSS JOIN analytics.mv_spells_v2 s
    WHERE s.spell_start < m.mes
      AND s.spell_end >= m.mes
    GROUP BY m.mes, s.branch_id, s.branch_name, s.segmento
),
churns_mes AS (
    SELECT
        churn_confirmed_mes AS mes,
        branch_id,
        segmento,
        COUNT(*) AS churns
    FROM analytics.mv_churn_events
    WHERE evento = 'CHURN'
      AND churn_confirmed_mes >= '2025-01-01'
    GROUP BY churn_confirmed_mes, branch_id, segmento
)
SELECT
    a.mes,
    a.branch_id,
    a.branch_name,
    a.segmento,
    a.ativos AS ativos_inicio_mes,
    COALESCE(c.churns, 0) AS churns,
    CASE
        WHEN a.ativos > 0
        THEN ROUND(COALESCE(c.churns, 0)::NUMERIC / a.ativos * 100, 2)
        ELSE 0
    END AS churn_rate_pct
FROM ativos_inicio a
LEFT JOIN churns_mes c
    ON a.mes = c.mes AND a.branch_id = c.branch_id AND a.segmento = c.segmento
WHERE a.segmento = 'REGULAR'
ORDER BY a.branch_id, a.mes;


-- ============================================================================
--  VAL_3F: DRILL-DOWN — CONFERÊNCIA MANUAL DE CHURNS
-- ============================================================================
-- INSTRUÇÕES:
--   1. Pegue 5-10 member_ids da lista abaixo
--   2. Confira no sistema EVO se o aluno realmente cancelou/expirou
--   3. Verifique se last_access_date bate com o esperado
-- ============================================================================

SELECT
    ce.member_id,
    m.full_name,
    ce.branch_name,
    ce.segmento,
    ce.spell_start,
    ce.spell_end,
    ce.churn_confirmed_date,
    ce.duration_months AS permanencia_meses,
    ce.contracts_in_spell,
    ce.membership_names,
    m.last_access_date,
    m.status AS status_evo,
    m.cellphone,
    m.email
FROM analytics.mv_churn_events ce
JOIN core.evo_members m ON ce.member_id = m.member_id
WHERE ce.evento = 'CHURN'
  AND ce.segmento = 'REGULAR'
  AND ce.churn_confirmed_mes = '2025-03-01'  -- ← ALTERAR MÊS CONFORME NECESSÁRIO
ORDER BY ce.spell_end DESC
LIMIT 20;


-- ============================================================================
--  VAL_4A: MEMBER_KPI_BASE — RESUMO POR BRANCH
-- ============================================================================

SELECT
    branch_id,
    branch_name,
    segmento_atual,
    COUNT(*)                                          AS total_membros,
    COUNT(*) FILTER (WHERE is_active)                 AS ativos,
    ROUND(AVG(total_months_active), 1)                AS permanencia_media,
    ROUND(AVG(avg_checkins_per_week) FILTER (WHERE is_active AND avg_checkins_per_week > 0), 2)
                                                      AS freq_semanal_media,
    ROUND(AVG(receita_total_paga) FILTER (WHERE segmento_atual = 'REGULAR' AND receita_total_paga > 0), 2)
                                                      AS receita_media_regular,
    ROUND(AVG(total_churns), 2)                       AS media_churns
FROM analytics.mv_member_kpi_base
GROUP BY branch_id, branch_name, segmento_atual
ORDER BY branch_id, segmento_atual;


-- ============================================================================
--  VAL_4B: ALERTA DE INATIVIDADE (RISCO DE CHURN)
-- ============================================================================
-- Membros ativos que não fazem checkin há mais de 15 dias
-- = candidatos para régua de e-mail preventiva
-- ============================================================================

SELECT
    member_id,
    full_name,
    branch_name,
    segmento_atual,
    last_checkin::DATE AS ultimo_checkin,
    dias_sem_checkin,
    avg_checkins_per_week AS freq_semanal,
    total_months_active AS permanencia_meses,
    total_churns AS churns_anteriores
FROM analytics.mv_member_kpi_base
WHERE is_active = TRUE
  AND dias_sem_checkin > 15
ORDER BY dias_sem_checkin DESC
LIMIT 30;


-- ============================================================================
--  VAL_4C: CROSS-CHECK — ATIVOS NA MV vs STATUS EVO
-- ============================================================================
-- O QUE VERIFICAR:
--   ✓ Membros is_active=TRUE devem ter status_evo='Active' (maioria)
--   ✓ Divergências podem indicar dessincronização EVO ↔ contratos
-- ============================================================================

SELECT
    CASE WHEN is_active THEN 'MV: Ativo' ELSE 'MV: Inativo' END AS status_mv,
    status_evo,
    COUNT(*) AS total,
    ROUND(COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER () * 100, 1) AS pct
FROM analytics.mv_member_kpi_base
GROUP BY is_active, status_evo
ORDER BY is_active DESC, status_evo;
