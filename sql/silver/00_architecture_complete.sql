-- ============================================================================
-- SKYFIT ANALYTICS — ARQUITETURA COMPLETA v2.0
-- ============================================================================
-- Data: 2026-02-12
-- Escopo: Branches MVP [345, 181, 59, 233, 401, 166, 33, 6, 149]
-- Período de análise: 2025-01-01 em diante
-- Data de corte dos dados: 2026-02-10
--
-- ESTRUTURA:
--   SEÇÃO 0: Schema + Índices nas tabelas fonte
--   SEÇÃO 1: MV contract_classified (classificação de contratos)
--   SEÇÃO 2: MV spells_v2 (períodos contínuos por segmento)
--   SEÇÃO 3: MV churn_events (eventos de churn/migração)
--   SEÇÃO 4: MV member_kpi_base (métricas consolidadas por membro)
--   SEÇÃO 5: Rotina de refresh
--
-- REGRAS DE NEGÓCIO:
--   Segmento AGREGADOR:
--     1. membership_name contém gympass/totalpass/wellhub, OU
--     2. membro tem gympass_id/code_totalpass E entries de agregador no período
--     3. "Diária validada com sucesso" (sem plataforma) = AGREGADOR
--   Churn: 30 dias sem contrato (qualquer segmento)
--   Migração: troca de segmento em < 30 dias (NÃO é churn)
--   Receita agregador: R$0 (repasse B2B fora da base)
-- ============================================================================


-- ============================================================================
-- SEÇÃO 0: SCHEMA + ÍNDICES NAS TABELAS FONTE
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- Constantes do projeto (para queries parametrizadas)
-- Usar em todas as MVs para consistência
-- Branch IDs MVP: '{345,181,59,233,401,166,33,6,149}'
-- Data início análise: '2025-01-01'
-- Data corte dados: '2026-02-10'

-- Índices nas tabelas fonte (executar uma única vez)
-- CONCURRENTLY não bloqueia reads/writes

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_entries_member_date_reason
ON core.evo_entries (member_id, entry_date)
INCLUDE (block_reason, branch_id)
WHERE block_reason IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_entries_branch_date
ON core.evo_entries (branch_id, entry_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_memberships_member_dates
ON core.evo_member_memberships (member_id, start_date, end_date)
INCLUDE (membership_name, membership_status, cancel_date);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_members_branch
ON core.evo_members (branch_id)
INCLUDE (member_id, gympass_id, code_totalpass);


-- ============================================================================
-- SEÇÃO 1: MV CONTRACT_CLASSIFIED
-- ============================================================================
-- Classifica cada contrato como REGULAR ou AGREGADOR
-- Calcula data_efetiva_fim (cancel_date se antecipado, senão end_date)
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS analytics.mv_churn_events CASCADE;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_spells_v2 CASCADE;
DROP MATERIALIZED VIEW IF EXISTS analytics.mv_contract_classified CASCADE;

CREATE MATERIALIZED VIEW analytics.mv_contract_classified AS
WITH
-- -----------------------------------------------------------------------
-- CTE 1: Contratos das branches MVP com dados do membro
-- -----------------------------------------------------------------------
contratos_raw AS (
    SELECT
        mm.member_membership_id,
        mm.member_id,
        m.branch_id,
        m.branch_name,
        mm.membership_id,
        mm.membership_name,
        mm.membership_status,
        mm.start_date::DATE                           AS start_date,
        mm.end_date::DATE                             AS end_date,
        mm.cancel_date,
        mm.sale_id,
        mm.sale_date,
        -- Data efetiva de fim: cancel_date se cancelou antes do end_date
        CASE
            WHEN mm.cancel_date IS NOT NULL
                 AND mm.cancel_date::DATE < mm.end_date::DATE
            THEN mm.cancel_date::DATE
            ELSE mm.end_date::DATE
        END                                           AS data_efetiva_fim,
        -- Flags do membro para cruzamento
        m.gympass_id,
        m.code_totalpass,
        m.register_date,
        m.gender,
        m.birth_date
    FROM core.evo_member_memberships mm
    JOIN core.evo_members m
        ON mm.member_id = m.member_id
    WHERE m.branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[])
),

-- -----------------------------------------------------------------------
-- CTE 2: Pré-classificação pelo nome do contrato (NÍVEL 1 - rápido)
-- -----------------------------------------------------------------------
classificacao_nivel1 AS (
    SELECT
        cr.*,
        CASE
            WHEN LOWER(COALESCE(cr.membership_name, ''))
                 ~ '(gympass|totalpass|wellhub)'
            THEN 'AGREGADOR'
            ELSE NULL  -- ainda não definido, vai para nível 2
        END AS segmento_n1
    FROM contratos_raw cr
),

-- -----------------------------------------------------------------------
-- CTE 3: Para contratos não classificados no nível 1,
--         verificar se membro tem ID de agregador E entries de agregador
--         no período do contrato (NÍVEL 2 - cruzamento com entries)
-- -----------------------------------------------------------------------
classificacao_nivel2 AS (
    SELECT
        c.*,
        CASE
            -- Já classificado no nível 1
            WHEN c.segmento_n1 IS NOT NULL THEN c.segmento_n1

            -- Membro tem ID de agregador?
            WHEN (c.gympass_id IS NOT NULL OR c.code_totalpass IS NOT NULL)
                 -- E tem entries de agregador no período do contrato?
                 AND EXISTS (
                     SELECT 1
                     FROM core.evo_entries e
                     WHERE e.member_id = c.member_id
                       AND e.entry_date::DATE BETWEEN c.start_date AND c.end_date
                       AND (
                           LOWER(e.block_reason) LIKE '%gympass%'
                           OR LOWER(e.block_reason) LIKE '%totalpass%'
                           OR LOWER(e.block_reason) LIKE '%wellhub%'
                           OR e.block_reason = 'Diária validada com sucesso'
                       )
                 )
            THEN 'AGREGADOR'

            -- Default
            ELSE 'REGULAR'
        END AS segmento
    FROM classificacao_nivel1 c
)

-- -----------------------------------------------------------------------
-- RESULTADO FINAL
-- -----------------------------------------------------------------------
SELECT
    member_membership_id,
    member_id,
    branch_id,
    branch_name,
    membership_id,
    membership_name,
    membership_status,
    start_date,
    end_date,
    cancel_date,
    data_efetiva_fim,
    sale_id,
    sale_date,
    segmento,
    gympass_id,
    code_totalpass,
    register_date,
    gender,
    birth_date
FROM classificacao_nivel2;

-- Índice único para REFRESH CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS idx_cc_pk
ON analytics.mv_contract_classified (member_membership_id);

-- Índices para queries downstream
CREATE INDEX IF NOT EXISTS idx_cc_member_segmento_dates
ON analytics.mv_contract_classified (member_id, segmento, start_date, data_efetiva_fim);

CREATE INDEX IF NOT EXISTS idx_cc_branch_segmento
ON analytics.mv_contract_classified (branch_id, segmento);

CREATE INDEX IF NOT EXISTS idx_cc_status
ON analytics.mv_contract_classified (membership_status);


-- ============================================================================
-- VALIDAÇÃO 1A: Distribuição de segmentos
-- ============================================================================
-- Espera-se que REGULAR > AGREGADOR na maioria das branches
-- Se AGREGADOR > 70%, investigar classificação
-- ============================================================================

/*
-- VAL_1A: Contagem por segmento e branch
SELECT
    branch_id,
    branch_name,
    segmento,
    COUNT(*)                                          AS total_contratos,
    COUNT(DISTINCT member_id)                         AS membros_unicos,
    COUNT(*) FILTER (WHERE membership_status = 'active')   AS contratos_ativos,
    COUNT(*) FILTER (WHERE membership_status = 'canceled') AS contratos_cancelados,
    COUNT(*) FILTER (WHERE membership_status = 'expired')  AS contratos_expirados
FROM analytics.mv_contract_classified
GROUP BY branch_id, branch_name, segmento
ORDER BY branch_id, segmento;
*/

-- ============================================================================
-- VALIDAÇÃO 1B: Contratos classificados por NÍVEL 2 (cruzamento com entries)
-- ============================================================================
-- Mostra contratos que NÃO foram pegos pelo nome mas SIM pelo cruzamento
-- Se esse número for grande, a regex do nível 1 pode estar incompleta
-- ============================================================================

/*
-- VAL_1B: Contratos classificados APENAS pelo cruzamento com entries
SELECT
    cc.member_membership_id,
    cc.member_id,
    cc.membership_name,
    cc.segmento,
    cc.start_date,
    cc.end_date,
    cc.gympass_id IS NOT NULL AS tem_gympass_id,
    cc.code_totalpass IS NOT NULL AS tem_totalpass_id
FROM analytics.mv_contract_classified cc
WHERE cc.segmento = 'AGREGADOR'
  AND NOT (LOWER(COALESCE(cc.membership_name, '')) ~ '(gympass|totalpass|wellhub)')
ORDER BY cc.member_id, cc.start_date
LIMIT 50;
*/

-- ============================================================================
-- VALIDAÇÃO 1C: Membros com contratos em AMBOS os segmentos (transições)
-- ============================================================================
-- Estes são os casos mais críticos para a lógica de spell/churn
-- ============================================================================

/*
-- VAL_1C: Membros com transição de segmento
WITH member_segments AS (
    SELECT
        member_id,
        array_agg(DISTINCT segmento) AS segmentos,
        COUNT(DISTINCT segmento) AS qtd_segmentos
    FROM analytics.mv_contract_classified
    GROUP BY member_id
)
SELECT
    ms.member_id,
    ms.segmentos,
    cc.membership_name,
    cc.segmento,
    cc.start_date,
    cc.data_efetiva_fim,
    cc.membership_status
FROM member_segments ms
JOIN analytics.mv_contract_classified cc ON ms.member_id = cc.member_id
WHERE ms.qtd_segmentos > 1
ORDER BY ms.member_id, cc.start_date
LIMIT 100;
*/

-- ============================================================================
-- VALIDAÇÃO 1D: Sanity check — data_efetiva_fim
-- ============================================================================
-- cancel_date < end_date deve usar cancel_date
-- Sem cancel_date deve usar end_date
-- ============================================================================

/*
-- VAL_1D: Verificar data_efetiva_fim
SELECT
    member_membership_id,
    membership_status,
    start_date,
    end_date,
    cancel_date,
    data_efetiva_fim,
    CASE
        WHEN cancel_date IS NOT NULL AND cancel_date::DATE < end_date
        THEN CASE WHEN data_efetiva_fim = cancel_date::DATE THEN 'OK' ELSE 'ERRO' END
        ELSE CASE WHEN data_efetiva_fim = end_date THEN 'OK' ELSE 'ERRO' END
    END AS check_resultado
FROM analytics.mv_contract_classified
WHERE CASE
        WHEN cancel_date IS NOT NULL AND cancel_date::DATE < end_date
        THEN data_efetiva_fim != cancel_date::DATE
        ELSE data_efetiva_fim != end_date
      END
LIMIT 20;
-- Se retornar 0 linhas = CORRETO
*/


-- ============================================================================
-- SEÇÃO 2: MV SPELLS_V2
-- ============================================================================
-- Agrupa contratos consecutivos do MESMO segmento com gap ≤ 30 dias
-- Cada spell = período contínuo de atividade em um segmento
--
-- Regras:
--   - Novo spell quando gap > 30 dias entre contratos do mesmo segmento
--   - Novo spell quando muda de segmento (mesmo que gap < 30 dias)
--   - Contratos sobrepostos = mesmo spell
-- ============================================================================

CREATE MATERIALIZED VIEW analytics.mv_spells_v2 AS
WITH
-- -----------------------------------------------------------------------
-- CTE 1: Ordenar contratos por membro+segmento, calcular running max de fim
-- -----------------------------------------------------------------------
contratos_ordenados AS (
    SELECT
        member_membership_id,
        member_id,
        branch_id,
        branch_name,
        segmento,
        start_date,
        data_efetiva_fim,
        membership_name,
        membership_status,
        -- Max data_efetiva_fim de todos os contratos ANTERIORES do mesmo membro+segmento
        -- Isso lida com contratos sobrepostos (ex: renovação que inicia antes do anterior terminar)
        MAX(data_efetiva_fim) OVER (
            PARTITION BY member_id, segmento
            ORDER BY start_date, data_efetiva_fim
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS max_fim_anterior
    FROM analytics.mv_contract_classified
),

-- -----------------------------------------------------------------------
-- CTE 2: Detectar quebras de spell (gap > 30 dias ou primeiro contrato)
-- -----------------------------------------------------------------------
spell_breaks AS (
    SELECT
        co.*,
        CASE
            -- Primeiro contrato do membro neste segmento
            WHEN max_fim_anterior IS NULL THEN 1
            -- Gap > 30 dias desde o fim do contrato anterior mais recente
            WHEN start_date - max_fim_anterior > 30 THEN 1
            -- Continuidade
            ELSE 0
        END AS is_new_spell
    FROM contratos_ordenados co
),

-- -----------------------------------------------------------------------
-- CTE 3: Atribuir spell_id incremental por membro+segmento
-- -----------------------------------------------------------------------
spell_numbered AS (
    SELECT
        sb.*,
        SUM(is_new_spell) OVER (
            PARTITION BY member_id, segmento
            ORDER BY start_date, data_efetiva_fim
        ) AS spell_num
    FROM spell_breaks sb
)

-- -----------------------------------------------------------------------
-- RESULTADO: Um registro por spell
-- -----------------------------------------------------------------------
SELECT
    -- Chave composta
    member_id,
    branch_id,
    branch_name,
    segmento,
    spell_num,

    -- Datas do spell
    MIN(start_date)                                   AS spell_start,
    MAX(data_efetiva_fim)                             AS spell_end,

    -- Duração
    MAX(data_efetiva_fim) - MIN(start_date)           AS duration_days,
    ROUND((MAX(data_efetiva_fim) - MIN(start_date))::NUMERIC / 30.44, 1)
                                                      AS duration_months,

    -- Composição
    COUNT(*)                                          AS contracts_in_spell,
    array_agg(DISTINCT membership_name ORDER BY membership_name)
                                                      AS membership_names,
    array_agg(DISTINCT membership_id ORDER BY membership_id)
                                                      AS membership_ids,

    -- Status: se algum contrato está ativo, spell está ativo
    CASE
        WHEN bool_or(membership_status = 'active') THEN 'active'
        ELSE 'ended'
    END                                               AS spell_status

FROM spell_numbered
GROUP BY member_id, branch_id, branch_name, segmento, spell_num;

-- Índice único para REFRESH CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS idx_spells_pk
ON analytics.mv_spells_v2 (member_id, segmento, spell_num);

-- Índices para queries downstream
CREATE INDEX IF NOT EXISTS idx_spells_branch_segmento
ON analytics.mv_spells_v2 (branch_id, segmento);

CREATE INDEX IF NOT EXISTS idx_spells_member_dates
ON analytics.mv_spells_v2 (member_id, spell_start, spell_end);

CREATE INDEX IF NOT EXISTS idx_spells_status
ON analytics.mv_spells_v2 (spell_status);

CREATE INDEX IF NOT EXISTS idx_spells_end
ON analytics.mv_spells_v2 (spell_end);


-- ============================================================================
-- VALIDAÇÃO 2A: Contagem de spells por segmento e branch
-- ============================================================================

/*
-- VAL_2A: Resumo de spells
SELECT
    branch_id,
    branch_name,
    segmento,
    COUNT(*)                                          AS total_spells,
    COUNT(DISTINCT member_id)                         AS membros_unicos,
    ROUND(AVG(duration_months), 1)                    AS duracao_media_meses,
    ROUND(AVG(contracts_in_spell), 1)                 AS contratos_por_spell,
    COUNT(*) FILTER (WHERE spell_status = 'active')   AS spells_ativos,
    COUNT(*) FILTER (WHERE spell_status = 'ended')    AS spells_encerrados
FROM analytics.mv_spells_v2
GROUP BY branch_id, branch_name, segmento
ORDER BY branch_id, segmento;
*/

-- ============================================================================
-- VALIDAÇÃO 2B: Verificar se nenhum contrato ficou fora de um spell
-- ============================================================================
-- O count de contratos nos spells deve bater com o total de contratos na MV1
-- ============================================================================

/*
-- VAL_2B: Integridade contratos ↔ spells
WITH total_contratos AS (
    SELECT COUNT(*) AS n FROM analytics.mv_contract_classified
),
total_em_spells AS (
    SELECT SUM(contracts_in_spell) AS n FROM analytics.mv_spells_v2
)
SELECT
    tc.n AS contratos_na_mv1,
    ts.n AS contratos_nos_spells,
    CASE WHEN tc.n = ts.n THEN 'OK ✓' ELSE 'DIVERGÊNCIA ✗' END AS status
FROM total_contratos tc, total_em_spells ts;
*/

-- ============================================================================
-- VALIDAÇÃO 2C: Membros com transição — verificar spells separados
-- ============================================================================
-- Membro que foi REGULAR e depois AGREGADOR deve ter spells distintos
-- ============================================================================

/*
-- VAL_2C: Spells de membros com transição
SELECT
    s.member_id,
    s.segmento,
    s.spell_num,
    s.spell_start,
    s.spell_end,
    s.duration_months,
    s.contracts_in_spell,
    s.membership_names
FROM analytics.mv_spells_v2 s
WHERE s.member_id IN (
    SELECT member_id
    FROM analytics.mv_spells_v2
    GROUP BY member_id
    HAVING COUNT(DISTINCT segmento) > 1
)
ORDER BY s.member_id, s.spell_start
LIMIT 50;
*/

-- ============================================================================
-- VALIDAÇÃO 2D: Não devem existir spells sobrepostos para o mesmo membro+segmento
-- ============================================================================

/*
-- VAL_2D: Checar sobreposição de spells
SELECT
    a.member_id,
    a.segmento,
    a.spell_num AS spell_a,
    a.spell_start AS start_a,
    a.spell_end AS end_a,
    b.spell_num AS spell_b,
    b.spell_start AS start_b,
    b.spell_end AS end_b
FROM analytics.mv_spells_v2 a
JOIN analytics.mv_spells_v2 b
    ON a.member_id = b.member_id
   AND a.segmento = b.segmento
   AND a.spell_num < b.spell_num
   AND a.spell_end >= b.spell_start  -- sobreposição
LIMIT 20;
-- Se retornar 0 linhas = CORRETO
*/


-- ============================================================================
-- SEÇÃO 3: MV CHURN_EVENTS
-- ============================================================================
-- Para cada spell encerrado, determina o que aconteceu:
--   CHURN: nenhum contrato novo (qualquer segmento) em 30 dias
--   MIGRACAO: contrato novo em segmento DIFERENTE em ≤ 30 dias
--   ATIVO: spell ainda vigente
--   INDETERMINADO: spell encerrou há < 30 dias (não sabemos ainda)
--
-- A data do churn = data_efetiva_fim do spell (último contrato)
-- A data de corte = '2026-02-10' (último dado disponível)
-- ============================================================================

CREATE MATERIALIZED VIEW analytics.mv_churn_events AS
WITH
-- -----------------------------------------------------------------------
-- CTE 1: Para cada spell, encontrar a PRÓXIMA atividade do membro
--         (qualquer segmento, qualquer spell)
-- -----------------------------------------------------------------------
spell_timeline AS (
    SELECT
        s.*,
        -- Próximo spell do membro (qualquer segmento)
        LEAD(spell_start) OVER w_member        AS next_spell_start,
        LEAD(segmento) OVER w_member           AS next_spell_segmento,
        LEAD(spell_num) OVER w_member          AS next_spell_num,
        -- Próximo spell do MESMO segmento
        LEAD(spell_start) OVER w_segmento      AS next_same_seg_start
    FROM analytics.mv_spells_v2 s
    WINDOW
        w_member   AS (PARTITION BY member_id ORDER BY spell_start),
        w_segmento AS (PARTITION BY member_id, segmento ORDER BY spell_start)
),

-- -----------------------------------------------------------------------
-- CTE 2: Calcular gap e classificar evento
-- -----------------------------------------------------------------------
eventos AS (
    SELECT
        st.*,
        -- Gap até próxima atividade (qualquer segmento)
        st.next_spell_start - st.spell_end     AS gap_any_days,
        -- Gap até próxima atividade no MESMO segmento
        st.next_same_seg_start - st.spell_end  AS gap_same_seg_days,
        -- Classificação do evento
        CASE
            -- Spell ainda ativo
            WHEN st.spell_status = 'active'
            THEN 'ATIVO'

            -- Spell encerrou há menos de 30 dias do corte — não podemos determinar
            WHEN st.spell_end > ('2026-02-10'::DATE - 30)
                 AND st.next_spell_start IS NULL
            THEN 'INDETERMINADO'

            -- Nenhuma atividade futura e já passaram 30+ dias
            WHEN st.next_spell_start IS NULL
                 AND ('2026-02-10'::DATE - st.spell_end) > 30
            THEN 'CHURN'

            -- Próxima atividade em segmento DIFERENTE em ≤ 30 dias = migração
            WHEN st.next_spell_start IS NOT NULL
                 AND (st.next_spell_start - st.spell_end) <= 30
                 AND st.next_spell_segmento != st.segmento
            THEN 'MIGRACAO'

            -- Próxima atividade no MESMO segmento em > 30 dias = churn + retorno
            WHEN st.next_same_seg_start IS NOT NULL
                 AND (st.next_same_seg_start - st.spell_end) > 30
                 AND (st.next_spell_start IS NULL
                      OR (st.next_spell_start - st.spell_end) > 30)
            THEN 'CHURN'

            -- Próxima atividade em segmento diferente > 30 dias = churn
            WHEN st.next_spell_start IS NOT NULL
                 AND (st.next_spell_start - st.spell_end) > 30
            THEN 'CHURN'

            -- Catch-all (não deveria chegar aqui)
            ELSE 'RENOVACAO'
        END AS evento,

        -- Mês do evento (para agregações mensais)
        DATE_TRUNC('month', st.spell_end)::DATE AS evento_mes

    FROM spell_timeline st
)

-- -----------------------------------------------------------------------
-- RESULTADO: Todos os eventos (filtrar por período na query de consumo)
-- -----------------------------------------------------------------------
SELECT
    member_id,
    branch_id,
    branch_name,
    segmento,
    spell_num,
    spell_start,
    spell_end,
    duration_days,
    duration_months,
    contracts_in_spell,
    membership_names,
    spell_status,
    next_spell_start,
    next_spell_segmento,
    gap_any_days,
    gap_same_seg_days,
    evento,
    evento_mes,
    -- Data efetiva do churn (spell_end + 30 para quando o churn se "confirma")
    CASE
        WHEN evento = 'CHURN' THEN spell_end + 30
        ELSE NULL
    END AS churn_confirmed_date,
    -- Mês confirmado do churn
    CASE
        WHEN evento = 'CHURN' THEN DATE_TRUNC('month', spell_end + 30)::DATE
        ELSE NULL
    END AS churn_confirmed_mes
FROM eventos;

-- Índice único para REFRESH CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS idx_churn_pk
ON analytics.mv_churn_events (member_id, segmento, spell_num);

-- Índices para queries de KPI
CREATE INDEX IF NOT EXISTS idx_churn_evento_mes
ON analytics.mv_churn_events (evento, evento_mes);

CREATE INDEX IF NOT EXISTS idx_churn_branch_evento
ON analytics.mv_churn_events (branch_id, evento);

CREATE INDEX IF NOT EXISTS idx_churn_confirmed_date
ON analytics.mv_churn_events (churn_confirmed_date)
WHERE evento = 'CHURN';

CREATE INDEX IF NOT EXISTS idx_churn_segmento_evento
ON analytics.mv_churn_events (segmento, evento);


-- ============================================================================
-- VALIDAÇÃO 3A: Distribuição de eventos por tipo
-- ============================================================================

/*
-- VAL_3A: Resumo de eventos
SELECT
    segmento,
    evento,
    COUNT(*)                                          AS total_eventos,
    COUNT(DISTINCT member_id)                         AS membros_unicos,
    ROUND(AVG(duration_months), 1)                    AS duracao_media_spell_meses,
    ROUND(AVG(gap_any_days) FILTER (WHERE gap_any_days IS NOT NULL), 1)
                                                      AS gap_medio_dias
FROM analytics.mv_churn_events
GROUP BY segmento, evento
ORDER BY segmento, evento;
*/

-- ============================================================================
-- VALIDAÇÃO 3B: Churns por mês e segmento (2025+)
-- ============================================================================

/*
-- VAL_3B: Linha do tempo de churns confirmados
SELECT
    churn_confirmed_mes,
    segmento,
    COUNT(*)                                          AS churns,
    COUNT(DISTINCT member_id)                         AS membros
FROM analytics.mv_churn_events
WHERE evento = 'CHURN'
  AND churn_confirmed_mes >= '2025-01-01'
GROUP BY churn_confirmed_mes, segmento
ORDER BY churn_confirmed_mes, segmento;
*/

-- ============================================================================
-- VALIDAÇÃO 3C: Migrações — direção e volume
-- ============================================================================

/*
-- VAL_3C: De qual segmento para qual
SELECT
    segmento                                          AS de_segmento,
    next_spell_segmento                               AS para_segmento,
    COUNT(*)                                          AS migracoes,
    ROUND(AVG(gap_any_days), 1)                       AS gap_medio_dias
FROM analytics.mv_churn_events
WHERE evento = 'MIGRACAO'
GROUP BY segmento, next_spell_segmento
ORDER BY migracoes DESC;
*/

-- ============================================================================
-- VALIDAÇÃO 3D: Integridade — todo spell deve ter exatamente 1 evento
-- ============================================================================

/*
-- VAL_3D: Um evento por spell
WITH spell_count AS (
    SELECT COUNT(*) AS n FROM analytics.mv_spells_v2
),
event_count AS (
    SELECT COUNT(*) AS n FROM analytics.mv_churn_events
)
SELECT
    sc.n AS total_spells,
    ec.n AS total_eventos,
    CASE WHEN sc.n = ec.n THEN 'OK ✓' ELSE 'DIVERGÊNCIA ✗' END AS status
FROM spell_count sc, event_count ec;
*/

-- ============================================================================
-- VALIDAÇÃO 3E: Drill-down — lista de alunos churned em um mês específico
-- ============================================================================
-- Usar para auditoria manual: pegar 10 alunos e conferir no sistema EVO
-- ============================================================================

/*
-- VAL_3E: Detalhe de churns de jan/2025
SELECT
    ce.member_id,
    m.full_name,
    ce.branch_name,
    ce.segmento,
    ce.spell_start,
    ce.spell_end,
    ce.duration_months,
    ce.contracts_in_spell,
    ce.membership_names,
    ce.churn_confirmed_date,
    m.last_access_date,
    m.status AS status_atual_evo
FROM analytics.mv_churn_events ce
JOIN core.evo_members m ON ce.member_id = m.member_id
WHERE ce.evento = 'CHURN'
  AND ce.churn_confirmed_mes = '2025-02-01'  -- churns confirmados em fev/2025
  AND ce.segmento = 'REGULAR'
ORDER BY ce.spell_end DESC
LIMIT 20;
*/

-- ============================================================================
-- VALIDAÇÃO 3F: Taxa de churn mensal por branch (KPI principal)
-- ============================================================================
-- churn_rate = churns_confirmados_no_mês / (ativos_início_do_mês + churns)
-- ============================================================================

/*
-- VAL_3F: Churn rate mensal — apenas REGULAR, 2025+
WITH meses AS (
    SELECT generate_series('2025-01-01'::DATE, '2026-01-01'::DATE, '1 month'::INTERVAL)::DATE AS mes
),
-- Spells ativos no início de cada mês
ativos_inicio AS (
    SELECT
        m.mes,
        s.branch_id,
        s.segmento,
        COUNT(DISTINCT s.member_id) AS ativos
    FROM meses m
    CROSS JOIN analytics.mv_spells_v2 s
    WHERE s.spell_start < m.mes           -- spell já começou
      AND s.spell_end >= m.mes            -- spell ainda não terminou
    GROUP BY m.mes, s.branch_id, s.segmento
),
-- Churns confirmados em cada mês
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
    a.segmento,
    a.ativos                                          AS ativos_inicio_mes,
    COALESCE(c.churns, 0)                             AS churns_no_mes,
    CASE
        WHEN a.ativos > 0
        THEN ROUND(COALESCE(c.churns, 0)::NUMERIC / a.ativos * 100, 2)
        ELSE 0
    END                                               AS churn_rate_pct
FROM ativos_inicio a
LEFT JOIN churns_mes c
    ON a.mes = c.mes
   AND a.branch_id = c.branch_id
   AND a.segmento = c.segmento
WHERE a.segmento = 'REGULAR'
ORDER BY a.branch_id, a.mes;
*/


-- ============================================================================
-- SEÇÃO 4: MV MEMBER_KPI_BASE
-- ============================================================================
-- Métricas consolidadas por membro para:
--   1. KPIs do dashboard (cards, rankings)
--   2. Feature store para modelo de ML de churn
--
-- Uma linha por membro com todos os indicadores calculados
-- ============================================================================

DROP MATERIALIZED VIEW IF EXISTS analytics.mv_member_kpi_base CASCADE;

CREATE MATERIALIZED VIEW analytics.mv_member_kpi_base AS
WITH
-- -----------------------------------------------------------------------
-- CTE 1: Métricas de spells por membro
-- -----------------------------------------------------------------------
spell_metrics AS (
    SELECT
        member_id,
        -- Totais
        COUNT(*)                                      AS total_spells,
        SUM(duration_days)                            AS total_days_active,
        SUM(contracts_in_spell)                       AS total_contracts,
        -- Spell mais recente
        MAX(spell_end)                                AS last_spell_end,
        MIN(spell_start)                              AS first_spell_start,
        -- Por segmento
        COUNT(*) FILTER (WHERE segmento = 'REGULAR')  AS spells_regular,
        COUNT(*) FILTER (WHERE segmento = 'AGREGADOR') AS spells_agregador,
        SUM(duration_days) FILTER (WHERE segmento = 'REGULAR')  AS days_regular,
        SUM(duration_days) FILTER (WHERE segmento = 'AGREGADOR') AS days_agregador,
        -- Ativo agora?
        bool_or(spell_status = 'active')              AS is_active,
        -- Segmento do spell ativo (ou último)
        (array_agg(segmento ORDER BY spell_end DESC))[1] AS current_segmento
    FROM analytics.mv_spells_v2
    GROUP BY member_id
),

-- -----------------------------------------------------------------------
-- CTE 2: Métricas de churn/evento por membro
-- -----------------------------------------------------------------------
churn_metrics AS (
    SELECT
        member_id,
        COUNT(*) FILTER (WHERE evento = 'CHURN')     AS total_churns,
        COUNT(*) FILTER (WHERE evento = 'MIGRACAO')   AS total_migracoes,
        -- Último evento
        (array_agg(evento ORDER BY spell_end DESC))[1] AS ultimo_evento,
        (array_agg(spell_end ORDER BY spell_end DESC))[1] AS ultimo_evento_date
    FROM analytics.mv_churn_events
    GROUP BY member_id
),

-- -----------------------------------------------------------------------
-- CTE 3: Frequência de acesso (entries) — apenas 2025+
-- -----------------------------------------------------------------------
freq_metrics AS (
    SELECT
        e.member_id,
        COUNT(*)                                      AS total_checkins_2025,
        COUNT(DISTINCT e.entry_date::DATE)            AS dias_com_checkin_2025,
        MAX(e.entry_date)                             AS last_checkin,
        -- Média de checkins por semana (2025+)
        CASE
            WHEN COUNT(DISTINCT e.entry_date::DATE) > 0
            THEN ROUND(
                COUNT(*)::NUMERIC /
                GREATEST(
                    (MAX(e.entry_date::DATE) - LEAST(MIN(e.entry_date::DATE), '2025-01-01'::DATE))::NUMERIC / 7,
                    1
                ),
                2
            )
            ELSE 0
        END                                           AS avg_checkins_per_week
    FROM core.evo_entries e
    WHERE e.branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[])
      AND e.entry_date >= '2025-01-01'
      AND e.entry_type = 'Controle de acesso'
    GROUP BY e.member_id
),

-- -----------------------------------------------------------------------
-- CTE 4: Receita do membro (receivables pagos) — apenas REGULAR
-- -----------------------------------------------------------------------
receita_metrics AS (
    SELECT
        r.member_id,
        SUM(r.amount_paid::NUMERIC)
            FILTER (WHERE r.status_conciliado = 'RECEBIDO')
                                                      AS total_pago,
        COUNT(*)
            FILTER (WHERE r.status_conciliado = 'RECEBIDO')
                                                      AS parcelas_pagas,
        SUM(r.amount::NUMERIC)
            FILTER (WHERE r.status_conciliado = 'EM ABERTO')
                                                      AS total_em_aberto,
        MAX(r.receiving_date)                         AS last_payment_date
    FROM ltv.mv_receivables_normalized r
    WHERE r.branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[])
    GROUP BY r.member_id
)

-- -----------------------------------------------------------------------
-- RESULTADO FINAL: Uma linha por membro
-- -----------------------------------------------------------------------
SELECT
    m.member_id,
    m.branch_id,
    m.branch_name,
    m.full_name,
    m.gender,
    m.birth_date,
    -- Idade calculada
    EXTRACT(YEAR FROM age('2026-02-10'::DATE, m.birth_date::DATE))::INT
                                                      AS idade,
    -- Faixa etária
    CASE
        WHEN EXTRACT(YEAR FROM age('2026-02-10'::DATE, m.birth_date::DATE)) < 18 THEN '<18'
        WHEN EXTRACT(YEAR FROM age('2026-02-10'::DATE, m.birth_date::DATE)) < 25 THEN '18-24'
        WHEN EXTRACT(YEAR FROM age('2026-02-10'::DATE, m.birth_date::DATE)) < 35 THEN '25-34'
        WHEN EXTRACT(YEAR FROM age('2026-02-10'::DATE, m.birth_date::DATE)) < 45 THEN '35-44'
        WHEN EXTRACT(YEAR FROM age('2026-02-10'::DATE, m.birth_date::DATE)) < 55 THEN '45-54'
        WHEN EXTRACT(YEAR FROM age('2026-02-10'::DATE, m.birth_date::DATE)) < 65 THEN '55-64'
        ELSE '65+'
    END                                               AS faixa_etaria,

    m.register_date,
    m.last_access_date,
    m.status                                          AS status_evo,

    -- Segmento atual
    COALESCE(sm.current_segmento, 'SEM_CONTRATO')     AS segmento_atual,
    COALESCE(sm.is_active, FALSE)                      AS is_active,

    -- Spells
    COALESCE(sm.total_spells, 0)                       AS total_spells,
    COALESCE(sm.total_days_active, 0)                  AS total_days_active,
    ROUND(COALESCE(sm.total_days_active, 0)::NUMERIC / 30.44, 1)
                                                       AS total_months_active,
    COALESCE(sm.total_contracts, 0)                    AS total_contracts,
    sm.first_spell_start,
    sm.last_spell_end,
    COALESCE(sm.spells_regular, 0)                     AS spells_regular,
    COALESCE(sm.spells_agregador, 0)                   AS spells_agregador,

    -- Churn
    COALESCE(cm.total_churns, 0)                       AS total_churns,
    COALESCE(cm.total_migracoes, 0)                    AS total_migracoes,
    cm.ultimo_evento,
    cm.ultimo_evento_date,

    -- Frequência
    COALESCE(fm.total_checkins_2025, 0)                AS total_checkins_2025,
    COALESCE(fm.dias_com_checkin_2025, 0)              AS dias_com_checkin_2025,
    fm.last_checkin,
    COALESCE(fm.avg_checkins_per_week, 0)              AS avg_checkins_per_week,

    -- Inatividade (dias sem checkin)
    CASE
        WHEN fm.last_checkin IS NOT NULL
        THEN ('2026-02-10'::DATE - fm.last_checkin::DATE)
        ELSE NULL
    END                                                AS dias_sem_checkin,

    -- Receita (apenas regular)
    COALESCE(rm.total_pago, 0)                         AS receita_total_paga,
    COALESCE(rm.parcelas_pagas, 0)                     AS parcelas_pagas,
    COALESCE(rm.total_em_aberto, 0)                    AS receita_em_aberto,
    rm.last_payment_date,
    -- Ticket médio mensal
    CASE
        WHEN COALESCE(sm.total_days_active, 0) > 0 AND COALESCE(rm.total_pago, 0) > 0
        THEN ROUND(rm.total_pago / (sm.total_days_active::NUMERIC / 30.44), 2)
        ELSE 0
    END                                                AS ticket_medio_mensal,

    -- LTV estimado (receita total paga = proxy de LTV para regulares)
    COALESCE(rm.total_pago, 0)                         AS ltv

FROM core.evo_members m
LEFT JOIN spell_metrics sm ON m.member_id = sm.member_id
LEFT JOIN churn_metrics cm ON m.member_id = cm.member_id
LEFT JOIN freq_metrics fm  ON m.member_id = fm.member_id
LEFT JOIN receita_metrics rm ON m.member_id = rm.member_id
WHERE m.branch_id = ANY('{345,181,59,233,401,166,33,6,149}'::BIGINT[]);

-- Índice único para REFRESH CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS idx_mkb_pk
ON analytics.mv_member_kpi_base (member_id);

-- Índices para filtros comuns
CREATE INDEX IF NOT EXISTS idx_mkb_branch_segmento_active
ON analytics.mv_member_kpi_base (branch_id, segmento_atual, is_active);

CREATE INDEX IF NOT EXISTS idx_mkb_ultimo_evento
ON analytics.mv_member_kpi_base (ultimo_evento)
WHERE ultimo_evento IS NOT NULL;


-- ============================================================================
-- VALIDAÇÃO 4A: Resumo geral por branch
-- ============================================================================

/*
-- VAL_4A: Dashboard de membros
SELECT
    branch_id,
    branch_name,
    segmento_atual,
    COUNT(*)                                          AS total_membros,
    COUNT(*) FILTER (WHERE is_active)                 AS ativos,
    ROUND(AVG(total_months_active), 1)                AS permanencia_media_meses,
    ROUND(AVG(avg_checkins_per_week) FILTER (WHERE is_active), 2)
                                                      AS freq_media_ativos,
    ROUND(AVG(receita_total_paga) FILTER (WHERE segmento_atual = 'REGULAR' AND receita_total_paga > 0), 2)
                                                      AS ticket_medio_regular,
    ROUND(AVG(total_churns), 2)                       AS media_churns_por_membro
FROM analytics.mv_member_kpi_base
GROUP BY branch_id, branch_name, segmento_atual
ORDER BY branch_id, segmento_atual;
*/

-- ============================================================================
-- VALIDAÇÃO 4B: Membros ativos sem checkin nos últimos 15 dias
-- (risco de churn iminente — insumo para régua de e-mail)
-- ============================================================================

/*
-- VAL_4B: Alerta de inatividade
SELECT
    member_id,
    full_name,
    branch_name,
    segmento_atual,
    last_checkin,
    dias_sem_checkin,
    avg_checkins_per_week,
    total_months_active
FROM analytics.mv_member_kpi_base
WHERE is_active = TRUE
  AND dias_sem_checkin > 15
ORDER BY dias_sem_checkin DESC
LIMIT 30;
*/


-- ============================================================================
-- SEÇÃO 5: ROTINA DE REFRESH
-- ============================================================================
-- Ordem importa: MV1 → MV2 → MV3 → MV4 (dependências em cascata)
-- Usar CONCURRENTLY para não bloquear reads durante refresh
--
-- Agendar via pg_cron ou Azure Functions (diário, ~4h da manhã)
-- ============================================================================

/*
-- REFRESH COMPLETO (executar nesta ordem)
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_contract_classified;
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_spells_v2;
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_churn_events;
REFRESH MATERIALIZED VIEW CONCURRENTLY analytics.mv_member_kpi_base;

-- Para primeiro build (sem CONCURRENTLY pois não tem dados ainda)
REFRESH MATERIALIZED VIEW analytics.mv_contract_classified;
REFRESH MATERIALIZED VIEW analytics.mv_spells_v2;
REFRESH MATERIALIZED VIEW analytics.mv_churn_events;
REFRESH MATERIALIZED VIEW analytics.mv_member_kpi_base;
*/


-- ============================================================================
-- FIM DO SCRIPT
-- ============================================================================
-- Próximos passos:
--   1. Executar este script no Azure PostgreSQL
--   2. Rodar os blocos de REFRESH (SEÇÃO 5)
--   3. Executar cada query de validação (VAL_*) e conferir resultados
--   4. Ajustar Azure Functions para consumir as novas MVs
--   5. Implementar endpoint de churn no function_app.py
-- ============================================================================
