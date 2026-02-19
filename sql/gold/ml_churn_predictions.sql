-- ============================================================================
-- SKYFIT ML — PREDICTIONS OUTPUT TABLE
-- ============================================================================
-- This table stores daily churn predictions for all active members.
-- Updated by the Airflow scoring pipeline each morning.
--
-- Consumers:
--   - Azure Functions API (endpoints for Lovable dashboard)
--   - Monitoring pipeline (drift detection)
--   - Playbook assignment engine
--
-- Data de criacao: 2026-02-19
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS ml;

-- ============================================================================
-- PREDICTIONS TABLE (latest scores only — one row per active member)
-- ============================================================================

DROP TABLE IF EXISTS ml.churn_predictions CASCADE;

CREATE TABLE ml.churn_predictions (
    prediction_id       BIGSERIAL PRIMARY KEY,
    member_id           BIGINT NOT NULL,
    branch_id           INT NOT NULL,
    scored_at           TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Model output
    churn_probability   NUMERIC(5, 4) NOT NULL CHECK (churn_probability BETWEEN 0 AND 1),
    risk_tier           TEXT NOT NULL CHECK (risk_tier IN ('HIGH', 'MEDIUM', 'LOW')),

    -- Churn type classification
    churn_type          TEXT NOT NULL CHECK (churn_type IN ('BEHAVIORAL', 'FINANCIAL', 'DEFAULT', 'FULL', 'NONE')),
    -- BEHAVIORAL: paying but not attending (10+ days absent, active contract) (D9)
    -- FINANCIAL:  has open receivable but not yet in default
    -- DEFAULT:    non-payment -> blocked at turnstile (D18). Absence is FORCED.
    --             Monthly contract failed to renew due to non-payment (D17).
    -- FULL:       both behavioral and financial signals present
    -- NONE:       low risk, no immediate signals

    -- SHAP explanations (pre-computed, plain language)
    top_3_reasons       JSONB NOT NULL,
    -- Example: [
    --   {"feature": "days_since_last_checkin", "value": 18, "impact": 0.23,
    --    "explanation": "Sem check-in ha 18 dias (media da academia: 3 dias)"},
    --   {"feature": "contract_expiring_30d", "value": true, "impact": 0.18,
    --    "explanation": "Contrato expira em 12 dias"},
    --   {"feature": "checkin_trend", "value": 0.3, "impact": 0.15,
    --    "explanation": "Frequencia caiu 70% nas ultimas 2 semanas"}
    -- ]

    -- Playbook assignment
    playbook_id         TEXT NOT NULL,
    -- Mapped from risk_tier + churn_type combination

    -- Quick-reference context for gym manager
    days_until_contract_end  INT,
    last_checkin_date        DATE,
    days_since_last_checkin  INT,
    avg_weekly_checkins      NUMERIC(5, 2),
    segmento                 TEXT,

    -- Model versioning
    model_version       TEXT NOT NULL,

    -- Partition key for efficient cleanup
    score_date          DATE NOT NULL DEFAULT CURRENT_DATE
);

-- Unique constraint: one prediction per member per day
CREATE UNIQUE INDEX IF NOT EXISTS idx_pred_member_date
ON ml.churn_predictions (member_id, score_date);

-- API query patterns
CREATE INDEX IF NOT EXISTS idx_pred_branch_tier
ON ml.churn_predictions (branch_id, risk_tier, score_date);

CREATE INDEX IF NOT EXISTS idx_pred_branch_type
ON ml.churn_predictions (branch_id, churn_type, score_date);

CREATE INDEX IF NOT EXISTS idx_pred_playbook
ON ml.churn_predictions (playbook_id, score_date);

CREATE INDEX IF NOT EXISTS idx_pred_score_date
ON ml.churn_predictions (score_date);


-- ============================================================================
-- PREDICTIONS HISTORY (append-only, for drift analysis)
-- ============================================================================

DROP TABLE IF EXISTS ml.churn_predictions_history CASCADE;

CREATE TABLE ml.churn_predictions_history (
    prediction_id       BIGSERIAL PRIMARY KEY,
    member_id           BIGINT NOT NULL,
    branch_id           INT NOT NULL,
    scored_at           TIMESTAMP NOT NULL,
    score_date          DATE NOT NULL,
    churn_probability   NUMERIC(5, 4) NOT NULL,
    risk_tier           TEXT NOT NULL,
    churn_type          TEXT NOT NULL,
    model_version       TEXT NOT NULL,
    -- Actual outcome (filled retroactively by monitoring pipeline)
    actual_churned      BOOLEAN,
    outcome_verified_at DATE
);

CREATE INDEX IF NOT EXISTS idx_pred_hist_member_date
ON ml.churn_predictions_history (member_id, score_date);

CREATE INDEX IF NOT EXISTS idx_pred_hist_model_date
ON ml.churn_predictions_history (model_version, score_date);


-- ============================================================================
-- PLAYBOOK DEFINITIONS
-- ============================================================================

DROP TABLE IF EXISTS ml.playbooks CASCADE;

CREATE TABLE ml.playbooks (
    playbook_id         TEXT PRIMARY KEY,
    name                TEXT NOT NULL,
    description         TEXT NOT NULL,
    risk_tier           TEXT NOT NULL,
    churn_type          TEXT NOT NULL,
    actions             JSONB NOT NULL,
    -- Example actions:
    -- [
    --   {"step": 1, "type": "whatsapp", "template": "comeback_gentle",
    --    "delay_days": 0, "description": "Mensagem amigavel de boas-vindas"},
    --   {"step": 2, "type": "email", "template": "value_reminder",
    --    "delay_days": 3, "description": "Email lembrando beneficios"},
    --   {"step": 3, "type": "call", "assigned_to": "manager",
    --    "delay_days": 7, "description": "Ligacao do gerente"}
    -- ]
    priority            INT NOT NULL DEFAULT 0,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- DEFAULT PLAYBOOKS
-- ============================================================================

INSERT INTO ml.playbooks (playbook_id, name, description, risk_tier, churn_type, actions, priority)
VALUES
    ('PB_HIGH_BEHAVIORAL', 'Resgate Urgente — Pagando mas Ausente',
     'Membro paga mas nao frequenta ha 10+ dias. Alta probabilidade de cancelar na proxima renovacao.',
     'HIGH', 'BEHAVIORAL',
     '[
        {"step": 1, "type": "whatsapp", "template": "miss_you", "delay_days": 0,
         "description": "WhatsApp: Sentimos sua falta! Que tal um treino diferente esta semana?"},
        {"step": 2, "type": "email", "template": "new_classes", "delay_days": 2,
         "description": "Email: Novas aulas e horarios disponiveis"},
        {"step": 3, "type": "call", "assigned_to": "manager", "delay_days": 5,
         "description": "Ligacao do gerente: oferecer avaliacao fisica gratuita"},
        {"step": 4, "type": "offer", "template": "personal_trial", "delay_days": 7,
         "description": "Oferta: 1 aula experimental com personal trainer"}
     ]'::JSONB, 100),

    ('PB_HIGH_FINANCIAL', 'Retencao Critica — Contrato Expirando',
     'Contrato expira em breve e frequencia ja caiu. Risco alto de nao renovar.',
     'HIGH', 'FINANCIAL',
     '[
        {"step": 1, "type": "whatsapp", "template": "renewal_reminder", "delay_days": 0,
         "description": "WhatsApp: Seu plano vence em X dias. Renovar com condicoes especiais?"},
        {"step": 2, "type": "call", "assigned_to": "manager", "delay_days": 2,
         "description": "Ligacao do gerente: entender motivo da queda de frequencia"},
        {"step": 3, "type": "offer", "template": "loyalty_discount", "delay_days": 4,
         "description": "Oferta: desconto fidelidade na renovacao (5-10%)"},
        {"step": 4, "type": "email", "template": "last_chance", "delay_days": 7,
         "description": "Email: ultima oportunidade de renovar com desconto"}
     ]'::JSONB, 90),

    ('PB_HIGH_FULL', 'Emergencia — Todos os Sinais de Saida',
     'Membro ausente + contrato expirando. Maior risco de perda.',
     'HIGH', 'FULL',
     '[
        {"step": 1, "type": "call", "assigned_to": "manager", "delay_days": 0,
         "description": "Ligacao imediata do gerente: conversa pessoal"},
        {"step": 2, "type": "offer", "template": "rescue_package", "delay_days": 1,
         "description": "Oferta personalizada: congela + desconto + personal"},
        {"step": 3, "type": "whatsapp", "template": "we_care", "delay_days": 3,
         "description": "WhatsApp: mensagem personalizada do gerente"}
     ]'::JSONB, 100),

    ('PB_MEDIUM_BEHAVIORAL', 'Engajamento — Frequencia Caindo',
     'Membro ativo mas com frequencia em queda. Intervencao leve.',
     'MEDIUM', 'BEHAVIORAL',
     '[
        {"step": 1, "type": "push", "template": "workout_suggestion", "delay_days": 0,
         "description": "Push: Sugestao de treino novo baseado no historico"},
        {"step": 2, "type": "email", "template": "community_events", "delay_days": 5,
         "description": "Email: Eventos e desafios da comunidade"},
        {"step": 3, "type": "whatsapp", "template": "gentle_nudge", "delay_days": 10,
         "description": "WhatsApp: Que tal voltar a treinar? Temos novidades!"}
     ]'::JSONB, 50),

    ('PB_MEDIUM_FINANCIAL', 'Prevencao — Contrato Expirando em Breve',
     'Contrato vence em 15-30 dias. Frequencia ok mas precisa renovar.',
     'MEDIUM', 'FINANCIAL',
     '[
        {"step": 1, "type": "email", "template": "renewal_early", "delay_days": 0,
         "description": "Email: Renove agora e garanta as mesmas condicoes"},
        {"step": 2, "type": "whatsapp", "template": "renewal_benefits", "delay_days": 7,
         "description": "WhatsApp: Beneficios de renovar antes do vencimento"}
     ]'::JSONB, 40),

    ('PB_LOW_ACTIVE', 'Monitoramento — Membro Saudavel',
     'Membro ativo e engajado. Apenas monitorar.',
     'LOW', 'NONE',
     '[
        {"step": 1, "type": "none", "delay_days": 0,
         "description": "Nenhuma acao necessaria. Monitorar score diariamente."}
     ]'::JSONB, 0);
