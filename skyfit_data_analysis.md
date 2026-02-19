# SkyFit Analytics — Análise de Dados & Arquitetura de Churn

**Data:** 2026-02-12 | **Versão:** 1.0 | **Escopo:** Branches MVP [345, 181, 59, 233, 401, 166, 33, 6, 149]

---

## 1. Mapeamento das Tabelas (EVO → PostgreSQL)

### 1.1 `core.evo_members` — Cadastro do Aluno
| Coluna | Tipo | Relevância |
|--------|------|------------|
| `member_id` | PK | Identificador único |
| `branch_id` | FK | Unidade de origem |
| `gympass_id` | TEXT | **AGREGADOR** se ≠ NULL |
| `code_totalpass` | TEXT | **AGREGADOR** se ≠ NULL |
| `user_id_gurupass` | TEXT | Sem dados na amostra |
| `status` | TEXT | Active / Inactive (snapshot atual) |
| `register_date` | TIMESTAMP | Data de cadastro |
| `conversion_date` | TIMESTAMP | Data de conversão prospect → aluno |
| `last_access_date` | TIMESTAMP | Último acesso registrado |

### 1.2 `core.evo_member_memberships` — Contratos (Cada Renovação = Nova Linha)
| Coluna | Tipo | Relevância |
|--------|------|------------|
| `member_membership_id` | PK | ID do contrato |
| `member_id` | FK | Vínculo com aluno |
| `membership_name` | TEXT | Nome do plano — **usado para classificar agregador** |
| `start_date` | DATE | Início do contrato |
| `end_date` | DATE | Fim programado do contrato |
| `membership_status` | TEXT | `active` / `expired` / `canceled` |
| `cancel_date` | TIMESTAMP | Data do cancelamento antecipado (NULL se expirou normalmente) |
| `value_next_month` | NUMERIC | ⚠️ **100% NULL na amostra** — NÃO usar para receita |
| `sale_id` | FK | Vínculo com venda |

### 1.3 `core.evo_entries` — Check-ins / Acessos
| Coluna | Tipo | Relevância |
|--------|------|------------|
| `entry_id` | PK | Hash único |
| `member_id` | FK | Aluno |
| `branch_id` | INT | Unidade de acesso (pode diferir da unidade de cadastro) |
| `entry_date` | TIMESTAMP | Data/hora do acesso |
| `block_reason` | TEXT | **CRÍTICO para classificação de agregador** |
| `entry_type` | TEXT | Controle de acesso / Impressão de treino |

### 1.4 `core.evo_sales` + `core.evo_sale_items` — Vendas
| Tabela | Coluna Chave | Observação |
|--------|-------------|------------|
| sales | `total_value`, `discount_value` | Valor bruto e desconto |
| sales | `removed` | Se TRUE, venda anulada |
| sale_items | `item_type` | `Membership` (970/1000 na amostra) |
| sale_items | `item_name` | Nome do plano vendido |

### 1.5 `ltv.mv_receivables_normalized` — Receita Real (Fonte Primária)
| Coluna | Tipo | Relevância |
|--------|------|------------|
| `amount` | NUMERIC | Valor devido |
| `amount_paid` | NUMERIC | Valor pago |
| `status_conciliado` | TEXT | RECEBIDO (95.7%) / EM ABERTO (4.3%) |
| `reference_date` | DATE | Data de referência do pagamento |

### 1.6 `ltv.mv_regular_spells` — Spells Atuais (COM PROBLEMAS)
| Coluna | Observação |
|--------|------------|
| `spell_start` / `spell_end` | Períodos contínuos de contrato |
| `membership_names` | Array de nomes — **mistura regular + agregador** |

---

## 2. Problemas Críticos Encontrados

### 🔴 P1: Classificação de Agregador Incompleta
**Estado atual:** Só usa `membership_name ILIKE '%gympass%|totalpass%|wellhub%'`

**O que falta:**
- `members.gympass_id IS NOT NULL` → 395/1000 na amostra
- `members.code_totalpass IS NOT NULL` → 87/1000 na amostra
- `entries.block_reason` com padrões de diária → 117 entradas "Diária validada com sucesso"
- Membros com `block_reason = 'Diária validada com sucesso'` **NÃO TÊM** gympass_id/code_totalpass (na amostra)

**Decisão do usuário:** "Diária validada com sucesso" (sem plataforma) = **AGREGADOR**

### 🔴 P2: `value_next_month` é 100% NULL
A Azure Function `planos-ranking` usa `SUM(value_next_month)` para receita → **retorna sempre zero**.
Receita real deve vir de `ltv.mv_receivables_normalized` (tabela de recebíveis).

### 🔴 P3: Spells Mistura Regular + Agregador
Membro 116888 — exemplo real da amostra:
```
2023-04-18 → 2023-08-17  PLANO PROMOCIONAL... (REGULAR, 5 renovações)
2023-08-17 → canceled
2023-09-20 → 2024-04-19  CONTRATO GYMPASS (AGREGADOR, 7 renovações)
```
**A mv_regular_spells atual agrupa tudo como um único spell**, sem distinguir que houve uma **migração de segmento**. Isso polui as métricas de duração e churn.

### 🟡 P4: Branch 149 Ausente no Default Anterior
O array antigo era `[345, 181, 59, 233, 401, 166, 33, 6]`. O novo inclui **149**.

### 🟡 P5: SQL Injection na Azure Function
Endpoints `vendas-planos` e `ltv` usam f-strings para montar queries:
```python
seg_filter = f"segmento = '{segmento}'"  # ⚠️ VULNERÁVEL
```
Deve usar queries parametrizadas.

---

## 3. Regras de Negócio Consolidadas

### 3.1 Classificação de Contrato (Nível CONTRATO, não membro)

```
NÍVEL 1 — Pelo nome do contrato (membership_name):
  LOWER(membership_name) ~ '(gympass|totalpass|wellhub)' → AGREGADOR

NÍVEL 2 — Pelo membro (members):
  gympass_id IS NOT NULL OR code_totalpass IS NOT NULL → flag no membro

NÍVEL 3 — Pela entrada (entries.block_reason):
  ILIKE '%gympass%' OR ILIKE '%totalpass%' OR ILIKE '%wellhub%'
  OR = 'Diária validada com sucesso' → AGREGADOR
```

**Proposta de lógica hierárquica por contrato:**
1. Se `membership_name` contém gympass/totalpass/wellhub → AGREGADOR
2. Senão, se o membro tem `gympass_id` ou `code_totalpass` preenchido E o período do contrato coincide com entradas de agregador → AGREGADOR
3. Default → REGULAR

### 3.2 Definição de Churn (30 dias sem contrato)

```
data_efetiva_fim = CASE
    WHEN cancel_date IS NOT NULL AND cancel_date < end_date THEN cancel_date::DATE
    ELSE end_date
END

proximo_contrato = próximo start_date do MESMO segmento para o mesmo membro

churn = CASE
    WHEN proximo_contrato IS NULL AND (DATA_REFERENCIA - data_efetiva_fim) > 30 → TRUE
    WHEN proximo_contrato IS NOT NULL AND (proximo_contrato - data_efetiva_fim) > 30 → TRUE
    ELSE FALSE
END
```

### 3.3 Migração ≠ Churn
- Regular → Agregador em < 30 dias → **Migração** (não é churn regular)
- Agregador → Regular em < 30 dias → **Migração** (não é churn agregador)
- Regular → Nenhum contrato > 30 dias → **Churn Regular**

### 3.4 Definição de Spell (com segregação de segmento)

Um spell é um período **contínuo** de contratos do **mesmo segmento** (regular OU agregador) com gap ≤ 30 dias entre eles.

Se o membro muda de segmento, o spell atual encerra e um novo spell começa no novo segmento.

---

## 4. Arquitetura Proposta — Nova MV de Spells + Churn

### Passo 1: CTE de classificação de contratos
```sql
contract_classified AS (
    SELECT
        mm.*,
        m.branch_id,
        m.gympass_id,
        m.code_totalpass,
        CASE
            WHEN LOWER(mm.membership_name) ~ '(gympass|totalpass|wellhub)' THEN 'AGREGADOR'
            WHEN m.gympass_id IS NOT NULL OR m.code_totalpass IS NOT NULL THEN
                -- Verificar se há entradas de agregador no período do contrato
                CASE WHEN EXISTS (
                    SELECT 1 FROM core.evo_entries e
                    WHERE e.member_id = mm.member_id
                      AND e.entry_date::DATE BETWEEN mm.start_date AND mm.end_date
                      AND (
                          LOWER(e.block_reason) LIKE '%gympass%'
                          OR LOWER(e.block_reason) LIKE '%totalpass%'
                          OR LOWER(e.block_reason) LIKE '%wellhub%'
                          OR e.block_reason = 'Diária validada com sucesso'
                      )
                ) THEN 'AGREGADOR' ELSE 'REGULAR' END
            ELSE 'REGULAR'
        END AS segmento,
        CASE
            WHEN mm.cancel_date IS NOT NULL
                 AND mm.cancel_date::DATE < mm.end_date
            THEN mm.cancel_date::DATE
            ELSE mm.end_date
        END AS data_efetiva_fim
    FROM core.evo_member_memberships mm
    JOIN core.evo_members m ON mm.member_id = m.member_id
    WHERE m.branch_id = ANY('{345,181,59,233,401,166,33,6,149}')
)
```

### Passo 2: Detecção de gaps entre contratos do mesmo segmento
### Passo 3: Agrupamento em spells
### Passo 4: Flag de churn (30 dias sem contrato no segmento)

---

## 5. Perguntas Abertas (Precisam de Resposta para Prosseguir)

### ❓ Q1: Classificação NÍVEL 2 — Custo vs Precisão
A lógica de cruzar `gympass_id IS NOT NULL` com entradas no período do contrato é precisa, mas faz um `EXISTS` por contrato. Para 130M+ de registros, isso pode ser caro.

**Alternativa simplificada:**
- Se `membership_name` é agregador → AGREGADOR
- Se `membership_name` NÃO é agregador → REGULAR
- Ignorar gympass_id/code_totalpass para classificação de contrato

**Pergunta:** Existem contratos com nomes "regulares" (ex: "PLANO PRIME RECORRENTE") que pertencem a membros agregadores que NUNCA tiveram contrato com nome de agregador? Se sim, como classificá-los?

### ❓ Q2: Receita do Agregador
Contratos de agregador (Gympass, TotalPass) tipicamente têm `value_next_month = NULL` e podem ter R$0 nos recebíveis. A receita do agregador vem de um repasse B2B que não está nessa base?

**Pergunta:** Devo calcular receita apenas para REGULAR e tratar receita de agregador como R$0? Ou existe outra fonte de dados para receita de agregador?

### ❓ Q3: Spell existente — Recriar ou Adaptar?
A `ltv.mv_regular_spells` atual:
- NÃO segrega por segmento
- Cobre apenas branches [1, 2, 65, 186] na amostra (nenhum dos branches MVP)

**Pergunta:** Devo criar uma **nova MV** (`analytics.mv_spells_segmented`) do zero? Ou a mv_regular_spells é usada em outros lugares e preciso manter retrocompatibilidade?

### ❓ Q4: Tabelas analytics.* — Já Existem?
O Azure Function referencia tabelas como `analytics.vendas_mensal`, `analytics.ativos_mensal`, `analytics.funil_mensal`, etc. Elas existem no banco ou ainda precisam ser criadas?

**Pergunta:** Essas tabelas já estão populadas ou preciso criá-las como parte deste projeto?

---

## 6. Próximos Passos (Após Respostas)

1. **SQL: Nova Materialized View de Spells** — `analytics.mv_spells_v2` com segregação regular/agregador
2. **SQL: Materialized View de Churn** — `analytics.mv_churn_events` com flag de churn, migração, data efetiva
3. **SQL: Queries de validação** — Para cada MV, query de auditoria cruzando com dados raw
4. **SQL: Índices otimizados** — Para suportar as novas MVs em 130M+ registros
5. **Azure Function: Endpoint de Churn** — Parametrizado, sem SQL injection
6. **Feature Store para ML** — Tabela com features por membro para modelo de predição
