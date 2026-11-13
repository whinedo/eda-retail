-- Esquema de la PoC EDA (incidencias logísticas).
-- Se ejecuta una sola vez al crear el volumen de PostgreSQL.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ---------------------------------------------------------------
-- Log de eventos: fuente de verdad, solo de añadir
-- ---------------------------------------------------------------
CREATE TABLE events (
    seq            BIGSERIAL PRIMARY KEY,
    event_id       UUID        NOT NULL UNIQUE,
    event_type     TEXT        NOT NULL,
    event_version  INTEGER     NOT NULL DEFAULT 1,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    correlation_id TEXT        NOT NULL,
    causation_id   UUID,
    producer       TEXT        NOT NULL,
    payload        JSONB       NOT NULL
);
CREATE INDEX events_correlation_idx ON events (correlation_id, seq);
CREATE INDEX events_type_idx ON events (event_type, seq);

CREATE FUNCTION events_reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'la tabla events es de solo añadir (% rechazado)', TG_OP;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER events_no_update_delete
    BEFORE UPDATE OR DELETE ON events
    FOR EACH ROW EXECUTE FUNCTION events_reject_mutation();

CREATE TRIGGER events_no_truncate
    BEFORE TRUNCATE ON events
    FOR EACH STATEMENT EXECUTE FUNCTION events_reject_mutation();

-- ---------------------------------------------------------------
-- Outbox: estado de publicación a Kafka (events permanece inmutable)
-- ---------------------------------------------------------------
CREATE TABLE outbox (
    seq          BIGINT PRIMARY KEY REFERENCES events (seq),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts     INTEGER     NOT NULL DEFAULT 0
);
CREATE INDEX outbox_pending_idx ON outbox (seq) WHERE published_at IS NULL;

-- ---------------------------------------------------------------
-- Inbox: deduplicación de eventos por consumidor
-- ---------------------------------------------------------------
CREATE TABLE inbox (
    consumer     TEXT        NOT NULL,
    event_id     UUID        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);

-- ---------------------------------------------------------------
-- Claves de idempotencia de los efectos
-- ---------------------------------------------------------------
CREATE TABLE effect_keys (
    idempotency_key TEXT PRIMARY KEY,
    incident_id     TEXT        NOT NULL,
    step_id         TEXT        NOT NULL,
    action          TEXT        NOT NULL,
    status          TEXT        NOT NULL CHECK (status IN ('in_progress', 'completed')),
    result          JSONB,
    locked_until    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);
CREATE INDEX effect_keys_incident_idx ON effect_keys (incident_id);

-- ---------------------------------------------------------------
-- Dead letters
-- ---------------------------------------------------------------
CREATE TABLE dead_letters (
    id             BIGSERIAL PRIMARY KEY,
    consumer       TEXT        NOT NULL,
    event_id       UUID,
    correlation_id TEXT,
    reason         TEXT        NOT NULL,
    raw_message    JSONB,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------
-- Estado de escritura del command handler (se actualiza en la misma
-- transacción que los eventos)
-- ---------------------------------------------------------------
CREATE TABLE incidents (
    incident_id       TEXT PRIMARY KEY,
    status            TEXT        NOT NULL CHECK (status IN ('open', 'resolved', 'escalated')),
    severity          TEXT        NOT NULL,
    cause             TEXT        NOT NULL,
    version           INTEGER     NOT NULL DEFAULT 0,   -- concurrencia optimista (expected_version)
    plan_version      INTEGER     NOT NULL DEFAULT 0,
    revisions         INTEGER     NOT NULL DEFAULT 0,
    tokens_used       BIGINT      NOT NULL DEFAULT 0,
    llm_calls         INTEGER     NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at         TIMESTAMPTZ
);

CREATE TABLE carrier_notices (
    carrier_notice_id TEXT PRIMARY KEY,
    incident_id       TEXT,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE plan_steps (
    step_id              TEXT PRIMARY KEY,
    incident_id          TEXT        NOT NULL REFERENCES incidents (incident_id),
    origin_plan_version  INTEGER     NOT NULL,
    action               TEXT        NOT NULL,
    params               JSONB       NOT NULL DEFAULT '{}',
    depends_on           TEXT[]      NOT NULL DEFAULT '{}',
    compensates          TEXT REFERENCES plan_steps (step_id),   -- paso original si es una compensación (-comp)
    status               TEXT        NOT NULL DEFAULT 'pending'
                         CHECK (status IN ('pending', 'requested', 'completed', 'failed',
                                      'cancelled', 'compensating', 'compensated'))
);
CREATE INDEX plan_steps_incident_idx ON plan_steps (incident_id);

-- ---------------------------------------------------------------
-- Datos de negocio simulados
-- ---------------------------------------------------------------
CREATE TABLE orders (
    order_id     TEXT PRIMARY KEY,
    customer_ref TEXT        NOT NULL,
    sku          TEXT        NOT NULL,
    quantity     INTEGER     NOT NULL,
    amount       NUMERIC(10, 2) NOT NULL,
    carrier      TEXT        NOT NULL,
    status       TEXT        NOT NULL DEFAULT 'shipped'
);

CREATE TABLE stock (
    warehouse TEXT    NOT NULL,
    sku       TEXT    NOT NULL,
    available INTEGER NOT NULL CHECK (available >= 0),
    PRIMARY KEY (warehouse, sku)
);

CREATE TABLE reservations (
    reservation_id TEXT PRIMARY KEY,
    order_id       TEXT    NOT NULL,
    warehouse      TEXT    NOT NULL,
    sku            TEXT    NOT NULL,
    quantity       INTEGER NOT NULL,
    status         TEXT    NOT NULL CHECK (status IN ('active', 'released'))
);

CREATE TABLE claims (
    claim_id    TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    order_id    TEXT NOT NULL,
    UNIQUE (incident_id, order_id)
);

CREATE TABLE refunds (
    refund_id TEXT PRIMARY KEY,
    order_id  TEXT NOT NULL,
    step_id   TEXT NOT NULL UNIQUE,
    amount    NUMERIC(10, 2) NOT NULL
);

CREATE TABLE customer_messages (
    message_id  TEXT PRIMARY KEY,
    order_id    TEXT NOT NULL,
    step_id     TEXT NOT NULL UNIQUE,
    sent_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------
-- Vistas CQRS (read models) y posición de los proyectores
-- ---------------------------------------------------------------
CREATE TABLE projector_offsets (
    projector TEXT PRIMARY KEY,
    last_seq  BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE view_incident_status (
    incident_id          TEXT PRIMARY KEY,
    status               TEXT        NOT NULL,
    plan_version         INTEGER     NOT NULL DEFAULT 0,
    steps                JSONB       NOT NULL DEFAULT '[]',
    escalation_reason    TEXT,
    last_applied_event   UUID,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE view_cost_per_incident (
    incident_id    TEXT PRIMARY KEY,
    llm_calls      INTEGER NOT NULL DEFAULT 0,
    tokens_in      BIGINT  NOT NULL DEFAULT 0,
    tokens_out     BIGINT  NOT NULL DEFAULT 0,
    llm_cost       NUMERIC(12, 6) NOT NULL DEFAULT 0,
    effects_cost   NUMERIC(12, 2) NOT NULL DEFAULT 0
);

CREATE TABLE view_audit_trail (
    seq          BIGINT PRIMARY KEY,
    incident_id  TEXT        NOT NULL,
    plan_version INTEGER,
    actor        TEXT        NOT NULL,
    decision     TEXT        NOT NULL,
    reason       TEXT,
    occurred_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX view_audit_incident_idx ON view_audit_trail (incident_id, seq);

CREATE TABLE view_carrier_dashboard (
    carrier        TEXT PRIMARY KEY,
    delays         INTEGER NOT NULL DEFAULT 0,
    claims_filed   INTEGER NOT NULL DEFAULT 0,
    refunds_issued INTEGER NOT NULL DEFAULT 0
);
