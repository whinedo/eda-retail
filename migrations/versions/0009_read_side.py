"""Lado de lectura sobre Kafka (T3, odd/tasks/eda-component-ownership.md).

- `event_log.events`: la vista del log de eventos consumido de Kafka, solo de añadir, con un `seq`
  creciente en el orden de consumo y la partición, el offset y el contexto de traza de cada mensaje.
- `views.*`: las vistas de los proyectores, ahora alimentadas desde Kafka, más `view_orders`.
- `<esquema>.offsets`: último offset aplicado por consumidor, topic y partición, que se escribe en
  la misma transacción que la vista.
- `views.applied_events`: eventos ya aplicados por cada proyector; un evento que se vuelve a
  publicar (mismo `event_id`, offset nuevo) no se aplica dos veces.

Es aditiva: las vistas y `projector_offsets` del esquema público se quedan, sin uso, hasta T10.

Revision ID: 0009
Revises: 0008
"""

from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def _offsets(schema: str) -> str:
    return f"""
CREATE TABLE {schema}.offsets (
    consumer        TEXT    NOT NULL,
    kafka_topic     TEXT    NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset    BIGINT  NOT NULL,
    PRIMARY KEY (consumer, kafka_topic, kafka_partition)
)"""


STATEMENTS = (
    """
CREATE TABLE event_log.events (
    seq             BIGSERIAL PRIMARY KEY,
    event_id        UUID        NOT NULL UNIQUE,
    event_type      TEXT        NOT NULL,
    event_version   INTEGER     NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    correlation_id  TEXT        NOT NULL,
    causation_id    UUID,
    producer        TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    kafka_topic     TEXT        NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    trace_context   JSONB,
    consumed_at     TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    "CREATE INDEX events_correlation_idx ON event_log.events (correlation_id, seq)",
    "CREATE INDEX events_type_idx ON event_log.events (event_type, seq)",
    """
CREATE FUNCTION event_log.reject_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'la tabla event_log.events es de solo añadir (% rechazado)', TG_OP;
END;
$$ LANGUAGE plpgsql""",
    """
CREATE TRIGGER events_no_update_delete BEFORE UPDATE OR DELETE ON event_log.events
    FOR EACH ROW EXECUTE FUNCTION event_log.reject_mutation()""",
    """
CREATE TRIGGER events_no_truncate BEFORE TRUNCATE ON event_log.events
    FOR EACH STATEMENT EXECUTE FUNCTION event_log.reject_mutation()""",
    _offsets("event_log"),
    _offsets("views"),
    """
CREATE TABLE views.applied_events (
    projector TEXT NOT NULL,
    event_id  UUID NOT NULL,
    PRIMARY KEY (projector, event_id)
)""",
    """
CREATE TABLE views.view_orders (
    order_id     TEXT PRIMARY KEY,
    customer_ref TEXT           NOT NULL,
    amount       NUMERIC(12, 2) NOT NULL
)""",
    """
CREATE TABLE views.view_incident_status (
    incident_id        TEXT PRIMARY KEY,
    status             TEXT        NOT NULL,
    plan_version       INTEGER     NOT NULL DEFAULT 0,
    steps              JSONB       NOT NULL DEFAULT '[]',
    escalation_reason  TEXT,
    last_applied_event UUID,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    """
CREATE TABLE views.view_cost_per_incident (
    incident_id    TEXT PRIMARY KEY,
    llm_calls      INTEGER        NOT NULL DEFAULT 0,
    tokens_in      BIGINT         NOT NULL DEFAULT 0,
    tokens_out     BIGINT         NOT NULL DEFAULT 0,
    claims         INTEGER        NOT NULL DEFAULT 0,
    reissues       INTEGER        NOT NULL DEFAULT 0,
    refunds_amount NUMERIC(12, 2) NOT NULL DEFAULT 0
)""",
    """
CREATE TABLE views.view_audit_trail (
    event_id        UUID PRIMARY KEY,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    incident_id     TEXT        NOT NULL,
    plan_version    INTEGER,
    actor           TEXT        NOT NULL,
    decision        TEXT        NOT NULL,
    reason          TEXT,
    occurred_at     TIMESTAMPTZ NOT NULL
)""",
    "CREATE INDEX view_audit_incident_idx ON views.view_audit_trail (incident_id, kafka_partition, kafka_offset)",
    """
CREATE TABLE views.view_carrier_dashboard (
    carrier        TEXT PRIMARY KEY,
    incidents      INTEGER NOT NULL DEFAULT 0,
    delays         INTEGER NOT NULL DEFAULT 0,
    claims_filed   INTEGER NOT NULL DEFAULT 0,
    refunds_issued INTEGER NOT NULL DEFAULT 0
)""",
    """
CREATE TABLE views.view_carrier_incidents (
    incident_id TEXT PRIMARY KEY,
    carrier     TEXT NOT NULL
)""",
)

DROPPED = (
    "views.view_carrier_incidents", "views.view_carrier_dashboard", "views.view_audit_trail",
    "views.view_cost_per_incident", "views.view_incident_status", "views.view_orders",
    "views.applied_events", "views.offsets", "event_log.offsets", "event_log.events",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for table in DROPPED:
        op.execute(f"DROP TABLE {table}")
    op.execute("DROP FUNCTION event_log.reject_mutation()")
