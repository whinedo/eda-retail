"""Vistas del proyector `operations` (spec incident-metrics): `view_incident_metrics` (una fila por incidencia con sus
hechos operativos), las tablas de hechos `view_llm_calls`, `view_step_failures` y `view_carrier_consults` (una fila
por evento, con el `event_id` como clave) y `view_operations_steps`, la acción de cada paso pedido, porque
`StepFailed` no la lleva. Solo identificadores, códigos y números: ningún texto libre.

Revision ID: 0026
Revises: 0025
"""

from alembic import op

revision = "0026"
down_revision = "0025"
branch_labels = None
depends_on = None

TABLES = ("view_incident_metrics", "view_llm_calls", "view_step_failures", "view_carrier_consults",
          "view_operations_steps")

STATEMENTS = (
    """
CREATE TABLE views.view_incident_metrics (
    incident_id       TEXT        PRIMARY KEY,
    carrier           TEXT        NOT NULL DEFAULT 'unknown',
    cause             TEXT,
    severity          TEXT,
    order_count       INTEGER     NOT NULL DEFAULT 0,
    state             TEXT        NOT NULL DEFAULT 'open'
                                  CHECK (state IN ('open', 'resolved', 'refunded', 'escalated')),
    escalation_reason TEXT,
    plan_version      INTEGER     NOT NULL DEFAULT 0,
    steps_failed      INTEGER     NOT NULL DEFAULT 0,
    compensations     INTEGER     NOT NULL DEFAULT 0,
    opened_at         TIMESTAMPTZ NOT NULL,
    closed_at         TIMESTAMPTZ,
    last_event_at     TIMESTAMPTZ NOT NULL,
    last_event_type   TEXT        NOT NULL,
    last_event_id     UUID        NOT NULL,
    kafka_partition   INTEGER     NOT NULL,
    kafka_offset      BIGINT      NOT NULL
)""",
    "CREATE INDEX view_incident_metrics_last_event_idx ON views.view_incident_metrics (last_event_at DESC)",
    """
CREATE TABLE views.view_llm_calls (
    event_id        UUID        PRIMARY KEY,
    incident_id     TEXT        NOT NULL,
    agent           TEXT        NOT NULL,
    model           TEXT        NOT NULL,
    tokens_in       INTEGER     NOT NULL,
    tokens_out      INTEGER     NOT NULL,
    latency_ms      INTEGER     NOT NULL,
    validated       BOOLEAN     NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL
)""",
    """
CREATE TABLE views.view_step_failures (
    event_id        UUID        PRIMARY KEY,
    incident_id     TEXT        NOT NULL,
    step_id         TEXT        NOT NULL,
    action          TEXT,
    error_code      TEXT        NOT NULL,
    plan_version    INTEGER     NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL
)""",
    """
CREATE TABLE views.view_carrier_consults (
    event_id        UUID        PRIMARY KEY,
    incident_id     TEXT        NOT NULL,
    outcome         TEXT        NOT NULL,
    rounds          INTEGER     NOT NULL,
    latency_ms      INTEGER     NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL
)""",
    """
CREATE TABLE views.view_operations_steps (
    incident_id TEXT NOT NULL,
    step_id     TEXT NOT NULL,
    action      TEXT NOT NULL,
    PRIMARY KEY (incident_id, step_id)
)""",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for table in reversed(TABLES):
        op.execute(f"DROP TABLE views.{table}")
