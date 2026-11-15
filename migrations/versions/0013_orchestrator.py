"""Tablas de la saga del orquestador (T7, odd/tasks/eda-component-ownership.md).

- `orchestrator.incidents`: estado de cada incidencia en la saga (`open`, `resolved`, `escalated`),
  la versión vigente del plan, las revisiones hechas y los datos que lleva `CreatePlan`.
- `orchestrator.plan_steps`: los pasos del plan y su estado (único dueño: el orquestador).
- `orchestrator.command_outbox`: los comandos que la saga invocará por REST, con su destino; el
  despachador los entrega con el mismo `command_id` hasta que el sistema responde.
- `orchestrator.deadlines`: el plazo durable de cada comando invocado.

Sin claves foráneas hacia otros esquemas. Es aditiva: las tablas compartidas se quedan hasta T10.

Revision ID: 0013
Revises: 0012
"""

from alembic import op

revision = "0013"
down_revision = "0012"
branch_labels = None
depends_on = None

STATEMENTS = (
    """
CREATE TABLE orchestrator.incidents (
    incident_id           TEXT PRIMARY KEY,
    status                TEXT        NOT NULL CHECK (status IN ('open', 'resolved', 'escalated')),
    plan_version          INTEGER     NOT NULL DEFAULT 0,
    revisions             INTEGER     NOT NULL DEFAULT 0,
    order_ids             TEXT[]      NOT NULL,
    cause                 TEXT        NOT NULL,
    estimated_delay_hours INTEGER     NOT NULL,
    severity              TEXT        NOT NULL,
    reason                TEXT,
    opened_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at             TIMESTAMPTZ
)""",
    """
CREATE TABLE orchestrator.plan_steps (
    incident_id         TEXT    NOT NULL REFERENCES orchestrator.incidents (incident_id),
    step_id             TEXT    NOT NULL,
    origin_plan_version INTEGER NOT NULL,
    action              TEXT    NOT NULL,
    params              JSONB   NOT NULL,
    depends_on          TEXT[]  NOT NULL DEFAULT '{}',
    compensates         TEXT,
    status              TEXT    NOT NULL CHECK (status IN ('pending', 'requested', 'completed', 'failed',
                                                           'cancelled', 'compensating', 'compensated')),
    attempts            INTEGER NOT NULL DEFAULT 0,
    result              JSONB,
    error               TEXT,
    PRIMARY KEY (incident_id, step_id)
)""",
    """
CREATE TABLE orchestrator.command_outbox (
    command_id      UUID PRIMARY KEY,
    incident_id     TEXT        NOT NULL,
    command_type    TEXT        NOT NULL,
    target          TEXT        NOT NULL,
    causation_id    UUID,
    payload         JSONB       NOT NULL,
    trace_context   JSONB,
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'delivered', 'rejected', 'expired')),
    attempts        INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    response        JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    "CREATE INDEX command_outbox_due_idx ON orchestrator.command_outbox (next_attempt_at) WHERE status = 'pending'",
    """
CREATE TABLE orchestrator.deadlines (
    command_id  UUID PRIMARY KEY REFERENCES orchestrator.command_outbox (command_id),
    incident_id TEXT        NOT NULL,
    kind        TEXT        NOT NULL CHECK (kind IN ('create_plan', 'revise_plan', 'execute_step')),
    step_id     TEXT,
    expires_at  TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ
)""",
    "CREATE INDEX deadlines_due_idx ON orchestrator.deadlines (expires_at) WHERE resolved_at IS NULL",
    "CREATE INDEX deadlines_incident_idx ON orchestrator.deadlines (incident_id) WHERE resolved_at IS NULL",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for table in ("orchestrator.deadlines", "orchestrator.command_outbox", "orchestrator.plan_steps",
                  "orchestrator.incidents"):
        op.execute(f"DROP TABLE {table}")
