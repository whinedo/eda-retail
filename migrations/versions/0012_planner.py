"""Tablas de dominio del planificador (T6, odd/tasks/eda-component-ownership.md).

- `planner.plans`: una por incidencia, con el `CreatePlan` que la planificó y su resultado: `created`
  (con `plan_version` 1 y sus pasos) o `escalated` (sin plan válido, con el motivo).
- `planner.llm_calls`: cada llamada al LLM del planificador (también se publica `LlmCallRecorded`).

Sin claves foráneas hacia otros esquemas. Es aditiva: `plan_steps`, `incidents` y el handler
compartido se quedan hasta T10.

Revision ID: 0012
Revises: 0011
"""

from alembic import op

revision = "0012"
down_revision = "0011"
branch_labels = None
depends_on = None

STATEMENTS = (
    """
CREATE TABLE planner.plans (
    incident_id  TEXT PRIMARY KEY,
    command_id   UUID        NOT NULL,
    plan_version INTEGER,
    steps        JSONB,
    status       TEXT        NOT NULL CHECK (status IN ('created', 'escalated')),
    reason       TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((status = 'created') = (steps IS NOT NULL AND plan_version IS NOT NULL))
)""",
    """
CREATE TABLE planner.llm_calls (
    id                BIGSERIAL PRIMARY KEY,
    incident_id       TEXT        NOT NULL,
    command_id        UUID        NOT NULL,
    model             TEXT        NOT NULL,
    tokens_in         INTEGER     NOT NULL,
    tokens_out        INTEGER     NOT NULL,
    latency_ms        INTEGER     NOT NULL,
    validation_passed BOOLEAN     NOT NULL,
    attempt           INTEGER     NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    "CREATE INDEX planner_llm_calls_incident_idx ON planner.llm_calls (incident_id)",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for table in ("planner.llm_calls", "planner.plans"):
        op.execute(f"DROP TABLE {table}")
