"""Tablas de dominio del replanificador (T8, odd/tasks/eda-component-ownership.md).

- `replanner.revisions`: una por incidencia y versión del plan revisada, con el `RevisePlan` que la revisó
  y su resultado: `revised` (con la `plan_version` nueva y sus pasos) o `escalated` (sin plan válido o
  sin revisiones, con el motivo).
- `replanner.llm_calls`: cada llamada al LLM del replanificador (también se publica `LlmCallRecorded`).

Sin claves foráneas hacia otros esquemas. Es aditiva: el replanner anterior y el handler compartido se
quedan hasta T10 y T11.

Revision ID: 0014
Revises: 0013
"""

from alembic import op

revision = "0014"
down_revision = "0013"
branch_labels = None
depends_on = None

STATEMENTS = (
    """
CREATE TABLE replanner.revisions (
    incident_id       TEXT        NOT NULL,
    base_plan_version INTEGER     NOT NULL,
    command_id        UUID        NOT NULL,
    plan_version      INTEGER,
    failed_step_id    TEXT        NOT NULL,
    error             TEXT        NOT NULL,
    steps             JSONB,
    status            TEXT        NOT NULL CHECK (status IN ('revised', 'escalated')),
    reason            TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (incident_id, base_plan_version),
    CHECK ((status = 'revised') = (steps IS NOT NULL AND plan_version IS NOT NULL))
)""",
    """
CREATE TABLE replanner.llm_calls (
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
    "CREATE INDEX replanner_llm_calls_incident_idx ON replanner.llm_calls (incident_id)",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for table in ("replanner.llm_calls", "replanner.revisions"):
        op.execute(f"DROP TABLE {table}")
