"""Tablas de dominio del intérprete (T5, odd/tasks/eda-component-ownership.md).

- `interpreter.interpretations`: una por aviso (`carrier_notice_id`), con su incidencia, la
  interpretación del LLM y su estado: `interpreted`, `parked` (esperando su pedido) o `escalated`.
- `interpreter.pending_notices`: avisos aparcados porque `view_orders` aún no tenía ninguno de sus
  pedidos, con su plazo (design.md §4.3).
- `interpreter.llm_calls`: cada llamada al LLM del intérprete (también se publica `LlmCallRecorded`).

Sin claves foráneas hacia otros esquemas. Es aditiva: `incidents` y el handler compartido se quedan
hasta T10.

Revision ID: 0011
Revises: 0010
"""

from alembic import op

revision = "0011"
down_revision = "0010"
branch_labels = None
depends_on = None

STATEMENTS = (
    """
CREATE TABLE interpreter.interpretations (
    carrier_notice_id     TEXT PRIMARY KEY,
    incident_id           TEXT        NOT NULL UNIQUE,
    command_id            UUID        NOT NULL,
    order_ids             TEXT[]      NOT NULL,
    cause                 TEXT,
    estimated_delay_hours INTEGER,
    severity              TEXT,
    status                TEXT        NOT NULL CHECK (status IN ('interpreted', 'parked', 'escalated')),
    reason                TEXT,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at           TIMESTAMPTZ
)""",
    """
CREATE TABLE interpreter.pending_notices (
    incident_id TEXT PRIMARY KEY REFERENCES interpreter.interpretations (incident_id),
    order_ids   TEXT[]      NOT NULL,
    deadline_at TIMESTAMPTZ NOT NULL
)""",
    "CREATE INDEX pending_notices_orders_idx ON interpreter.pending_notices USING GIN (order_ids)",
    "CREATE INDEX pending_notices_deadline_idx ON interpreter.pending_notices (deadline_at)",
    """
CREATE TABLE interpreter.llm_calls (
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
    "CREATE INDEX llm_calls_incident_idx ON interpreter.llm_calls (incident_id)",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for table in ("interpreter.llm_calls", "interpreter.pending_notices", "interpreter.interpretations"):
        op.execute(f"DROP TABLE {table}")
