"""Contexto de traza (W3C traceparent/tracestate) en el outbox.

El relay lo copia a las cabeceras del mensaje de Kafka. Va en `outbox` y no en
`events` para no cambiar el log.

Revision ID: 0002
Revises: 0001
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS trace_context JSONB")


def downgrade() -> None:
    op.execute("ALTER TABLE outbox DROP COLUMN IF EXISTS trace_context")
