"""Partición y offset de Kafka de cada evento publicado, en el outbox.

El relay los guarda al confirmar el broker la entrega (spec event-backbone, "Kafka como bus
de entrega"); el panel de eventos del front los muestra. Van en `outbox` y no en `events`
porque el log es inmutable y la posición cambia si el evento se republica.

Revision ID: 0007
Revises: 0006
"""

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE outbox ADD COLUMN IF NOT EXISTS kafka_partition INTEGER,"
        " ADD COLUMN IF NOT EXISTS kafka_offset BIGINT"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE outbox DROP COLUMN IF EXISTS kafka_partition, DROP COLUMN IF EXISTS kafka_offset")
