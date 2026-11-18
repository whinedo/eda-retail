"""Tabla de hechos `view_reissued_shipments` del proyector `operations` (spec incident-metrics, «Hechos de apoyo»): una
fila por `ShipmentReissued`, con el `event_id` como clave, para que la consulta de beneficio cobre el transporte del
pedido reenviado. Solo identificadores y la posición en Kafka.

Revision ID: 0027
Revises: 0026
"""

from alembic import op

revision = "0027"
down_revision = "0026"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
CREATE TABLE views.view_reissued_shipments (
    event_id        UUID        PRIMARY KEY,
    incident_id     TEXT        NOT NULL,
    order_id        TEXT        NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL
)"""
    )


def downgrade() -> None:
    op.execute("DROP TABLE views.view_reissued_shipments")
