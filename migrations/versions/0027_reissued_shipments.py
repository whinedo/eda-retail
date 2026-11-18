"""Tabla de hechos `view_reissued_shipments` del proyector `operations` (spec incident-metrics, «Hechos de apoyo»): una
fila por `ShipmentReissued`, con el `event_id` como clave, para que la consulta de beneficio cobre el transporte del
pedido reenviado. Solo identificadores y la posición en Kafka.

El proyector ya pasó los `ShipmentReissued` antiguos de un volumen existente: la migración vacía sus vistas, su
posición (`views.offsets`) y sus eventos aplicados (`views.applied_events`), como hace la reconstrucción, para que
vuelva a consumir `eda.events` desde el principio (retención infinita) y la tabla nueva tenga también el pasado.

Revision ID: 0027
Revises: 0026
"""

from alembic import op

revision = "0027"
down_revision = "0026"
branch_labels = None
depends_on = None


PROJECTOR = "operations"
VIEWS = ("view_incident_metrics", "view_llm_calls", "view_step_failures", "view_carrier_consults",
         "view_operations_steps")


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
    for table in VIEWS:
        op.execute(f"DELETE FROM views.{table}")
    op.execute(f"DELETE FROM views.offsets WHERE consumer = '{PROJECTOR}'")
    op.execute(f"DELETE FROM views.applied_events WHERE projector = '{PROJECTOR}'")


def downgrade() -> None:
    op.execute("DROP TABLE views.view_reissued_shipments")
