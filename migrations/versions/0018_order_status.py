"""Vista `order_status` (T12, odd/tasks/eda-component-ownership.md; spec cqrs-views, «Estado de los pedidos»).

Una fila por pedido e incidencia con su estado de negocio y el último evento que lo cambió (tipo,
`event_id`, timestamp y posición en Kafka). La mantiene el proyector `order_status`, que consume Kafka.

Revision ID: 0018
Revises: 0017
"""

from alembic import op

revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None

STATEMENTS = (
    """
CREATE TABLE views.view_order_status (
    order_id        TEXT        NOT NULL,
    incident_id     TEXT        NOT NULL,
    status          TEXT        NOT NULL
        CHECK (status IN ('delayed', 'in_resolution', 'resolved', 'refunded', 'escalated')),
    last_event_type TEXT        NOT NULL,
    last_event_id   UUID        NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    PRIMARY KEY (order_id, incident_id)
)""",
    "CREATE INDEX view_order_status_incident_idx ON views.view_order_status (incident_id)",
    "CREATE INDEX view_order_status_recent_idx ON views.view_order_status (occurred_at DESC)",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    op.execute("DROP TABLE views.view_order_status")
