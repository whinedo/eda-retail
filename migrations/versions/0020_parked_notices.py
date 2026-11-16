"""Vista `parked_notices` (odd/tasks/parked-notices.md T2; spec cqrs-views, «Avisos aparcados»).

Una fila por aviso que el intérprete aparcó esperando sus pedidos (`NoticeParked`), con los pedidos que espera,
su plazo y la posición en Kafka del evento. La borra el `ShipmentDelayed` (reanudado) o el `IncidentEscalated`
(vencido) de su incidencia. La mantiene el proyector `parked_notices`, que consume Kafka.

Revision ID: 0020
Revises: 0019
"""

from alembic import op

revision = "0020"
down_revision = "0019"
branch_labels = None
depends_on = None

STATEMENTS = (
    """
CREATE TABLE views.view_parked_notices (
    incident_id     TEXT        PRIMARY KEY,
    order_ids       TEXT[]      NOT NULL,
    deadline_at     TIMESTAMPTZ NOT NULL,
    parked_at       TIMESTAMPTZ NOT NULL,
    last_event_id   UUID        NOT NULL,
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL
)""",
    "CREATE INDEX view_parked_notices_deadline_idx ON views.view_parked_notices (deadline_at)",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    op.execute("DROP TABLE views.view_parked_notices")
