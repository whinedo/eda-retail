"""Una reclamación por incidencia y pedido en `executor_file_claim.claims` (spec idempotency, «Una reclamación
por incidencia y pedido»; odd/tasks/scenario-coverage.md L3).

El ejecutor de `file_claim` devuelve la reclamación existente cuando una revisión del plan vuelve a pedirla con
otro `step_id`; este índice único lo garantiza también ante dos trabajos concurrentes. Si la tabla ya tiene
reclamaciones repetidas, la migración se detiene con un error y no borra ninguna: decidir cuál se conserva es
una decisión de negocio, no de la migración.

Revision ID: 0022
Revises: 0021
"""

from alembic import op

revision = "0022"
down_revision = "0021"
branch_labels = None
depends_on = None

INDEX = "claims_incident_order_key"


def upgrade() -> None:
    duplicates = op.get_bind().exec_driver_sql(
        "SELECT count(*) FROM (SELECT 1 FROM executor_file_claim.claims"
        " GROUP BY incident_id, order_id HAVING count(*) > 1) AS d"
    ).scalar()
    if duplicates:
        raise RuntimeError(
            f"executor_file_claim.claims tiene {duplicates} pares (incident_id, order_id) con más de una"
            " reclamación; resuélvelos antes de aplicar la migración 0022 (no se borra ninguna)"
        )
    op.execute(f"CREATE UNIQUE INDEX {INDEX} ON executor_file_claim.claims (incident_id, order_id)")


def downgrade() -> None:
    op.execute(f"DROP INDEX executor_file_claim.{INDEX}")
