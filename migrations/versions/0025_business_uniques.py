"""Un reembolso por incidencia y pedido, y un aviso por incidencia, pedido y versión del plan (spec idempotency;
odd/tasks/e23-business-duplicates.md S1-S2).

- `executor_issue_refund.refunds (incident_id, order_id)`: una revisión del plan puede volver a pedir
  `issue_refund` con otro `step_id`; el ejecutor devuelve el reembolso existente y este índice lo garantiza también
  ante dos trabajos concurrentes. El de la 0023 por (incident_id, step_id) se mantiene.
- `executor_notify_customer.customer_messages.plan_version`: la versión del plan con la que la saga pidió el paso.
  Es nula en los avisos anteriores a esta migración, que no se rellenan (no se sabe su versión), así que el índice
  `(incident_id, order_id, plan_version)` no los compara entre sí.

Si `refunds` ya tiene reembolsos repetidos para un pedido de una incidencia, la migración se detiene con un error y
no borra ninguno: decidir cuál se conserva es una decisión de negocio, no de la migración.

Revision ID: 0025
Revises: 0024
"""

from alembic import op

revision = "0025"
down_revision = "0024"
branch_labels = None
depends_on = None

REFUNDS_INDEX = "refunds_incident_order_key"
MESSAGES_INDEX = "customer_messages_incident_order_version_key"


def upgrade() -> None:
    duplicates = op.get_bind().exec_driver_sql(
        "SELECT count(*) FROM (SELECT 1 FROM executor_issue_refund.refunds"
        " GROUP BY incident_id, order_id HAVING count(*) > 1) AS d"
    ).scalar()
    if duplicates:
        raise RuntimeError(
            f"executor_issue_refund.refunds tiene {duplicates} pares (incident_id, order_id) con más de un"
            " reembolso; resuélvelos antes de aplicar la migración 0025 (no se borra ninguno)"
        )
    op.execute(f"CREATE UNIQUE INDEX {REFUNDS_INDEX} ON executor_issue_refund.refunds (incident_id, order_id)")
    op.execute("ALTER TABLE executor_notify_customer.customer_messages ADD COLUMN plan_version INTEGER")
    op.execute(f"CREATE UNIQUE INDEX {MESSAGES_INDEX}"
               " ON executor_notify_customer.customer_messages (incident_id, order_id, plan_version)")


def downgrade() -> None:
    op.execute(f"DROP INDEX executor_notify_customer.{MESSAGES_INDEX}")
    op.execute("ALTER TABLE executor_notify_customer.customer_messages DROP COLUMN plan_version")
    op.execute(f"DROP INDEX executor_issue_refund.{REFUNDS_INDEX}")
