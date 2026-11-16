"""Un aviso y un reembolso por paso: `customer_messages (incident_id, step_id)` y `refunds (incident_id, step_id)`
(design.md §8, restricciones de unicidad que refuerzan la idempotencia incluso sin `effect_keys`; la de
`claims (incident_id, order_id)` es la 0022).

Los ejecutores de `notify_customer` e `issue_refund` devuelven el efecto ya registrado para el paso aunque la
`idempotency_key` sea otra; estos índices únicos lo garantizan también ante dos trabajos concurrentes. Si una
tabla ya tiene efectos repetidos para un paso, la migración se detiene con un error y no borra ninguno.

Revision ID: 0023
Revises: 0022
"""

from alembic import op

revision = "0023"
down_revision = "0022"
branch_labels = None
depends_on = None

# (esquema, tabla, índice)
INDEXES = (
    ("executor_notify_customer", "customer_messages", "customer_messages_incident_step_key"),
    ("executor_issue_refund", "refunds", "refunds_incident_step_key"),
)


def upgrade() -> None:
    bind = op.get_bind()
    for schema, table, _ in INDEXES:
        duplicates = bind.exec_driver_sql(
            f"SELECT count(*) FROM (SELECT 1 FROM {schema}.{table}"
            " GROUP BY incident_id, step_id HAVING count(*) > 1) AS d"
        ).scalar()
        if duplicates:
            raise RuntimeError(
                f"{schema}.{table} tiene {duplicates} pares (incident_id, step_id) con más de un efecto;"
                " resuélvelos antes de aplicar la migración 0023 (no se borra ninguno)"
            )
    for schema, table, index in INDEXES:
        op.execute(f"CREATE UNIQUE INDEX {index} ON {schema}.{table} (incident_id, step_id)")


def downgrade() -> None:
    for schema, _, index in INDEXES:
        op.execute(f"DROP INDEX {schema}.{index}")
