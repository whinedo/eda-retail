"""Borra el modelo compartido del esquema público (T10b, odd/tasks/eda-component-ownership.md).

Desde T10a ningún componente lee ni escribe estas tablas: cada uno tiene su esquema (0008–0016), el log
de eventos es `event_log.events` y las vistas están en `views`. Se borran el log compartido (`events`,
con sus triggers de solo añadir y su función), su `outbox`, el `inbox`, `effect_keys` y `dead_letters`
compartidos, el estado de escritura compartido (`incidents`, `carrier_notices`, `plan_steps`) y las
tablas de negocio simuladas (`orders`, `stock`, `reservations` y las de efectos), además de las vistas y
`projector_offsets` públicas.

`db/init/001_schema.sql` y 0001 se quedan como base histórica: una base de datos nueva los crea y esta
migración los borra. No hay vuelta atrás (como 0001): los datos del modelo compartido no se reconstruyen.

Revision ID: 0017
Revises: 0016
"""

from alembic import op

revision = "0017"
down_revision = "0016"
branch_labels = None
depends_on = None

# Orden: primero las tablas con claves foráneas (outbox -> events, plan_steps -> incidents).
TABLES = (
    "outbox", "events", "plan_steps", "incidents", "carrier_notices", "inbox", "effect_keys", "dead_letters",
    "orders", "stock", "reservations", "claims", "refunds", "customer_messages", "shipments",
    "projector_offsets", "view_incident_status", "view_cost_per_incident", "view_audit_trail",
    "view_carrier_dashboard", "view_carrier_incidents",
)


def upgrade() -> None:
    for table in TABLES:
        op.execute(f"DROP TABLE IF EXISTS public.{table}")
    op.execute("DROP FUNCTION IF EXISTS public.events_reject_mutation()")


def downgrade() -> None:
    raise NotImplementedError("0017 borra el modelo compartido: no se reconstruye")
