"""Ejecutores (fase 4): intentos por paso, efectos por incidencia, envíos y dead letters.

- `plan_steps.attempts`: intentos del ejecutor cuando el paso termina (reintentos agotados).
- `refunds` y `customer_messages`: el `step_id` es único por incidencia, no global (el
  planificador repite S1, S2… entre incidencias). Ganan `incident_id`; no hay filas previas
  porque no había ejecutores, así que la columna se crea `NOT NULL` sin valor por defecto.
- `shipments`: efecto simulado de `reissue_shipment`.
- `dead_letters.published_at`: el relay publica las dead letters en `eda.dlq`.

Revision ID: 0004
Revises: 0003
"""

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE plan_steps ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
    for table in ("refunds", "customer_messages"):
        op.execute(f"ALTER TABLE {table} ADD COLUMN incident_id TEXT NOT NULL")
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT {table}_step_id_key")
        op.execute(f"ALTER TABLE {table} ADD CONSTRAINT {table}_incident_step_key UNIQUE (incident_id, step_id)")
    op.execute(
        """
        CREATE TABLE shipments (
            shipment_id TEXT PRIMARY KEY,
            incident_id TEXT        NOT NULL,
            order_id    TEXT        NOT NULL,
            step_id     TEXT        NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
            UNIQUE (incident_id, step_id)
        )
        """
    )
    op.execute("ALTER TABLE dead_letters ADD COLUMN published_at TIMESTAMPTZ")
    op.execute("CREATE INDEX dead_letters_pending_idx ON dead_letters (id) WHERE published_at IS NULL")


def downgrade() -> None:
    # Falla si dos incidencias comparten step_id en un efecto: es la restricción que se quita al subir.
    op.execute("DROP INDEX dead_letters_pending_idx")
    op.execute("ALTER TABLE dead_letters DROP COLUMN published_at")
    op.execute("DROP TABLE shipments")
    for table in ("refunds", "customer_messages"):
        op.execute(f"ALTER TABLE {table} DROP CONSTRAINT {table}_incident_step_key")
        op.execute(f"ALTER TABLE {table} ADD CONSTRAINT {table}_step_id_key UNIQUE (step_id)")
        op.execute(f"ALTER TABLE {table} DROP COLUMN incident_id")
    op.execute("ALTER TABLE plan_steps DROP COLUMN attempts")
