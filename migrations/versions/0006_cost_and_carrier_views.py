"""Vistas de coste y del panel por transportista (fase 5, odd/tasks/views.md).

- `view_cost_per_incident` guarda solo hechos del log (tokens, reclamaciones, reenvíos e
  importe reembolsado). El coste en dinero lo calcula la consulta con los precios de la
  configuración, así una vista reconstruida es idéntica aunque cambien los precios (H4).
  Se quitan `llm_cost` y `effects_cost`: las vistas se reconstruyen desde el log.
- `last_seq` por fila: un evento con `seq <= last_seq` ya está aplicado (contadores
  idempotentes).
- `view_carrier_incidents`: a qué transportista pertenece cada incidencia; el transportista
  solo llega en `CarrierNoticeReceived`.

Revision ID: 0006
Revises: 0005
"""

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE view_cost_per_incident
            DROP COLUMN llm_cost,
            DROP COLUMN effects_cost,
            ADD COLUMN claims         INTEGER        NOT NULL DEFAULT 0,
            ADD COLUMN reissues       INTEGER        NOT NULL DEFAULT 0,
            ADD COLUMN refunds_amount NUMERIC(12, 2) NOT NULL DEFAULT 0,
            ADD COLUMN last_seq       BIGINT         NOT NULL DEFAULT 0
        """
    )
    op.execute(
        """
        ALTER TABLE view_carrier_dashboard
            ADD COLUMN incidents INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN last_seq  BIGINT  NOT NULL DEFAULT 0
        """
    )
    op.execute(
        """
        CREATE TABLE view_carrier_incidents (
            incident_id TEXT PRIMARY KEY,
            carrier     TEXT NOT NULL
        )
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE view_carrier_incidents")
    op.execute("ALTER TABLE view_carrier_dashboard DROP COLUMN incidents, DROP COLUMN last_seq")
    op.execute(
        """
        ALTER TABLE view_cost_per_incident
            DROP COLUMN claims,
            DROP COLUMN reissues,
            DROP COLUMN refunds_amount,
            DROP COLUMN last_seq,
            ADD COLUMN llm_cost     NUMERIC(12, 6) NOT NULL DEFAULT 0,
            ADD COLUMN effects_cost NUMERIC(12, 2) NOT NULL DEFAULT 0
        """
    )
