"""Esquema `support` de la herramienta de soporte técnico `eda-support` (spec support, «Registro de las ejecuciones»):
`support.runs`, una fila por ejecución confirmada (no las simulaciones), con su acción, argumentos, token, origen,
inicio, fin, resultado y resumen. Sin datos personales.

Revision ID: 0028
Revises: 0027
"""

from alembic import op

revision = "0028"
down_revision = "0027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS support")
    op.execute(
        """
CREATE TABLE support.runs (
    id          BIGSERIAL   PRIMARY KEY,
    action      TEXT        NOT NULL CHECK (action IN ('republish', 'replay-from', 'rebuild')),
    args        JSONB       NOT NULL,
    token       TEXT        NOT NULL,
    origin      TEXT        NOT NULL CHECK (origin IN ('cli', 'web')),
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    outcome     TEXT        CHECK (outcome IN ('succeeded', 'failed', 'refused')),
    summary     JSONB
)"""
    )


def downgrade() -> None:
    op.execute("DROP SCHEMA support CASCADE")
