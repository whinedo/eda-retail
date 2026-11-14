"""Esquema base: db/init/001_schema.sql.

En un volumen creado por docker compose el esquema ya existe y esta revisión no hace
nada; en una base vacía ejecuta el script.

Revision ID: 0001
Revises:
"""

from pathlib import Path

from alembic import op

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None

SCHEMA = Path(__file__).resolve().parents[2] / "db" / "init" / "001_schema.sql"


def upgrade() -> None:
    bind = op.get_bind()
    exists = bind.exec_driver_sql("SELECT to_regclass('public.events') IS NOT NULL").scalar()
    if exists:
        return
    # Conexión psycopg directa: el script tiene varias sentencias y '%' en PL/pgSQL.
    bind.connection.dbapi_connection.execute(SCHEMA.read_text(encoding="utf-8"))


def downgrade() -> None:
    raise NotImplementedError("el esquema base no se revierte (events es solo de añadir)")
