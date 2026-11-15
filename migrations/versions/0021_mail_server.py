"""Esquema `mail_server` del servidor de correo MCP (segunda versión; spec mcp-mail, odd/tasks/v2-mcp-a2a.md T2).

- `mail_server.customers`: directorio sintético (`customer_ref` → dirección). Lo siembra `eda-mail-server`
  al arrancar.
- `mail_server.mailbox`: buzón simulado, un correo por `idempotency_key` (deduplica los envíos repetidos).

Solo lo lee y escribe el servidor de correo. Sin claves foráneas hacia otros esquemas.

Revision ID: 0021
Revises: 0020
"""

from alembic import op

revision = "0021"
down_revision = "0020"
branch_labels = None
depends_on = None

STATEMENTS = (
    "CREATE SCHEMA IF NOT EXISTS mail_server",
    """
CREATE TABLE mail_server.customers (
    customer_ref TEXT PRIMARY KEY,
    email        TEXT NOT NULL
)""",
    """
CREATE TABLE mail_server.mailbox (
    message_id      TEXT        PRIMARY KEY,
    idempotency_key TEXT        NOT NULL UNIQUE,
    customer_ref    TEXT        NOT NULL,
    to_address      TEXT        NOT NULL,
    subject         TEXT        NOT NULL,
    body            TEXT        NOT NULL,
    accepted_at     TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    op.execute("DROP TABLE mail_server.mailbox")
    op.execute("DROP TABLE mail_server.customers")
    op.execute("DROP SCHEMA mail_server")
