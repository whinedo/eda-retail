"""Esquema de la base síncrona de referencia (T11, odd/tasks/eda-component-ownership.md).

La base síncrona (E6) atiende cada aviso de principio a fin con llamadas directas: su estado de negocio
vive en sus propias tablas (`baseline_sync.*`), no en las públicas que T10b borra. No tiene log, outbox
ni inbox. Sin claves foráneas hacia otros esquemas.

Revision ID: 0016
Revises: 0015
"""

from alembic import op

revision = "0016"
down_revision = "0015"
branch_labels = None
depends_on = None

STATEMENTS = (
    "CREATE SCHEMA IF NOT EXISTS baseline_sync",
    """
CREATE TABLE baseline_sync.incidents (
    incident_id  TEXT PRIMARY KEY,
    status       TEXT        NOT NULL CHECK (status IN ('open', 'resolved', 'escalated')),
    severity     TEXT,
    cause        TEXT,
    plan_version INTEGER     NOT NULL DEFAULT 0,
    reason       TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at    TIMESTAMPTZ
)""",
    """
CREATE TABLE baseline_sync.orders (
    order_id     TEXT PRIMARY KEY,
    customer_ref TEXT          NOT NULL,
    sku          TEXT          NOT NULL,
    quantity     INTEGER       NOT NULL CHECK (quantity > 0),
    amount       NUMERIC(10,2) NOT NULL,
    carrier      TEXT          NOT NULL
)""",
    """
CREATE TABLE baseline_sync.stock (
    warehouse TEXT    NOT NULL,
    sku       TEXT    NOT NULL,
    available INTEGER NOT NULL CHECK (available >= 0),
    PRIMARY KEY (warehouse, sku)
)""",
    """
CREATE TABLE baseline_sync.reservations (
    reservation_id TEXT PRIMARY KEY,
    incident_id    TEXT    NOT NULL,
    step_id        TEXT    NOT NULL,
    order_id       TEXT    NOT NULL,
    warehouse      TEXT    NOT NULL,
    sku            TEXT    NOT NULL,
    quantity       INTEGER NOT NULL,
    status         TEXT    NOT NULL CHECK (status IN ('active', 'released'))
)""",
    *(f"""
CREATE TABLE baseline_sync.{table} (
    {key}       TEXT PRIMARY KEY,
    incident_id TEXT        NOT NULL,
    order_id    TEXT        NOT NULL,
    step_id     TEXT        NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
)""" for table, key in (("customer_messages", "message_id"), ("claims", "claim_id"), ("shipments", "shipment_id"))),
    """
CREATE TABLE baseline_sync.refunds (
    refund_id   TEXT PRIMARY KEY,
    incident_id TEXT          NOT NULL,
    order_id    TEXT          NOT NULL,
    step_id     TEXT          NOT NULL,
    amount      NUMERIC(10,2) NOT NULL CHECK (amount > 0),
    created_at  TIMESTAMPTZ   NOT NULL DEFAULT now()
)""",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    op.execute("DROP SCHEMA baseline_sync CASCADE")
