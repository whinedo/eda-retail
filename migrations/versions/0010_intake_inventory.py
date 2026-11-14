"""Tablas de dominio de `intake` e `inventory` (T4, odd/tasks/eda-component-ownership.md).

- `intake.carrier_notices`: los avisos registrados (un aviso, una incidencia como mucho).
- `inventory.orders`, `inventory.stock`, `inventory.reservations`: pedidos, existencias y reservas
  de `inventory`. Cada reserva guarda la incidencia y el paso que la hizo, para que su compensación
  (`<paso>-comp`) la encuentre sin conocer el `reservation_id`.

Sin claves foráneas hacia otros esquemas. Es aditiva: las tablas públicas `carrier_notices`,
`orders`, `stock` y `reservations` se quedan hasta T10.

Revision ID: 0010
Revises: 0009
"""

from alembic import op

revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None

STATEMENTS = (
    """
CREATE TABLE intake.carrier_notices (
    carrier_notice_id TEXT PRIMARY KEY,
    incident_id       TEXT        NOT NULL UNIQUE,
    carrier           TEXT        NOT NULL,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    """
CREATE TABLE inventory.orders (
    order_id     TEXT PRIMARY KEY,
    customer_ref TEXT           NOT NULL,
    sku          TEXT           NOT NULL,
    quantity     INTEGER        NOT NULL CHECK (quantity > 0),
    amount       NUMERIC(10, 2) NOT NULL CHECK (amount > 0),
    carrier      TEXT           NOT NULL,
    status       TEXT           NOT NULL DEFAULT 'shipped'
)""",
    """
CREATE TABLE inventory.stock (
    warehouse TEXT    NOT NULL,
    sku       TEXT    NOT NULL,
    available INTEGER NOT NULL CHECK (available >= 0),
    PRIMARY KEY (warehouse, sku)
)""",
    """
CREATE TABLE inventory.reservations (
    reservation_id TEXT PRIMARY KEY,
    order_id       TEXT        NOT NULL REFERENCES inventory.orders (order_id),
    warehouse      TEXT        NOT NULL,
    sku            TEXT        NOT NULL,
    quantity       INTEGER     NOT NULL,
    status         TEXT        NOT NULL CHECK (status IN ('active', 'released')),
    incident_id    TEXT        NOT NULL,
    step_id        TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
)""",
    "CREATE INDEX reservations_step_idx ON inventory.reservations (incident_id, step_id)",
)


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for table in ("inventory.reservations", "inventory.stock", "inventory.orders", "intake.carrier_notices"):
        op.execute(f"DROP TABLE {table}")
