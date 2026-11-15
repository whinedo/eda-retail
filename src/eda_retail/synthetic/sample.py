"""Pedidos, stock y avisos de ejemplo para probar el flujo completo a mano.

Los pedidos solo llevan referencias (`customer_ref`), nunca datos personales.
`P-1004` usa un SKU sin existencias: su reserva termina en `out_of_stock` y obliga a
revisar el plan.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import psycopg

from eda_retail.command.handler import CommandHandler, CommandResult, incident_id_for_notice
from eda_retail.command.models import RegisterCarrierNotice
from eda_retail.synthetic.sample_data import CARRIER, EXAMPLES, ORDERS, STOCK  # noqa: F401


@dataclass(frozen=True)
class Sent:
    carrier_notice_id: str
    incident_id: str
    result: CommandResult


def seed(conn: psycopg.Connection) -> tuple[int, int]:
    """Carga pedidos y stock de ejemplo; repetirlo no duplica nada. Devuelve (pedidos, stock) nuevos."""
    with conn.transaction():
        orders = sum(
            conn.execute(
                "INSERT INTO orders (order_id, customer_ref, sku, quantity, amount, carrier) "
                "VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (*row, CARRIER),
            ).rowcount
            for row in ORDERS
        )
        stock = sum(
            conn.execute(
                "INSERT INTO stock (warehouse, sku, available) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", row
            ).rowcount
            for row in STOCK
        )
    return orders, stock


def send_notice(
    conn: psycopg.Connection, text: str, *, carrier: str = CARRIER, handler: CommandHandler | None = None,
    carrier_notice_id: str | None = None,
) -> Sent:
    """Registra un aviso del transportista: la entrada del sistema (`CarrierNoticeReceived`)."""
    notice = carrier_notice_id or f"N-{uuid.uuid4().hex[:8]}"
    with conn.transaction():
        result = (handler or CommandHandler()).handle(
            conn,
            RegisterCarrierNotice(
                command_id=uuid.uuid4(), expected_version=0, producer="intake",
                carrier_notice_id=notice, carrier=carrier, raw_text=text,
            ),
        )
    return Sent(notice, incident_id_for_notice(notice), result)
