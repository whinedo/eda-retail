"""Utilidades de test para el command handler."""

from __future__ import annotations

import uuid
from decimal import Decimal

import psycopg

from eda_retail.command.handler import CommandHandler, CommandResult, incident_id_for_notice
from eda_retail.command.models import OpenIncident, RegisterCarrierNotice
from eda_retail.command.rules import BusinessRules

RULES = BusinessRules(max_refund_amount=Decimal("100.00"))


def handler() -> CommandHandler:
    return CommandHandler(RULES)


def add_order(conn: psycopg.Connection, amount: str = "50.00") -> str:
    order = f"P-{uuid.uuid4().hex[:8]}"
    with conn.transaction():
        conn.execute(
            "INSERT INTO orders (order_id, customer_ref, sku, quantity, amount, carrier) "
            "VALUES (%s, 'C-ref', 'SKU1', 1, %s, 'demo')",
            (order, Decimal(amount)),
        )
    return order


def register_notice(conn: psycopg.Connection) -> tuple[str, CommandResult]:
    notice = f"N-{uuid.uuid4().hex[:8]}"
    result = handler().handle(
        conn,
        RegisterCarrierNotice(
            command_id=uuid.uuid4(), expected_version=0, producer="intake",
            carrier_notice_id=notice, carrier="demo", raw_text="el pedido llega con un día de retraso",
        ),
    )
    conn.commit()
    return notice, result


def open_incident(
    conn: psycopg.Connection, *, cause: str = "loss", severity: str = "high", orders: list[str] | None = None
) -> tuple[str, CommandResult]:
    """Registra un aviso y abre su incidencia; devuelve (incident_id, resultado de OpenIncident)."""
    notice, registered = register_notice(conn)
    incident = incident_id_for_notice(notice)
    result = handler().handle(
        conn,
        OpenIncident(
            command_id=registered.events[0].event_id, expected_version=0, producer="interpreter",
            incident_id=incident, carrier_notice_id=notice,
            order_ids=orders if orders is not None else [add_order(conn)],
            cause=cause, estimated_delay_hours=24, severity=severity,
        ),
    )
    conn.commit()
    return incident, result


def events_of(conn: psycopg.Connection, incident: str) -> list[dict]:
    rows = conn.execute(
        "SELECT event_type, payload, causation_id FROM events WHERE correlation_id = %s ORDER BY seq", (incident,)
    ).fetchall()
    conn.commit()
    return rows


def version_of(conn: psycopg.Connection, incident: str) -> int:
    v = conn.execute("SELECT version FROM incidents WHERE incident_id = %s", (incident,)).fetchone()["version"]
    conn.commit()
    return v
