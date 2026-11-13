"""Texto común de los prompts de planificación y lectura de los datos de negocio que citan."""

from __future__ import annotations

import psycopg

ACTIONS = """Acciones permitidas:
- notify_customer: avisa al cliente del problema.
- file_claim: abre una reclamación al transportista.
- reserve_stock: reserva stock para un reenvío.
- reissue_shipment: reenvía el pedido (necesita una reserva de stock).
- issue_refund: reembolsa el pedido; es irreversible y va al final."""


def format_orders(conn: psycopg.Connection, order_ids: list[str]) -> str:
    """Pedidos con su importe (sistema de negocio simulado, no el estado de escritura).
    Solo referencias e importes: sin datos personales."""
    rows = conn.execute(
        "SELECT order_id, sku, quantity, amount FROM orders WHERE order_id = ANY(%s) ORDER BY order_id",
        (order_ids,),
    ).fetchall()
    return "\n".join(f"- {r['order_id']}: {r['quantity']} x {r['sku']}, importe {r['amount']}" for r in rows)
