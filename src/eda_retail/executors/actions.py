"""Las seis acciones permitidas. Efectos simulados en PostgreSQL y en servicios falsos.

| Acción | Efecto | Errores |
| --- | --- | --- |
| `notify_customer` | `customer_messages` + correo (texto del redactor) | `invalid_draft` (reintentable), `timeout` |
| `file_claim` | `claims` + aseguradora (texto del redactor) | `invalid_draft`, `timeout` |
| `reserve_stock` | `stock` − cantidad, `reservations` activa | `out_of_stock` (negocio) |
| `release_stock` | reserva `released`, `stock` + cantidad | `reservation_not_found` (negocio) |
| `reissue_shipment` | `shipments` + transportista | `carrier_rejected` (negocio), `timeout` |
| `issue_refund` | `refunds` + pagos | `refund_limit` (negocio, escala la incidencia), `timeout` |

Ninguna acción llama al LLM para decidir si ejecuta: el redactor solo escribe el texto,
fuera de la transacción, y el código lo valida antes de enviarlo (spec step-execution).
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

import psycopg

from eda_retail.agents.writer import INVALID_DRAFT, DraftRequest, Writer
from eda_retail.command.handler import CommandHandler
from eda_retail.command.rules import BusinessRules
from eda_retail.executors.base import BusinessError, Effect, EffectError, NoPreparation, Prepared, StepRequest
from eda_retail.executors.services import FakeService, Services
from eda_retail.llm.generation import BUDGET_EXHAUSTED, LlmClient
from eda_retail.step_executors.errors import REFUND_LIMIT, Escalating

OUT_OF_STOCK = "out_of_stock"
ORDER_NOT_FOUND = "order_not_found"
RESERVATION_NOT_FOUND = "reservation_not_found"


def _order(conn: psycopg.Connection, order_id: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM orders WHERE order_id = %s", (order_id,)).fetchone()
    if row is None:
        raise BusinessError(ORDER_NOT_FOUND)
    return row


class _Drafted:
    """Acción cuyo texto redacta el agente redactor antes de la transacción (supuesto A9)."""

    kind = "customer_notice"

    def __init__(self, writer: Writer, handler: CommandHandler, service: FakeService) -> None:
        self._writer = writer
        self._handler = handler
        self._service = service

    def prepare(self, conn: psycopg.Connection, request: StepRequest) -> Prepared:
        with conn.transaction():
            order = conn.execute("SELECT * FROM orders WHERE order_id = %s", (request.params["order_id"],)).fetchone()
            incident = conn.execute(
                "SELECT cause FROM incidents WHERE incident_id = %s", (request.incident_id,)
            ).fetchone()
            budget = self._handler.budget(conn, request.incident_id)
        if order is None:
            return Prepared(error=BusinessError(ORDER_NOT_FOUND))
        draft = self._writer.draft(
            DraftRequest(kind=self.kind, incident_id=request.incident_id, order_id=order["order_id"],  # type: ignore[arg-type]
                         sku=order["sku"], quantity=order["quantity"], cause=incident["cause"],
                         estimated_delay_hours=int(request.params.get("estimated_delay_hours", 0))),
            budget,
        )
        if draft.text is None:
            retryable = draft.error != BUDGET_EXHAUSTED
            return Prepared(calls=draft.calls, error=EffectError(draft.error or INVALID_DRAFT, retryable=retryable))
        return Prepared(draft.text, draft.calls)


class NotifyCustomer(_Drafted):
    action = "notify_customer"

    def execute(self, conn: psycopg.Connection, request: StepRequest, prepared: Prepared) -> Effect:
        message_id = self._service.call(request.idempotency_key)
        conn.execute(
            "INSERT INTO customer_messages (message_id, incident_id, order_id, step_id) VALUES (%s, %s, %s, %s)",
            (message_id, request.incident_id, request.params["order_id"], request.step_id),
        )
        return Effect({"message_id": message_id}, {"order_id": request.params["order_id"], "message_id": message_id})


class FileClaim(_Drafted):
    action = "file_claim"
    kind = "claim"

    def execute(self, conn: psycopg.Connection, request: StepRequest, prepared: Prepared) -> Effect:
        claim_id = self._service.call(request.idempotency_key)
        conn.execute(
            "INSERT INTO claims (claim_id, incident_id, order_id) VALUES (%s, %s, %s)",
            (claim_id, request.incident_id, request.params["order_id"]),
        )
        return Effect({"claim_id": claim_id}, {"order_id": request.params["order_id"], "claim_id": claim_id})


class ReserveStock(NoPreparation):
    action = "reserve_stock"

    def execute(self, conn: psycopg.Connection, request: StepRequest, prepared: Prepared) -> Effect:
        order = _order(conn, request.params["order_id"])
        row = conn.execute(
            """
            UPDATE stock SET available = available - %s
            WHERE (warehouse, sku) = (
                SELECT warehouse, sku FROM stock WHERE sku = %s AND available >= %s
                ORDER BY warehouse LIMIT 1 FOR UPDATE
            )
            RETURNING warehouse
            """,
            (order["quantity"], order["sku"], order["quantity"]),
        ).fetchone()
        if row is None:
            raise BusinessError(OUT_OF_STOCK)
        reservation_id = f"R-{uuid.uuid4().hex[:8]}"
        conn.execute(
            "INSERT INTO reservations (reservation_id, order_id, warehouse, sku, quantity, status) "
            "VALUES (%s, %s, %s, %s, %s, 'active')",
            (reservation_id, order["order_id"], row["warehouse"], order["sku"], order["quantity"]),
        )
        return Effect(
            {"reservation_id": reservation_id, "warehouse": row["warehouse"]},
            {"order_id": order["order_id"], "warehouse": row["warehouse"], "reservation_id": reservation_id},
        )


class ReleaseStock(NoPreparation):
    action = "release_stock"

    def execute(self, conn: psycopg.Connection, request: StepRequest, prepared: Prepared) -> Effect:
        reservation_id = request.params.get("reservation_id")
        row = conn.execute(
            "SELECT * FROM reservations WHERE reservation_id = %s FOR UPDATE", (reservation_id,)
        ).fetchone()
        if row is None:
            raise BusinessError(RESERVATION_NOT_FOUND)
        if row["status"] == "active":
            conn.execute("UPDATE reservations SET status = 'released' WHERE reservation_id = %s", (reservation_id,))
            conn.execute(
                "UPDATE stock SET available = available + %s WHERE warehouse = %s AND sku = %s",
                (row["quantity"], row["warehouse"], row["sku"]),
            )
        return Effect({"released": True, "reservation_id": reservation_id}, {"reservation_id": reservation_id})


class ReissueShipment(NoPreparation):
    action = "reissue_shipment"

    def __init__(self, service: FakeService) -> None:
        self._service = service

    def execute(self, conn: psycopg.Connection, request: StepRequest, prepared: Prepared) -> Effect:
        order = _order(conn, request.params["order_id"])
        shipment_id = self._service.call(request.idempotency_key)
        conn.execute(
            "INSERT INTO shipments (shipment_id, incident_id, order_id, step_id) VALUES (%s, %s, %s, %s)",
            (shipment_id, request.incident_id, order["order_id"], request.step_id),
        )
        return Effect({"new_shipment_id": shipment_id}, {"order_id": order["order_id"], "new_shipment_id": shipment_id})


class IssueRefund(NoPreparation):
    action = "issue_refund"

    def __init__(self, service: FakeService, rules: BusinessRules) -> None:
        self._service = service
        self._rules = rules

    def execute(self, conn: psycopg.Connection, request: StepRequest, prepared: Prepared) -> Effect:
        amount = Decimal(str(request.params["amount"]))
        if amount > self._rules.max_refund_amount:
            raise Escalating(REFUND_LIMIT)  # no se ejecuta: StepFailed y IncidentEscalated
        order = _order(conn, request.params["order_id"])
        refund_id = self._service.call(request.idempotency_key)
        conn.execute(
            "INSERT INTO refunds (refund_id, incident_id, order_id, step_id, amount) VALUES (%s, %s, %s, %s, %s)",
            (refund_id, request.incident_id, order["order_id"], request.step_id, amount),
        )
        return Effect({"refund_id": refund_id}, {"order_id": order["order_id"], "amount": str(amount), "refund_id": refund_id})


def build_actions(llm: LlmClient, handler: CommandHandler, services: Services | None = None) -> dict[str, Any]:
    """Una acción por nombre; cada una se ejecuta en su propio proceso (eda-executor <acción>)."""
    s = services or Services()
    writer = Writer(llm, max_generations=handler.rules.max_generations)
    actions = [
        NotifyCustomer(writer, handler, s.mail), FileClaim(writer, handler, s.claims), ReserveStock(),
        ReleaseStock(), ReissueShipment(s.carrier), IssueRefund(s.payments, handler.rules),
    ]
    return {a.action: a for a in actions}
