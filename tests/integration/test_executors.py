"""Las seis acciones: efectos simulados, errores clasificados, límite de reembolso y borradores."""

from __future__ import annotations

import uuid
from decimal import Decimal

from eda_retail.command.handler import CommandHandler
from eda_retail.command.rules import BusinessRules
from eda_retail.executors.actions import build_actions
from eda_retail.executors.services import Services
from eda_retail.infra.consumer import Outcome
from eda_retail.llm.fake import FakeLlmClient

from .command_helpers import events_of, handler, open_incident
from .executor_helpers import executor
from .test_plans import create, plan_steps, revise, step
from .test_saga_commands import advance, apply_revision
from .test_steps import record


def text_for(order: str) -> str:
    return (f"Hola. Te escribimos por tu pedido {order}. El transportista nos ha avisado de que la entrega "
            "llegará con retraso. Sentimos las molestias y te mantendremos informado de la solución.")


def make_order(conn, *, stock: int | None = 5, amount: str = "50.00") -> str:
    """Pedido de un SKU propio, con `stock` unidades en un almacén (None: sin fila de stock)."""
    order, sku = f"P-{uuid.uuid4().hex[:8]}", f"SKU-{uuid.uuid4().hex[:6]}"
    with conn.transaction():
        conn.execute(
            "INSERT INTO orders (order_id, customer_ref, sku, quantity, amount, carrier) "
            "VALUES (%s, 'C-ref', %s, 1, %s, 'demo')",
            (order, sku, Decimal(amount)),
        )
        if stock is not None:
            conn.execute("INSERT INTO stock (warehouse, sku, available) VALUES ('W1', %s, %s)", (sku, stock))
    return order


def stock_of(conn, order: str) -> int:
    n = conn.execute(
        "SELECT s.available FROM stock s JOIN orders o USING (sku) WHERE o.order_id = %s", (order,)
    ).fetchone()["available"]
    conn.commit()
    return n


def run(conn, action: str, params: dict, *, llm_outputs: list | None = None, services: Services | None = None):
    """Incidencia con un único paso `action`; lo pide y lo ejecuta. Devuelve (incidencia, eventos nuevos)."""
    order = params["order_id"]
    incident, _ = open_incident(conn, orders=[order])
    assert create(conn, incident, [step("S1", action, **params)]).accepted
    [req] = [e for e in advance(conn, incident).events if e.event_type == "StepRequested"]
    before = len(events_of(conn, incident))
    actions = build_actions(FakeLlmClient(llm_outputs or []), handler(), services or Services())
    ex, _ = executor(actions[action], max_attempts=1)
    assert ex.process(conn, req) == Outcome.PROCESSED
    return incident, req, ex, events_of(conn, incident)[before:]


def types(events) -> list[str]:
    return [e["event_type"] for e in events]


def test_claim_filed(conn):
    """Reclamación creada"""
    order = make_order(conn)
    _incident, _, _, events = run(conn, "file_claim", {"order_id": order}, llm_outputs=[{"text": text_for(order)}])
    assert types(events) == ["StepCompleted", "ClaimFiled", "LlmCallRecorded"]
    claim = events[1]["payload"]
    assert claim["claim_id"].startswith("C-") and claim["order_id"] == order
    assert events[0]["payload"]["step_id"] == "S1"
    n = conn.execute("SELECT count(*) AS n FROM claims WHERE claim_id = %s", (claim["claim_id"],)).fetchone()["n"]
    conn.commit()
    assert n == 1


def test_customer_notified(conn):
    """El aviso al cliente se envía con el texto validado del redactor"""
    order = make_order(conn)
    incident, _, _, events = run(conn, "notify_customer", {"order_id": order}, llm_outputs=[{"text": text_for(order)}])
    assert types(events) == ["StepCompleted", "CustomerNotified", "LlmCallRecorded"]
    row = conn.execute("SELECT incident_id, step_id FROM customer_messages WHERE order_id = %s", (order,)).fetchone()
    conn.commit()
    assert (row["incident_id"], row["step_id"]) == (incident, "S1")


def test_draft_with_another_order(conn):
    """Borrador con datos de otro pedido"""
    order = make_order(conn)
    draft = text_for(order) + " También revisamos el pedido P-9f9f9f9f de otro cliente."
    services = Services()
    _incident, _, _, events = run(conn, "notify_customer", {"order_id": order},
                                 llm_outputs=[{"text": draft}] * 3, services=services)
    assert services.mail.calls == 0  # el ejecutor no lo envía
    [failed] = [e["payload"] for e in events if e["event_type"] == "StepFailed"]
    assert (failed["error"], failed["retryable"]) == ("invalid_draft", True)
    assert "LlmCallRecorded" in types(events)  # el coste del LLM se registra aunque falle
    n = conn.execute("SELECT count(*) AS n FROM customer_messages WHERE order_id = %s", (order,)).fetchone()["n"]
    conn.commit()
    assert n == 0


def test_stock_reserved(conn):
    """reserve_stock descuenta el stock y deja una reserva activa"""
    order = make_order(conn, stock=2)
    _, _, _, events = run(conn, "reserve_stock", {"order_id": order})
    assert types(events) == ["StepCompleted", "StockReserved"]
    reservation = events[1]["payload"]["reservation_id"]
    status = conn.execute("SELECT status FROM reservations WHERE reservation_id = %s", (reservation,)).fetchone()
    conn.commit()
    assert status["status"] == "active" and stock_of(conn, order) == 1


def test_business_error(conn):
    """Error de negocio"""
    order = make_order(conn, stock=0)
    _, _, _, events = run(conn, "reserve_stock", {"order_id": order})
    [failed] = [e["payload"] for e in events if e["event_type"] == "StepFailed"]
    assert (failed["error"], failed["retryable"]) == ("out_of_stock", False)
    assert "StockReserved" not in types(events)


def release_request(conn, order: str):
    """Saga real: reserva completada, reenvío rechazado y plan revisado; devuelve el StepRequested de S1-comp."""
    incident, _ = open_incident(conn, orders=[order])
    create(conn, incident, [step("S1", "reserve_stock", order_id=order),
                            step("S2", "reissue_shipment", ["S1"], order_id=order)])
    [reserve] = [e for e in advance(conn, incident).events if e.event_type == "StepRequested"]
    actions = build_actions(FakeLlmClient(), handler())
    executor(actions["reserve_stock"], max_attempts=1)[0].process(conn, reserve)
    advance(conn, incident)
    assert record(conn, incident, "S2", outcome="failed", error="carrier_rejected", retryable=False).accepted
    assert revise(conn, incident, [step("S3", "issue_refund", order_id=order, amount="30")],
                  reason="carrier_rejected").accepted
    assert apply_revision(conn, incident, 2, ["S3"]).accepted
    [comp] = [e for e in advance(conn, incident).events if e.event_type == "StepRequested"]
    assert comp.payload["step_id"] == "S1-comp"
    return incident, comp


def test_repeated_compensation(conn):
    """Compensación repetida"""
    order = make_order(conn, stock=1)
    incident, comp = release_request(conn, order)
    assert stock_of(conn, order) == 0
    ex, _ = executor(build_actions(FakeLlmClient(), handler())["release_stock"], max_attempts=1)
    assert ex.process(conn, comp) == Outcome.PROCESSED
    assert stock_of(conn, order) == 1
    assert plan_steps(conn, incident)["S1"]["status"] == "compensated"
    before = types(events_of(conn, incident)).count("StockReleased")
    again = comp.model_copy(update={"event_id": uuid.uuid4()})
    assert ex.process(conn, again) == Outcome.PROCESSED
    assert stock_of(conn, order) == 1  # el ejecutor no libera nada
    assert types(events_of(conn, incident)).count("StockReleased") == before == 1  # devuelve el resultado guardado


def test_refund_over_the_limit(conn):
    """Importe por encima del máximo"""
    order = make_order(conn)
    services = Services()
    low_limit = BusinessRules(max_refund_amount=Decimal("50.00"))  # el ejecutor aplica su propio máximo
    incident, _ = open_incident(conn, orders=[order])
    create(conn, incident, [step("S1", "issue_refund", order_id=order, amount="80")])
    [req] = [e for e in advance(conn, incident).events if e.event_type == "StepRequested"]
    refund = build_actions(FakeLlmClient(), CommandHandler(low_limit), services)["issue_refund"]
    ex, _ = executor(refund, max_attempts=1)
    assert ex.process(conn, req) == Outcome.PROCESSED
    events = events_of(conn, incident)
    assert "RefundIssued" not in types(events) and services.payments.calls == 0
    [escalated] = [e["payload"] for e in events if e["event_type"] == "IncidentEscalated"]
    assert escalated["reason"] == "refund_limit"
    [failed] = [e["payload"] for e in events if e["event_type"] == "StepFailed"]
    assert (failed["error"], failed["retryable"]) == ("refund_limit", False)


def test_refund_issued(conn):
    """issue_refund dentro del máximo registra el reembolso y publica RefundIssued"""
    order = make_order(conn)
    incident, _, _, events = run(conn, "issue_refund", {"order_id": order, "amount": "30"})
    assert types(events) == ["StepCompleted", "RefundIssued"]
    row = conn.execute("SELECT amount, step_id FROM refunds WHERE incident_id = %s", (incident,)).fetchone()
    conn.commit()
    assert (row["amount"], row["step_id"]) == (Decimal("30.00"), "S1")


def test_reissue_shipment(conn):
    """reissue_shipment crea el envío y publica ShipmentReissued; un rechazo del transportista es de negocio"""
    order = make_order(conn)
    _, _, _, events = run(conn, "reissue_shipment", {"order_id": order})
    assert types(events) == ["StepCompleted", "ShipmentReissued"]
    rejected = Services()
    rejected.carrier.reject = "carrier_rejected"
    _, _, _, events = run(conn, "reissue_shipment", {"order_id": make_order(conn)}, services=rejected)
    [failed] = [e["payload"] for e in events if e["event_type"] == "StepFailed"]
    assert (failed["error"], failed["retryable"]) == ("carrier_rejected", False)
