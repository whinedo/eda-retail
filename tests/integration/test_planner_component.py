"""`planner` contra PostgreSQL real (T6): su command handler REST, sus tablas y su outbox.

Los pedidos se dan de alta directamente en `views.view_orders`, la proyección que consulta. El LLM es el
cliente simulado.
"""

from __future__ import annotations

import uuid
from dataclasses import replace

from eda_retail.config import Settings
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.planner.handler import specs
from eda_retail.rules import BusinessRules

from .component_helpers import body, client, outbox

RULES = BusinessRules()


def _step(step_id: str, action: str, order: str, depends_on=(), amount=None) -> dict:
    return {"step_id": step_id, "action": action, "params": {"order_id": order, "amount": amount},
            "depends_on": list(depends_on)}


def _loss_plan(order: str) -> dict:
    return {"steps": [_step("S1", "notify_customer", order), _step("S2", "file_claim", order),
                      _step("S3", "reserve_stock", order), _step("S4", "reissue_shipment", order, ["S3"])]}


def _order(conn, amount: str = "40.00") -> str:
    order = f"P-{uuid.uuid4().hex[:8]}"
    with conn.transaction():
        conn.execute("INSERT INTO views.view_orders (order_id, customer_ref, amount) VALUES (%s, 'CUST-1', %s)",
                     (order, amount))
    return order


def _incident() -> str:
    return f"I-{uuid.uuid4().hex[:8]}"


def _api(database_url, llm):
    settings = replace(Settings(), database_url=database_url)
    return client(database_url, "planner", specs(settings, llm, RULES))


def _create(api, incident: str, order: str, *, cause="loss", severity="high", command_id=None):
    payload = {"order_ids": [order], "cause": cause, "estimated_delay_hours": 24, "severity": severity}
    return api.post("/commands/CreatePlan", json=body(incident, payload, command_id))


def _types(conn, incident) -> list[str]:
    return [e["event_type"] for e in outbox(conn, "planner", incident)]


def _plan(conn, incident) -> dict | None:
    with conn.transaction():
        return conn.execute("SELECT * FROM planner.plans WHERE incident_id = %s", (incident,)).fetchone()


def _llm_calls(conn, incident) -> int:
    with conn.transaction():
        return conn.execute("SELECT count(*) AS n FROM planner.llm_calls WHERE incident_id = %s",
                            (incident,)).fetchone()["n"]


def test_low_severity_delay_gets_a_single_notification(database_url, conn):
    """Retraso leve: PlanCreated con plan_version 1 y un único paso notify_customer"""
    order, incident = _order(conn), _incident()
    llm = FakeLlmClient([])

    response = _create(_api(database_url, llm), incident, order, cause="delay", severity="low")

    assert response.status_code == 202
    [created] = outbox(conn, "planner", incident)
    assert created["event_type"] == "PlanCreated"
    assert created["payload"]["plan_version"] == 1
    assert [s["action"] for s in created["payload"]["steps"]] == ["notify_customer"]
    assert llm.requests == []
    assert _plan(conn, incident)["status"] == "created"


def test_lost_package_gets_the_full_plan(database_url, conn):
    """Pérdida del paquete: notify_customer, file_claim, reserve_stock y reissue_shipment"""
    order, incident = _order(conn), _incident()

    response = _create(_api(database_url, FakeLlmClient([_loss_plan(order)])), incident, order)

    assert response.status_code == 202
    assert _types(conn, incident) == ["LlmCallRecorded", "PlanCreated"]
    created = outbox(conn, "planner", incident)[-1]
    assert [s["action"] for s in created["payload"]["steps"]] == ["notify_customer", "file_claim",
                                                                  "reserve_stock", "reissue_shipment"]
    assert _llm_calls(conn, incident) == 1


def test_plan_created_is_caused_by_the_command(database_url, conn):
    """Comando derivado de un evento: el PlanCreated lleva causation_id = command_id"""
    order, incident, command_id = _order(conn), _incident(), uuid.uuid4()

    _create(_api(database_url, FakeLlmClient([_loss_plan(order)])), incident, order, command_id=command_id)

    assert {e["causation_id"] for e in outbox(conn, "planner", incident)} == {command_id}


def test_a_forbidden_action_never_reaches_plan_created(database_url, conn):
    """Comando rechazado: un plan con delete_order no se escribe como PlanCreated"""
    order, incident = _order(conn), _incident()
    llm = FakeLlmClient([{"steps": [_step("S1", "delete_order", order)]}] * RULES.max_generations)

    response = _create(_api(database_url, llm), incident, order)

    assert response.status_code == 202
    assert response.json()["outcome"] == "escalated"
    assert _types(conn, incident) == ["LlmCallRecorded"] * RULES.max_generations + ["IncidentEscalated"]
    assert outbox(conn, "planner", incident)[-1]["payload"]["reason"] == "llm_output_invalid"
    assert _plan(conn, incident)["status"] == "escalated"


def test_refund_and_reissue_of_the_same_order_is_rejected(database_url, conn):
    """Reembolso y reenvío del mismo pedido: refund_and_reissue_conflict, sin PlanCreated"""
    order, incident = _order(conn), _incident()
    conflict = {"steps": [_step("S1", "reserve_stock", order), _step("S2", "reissue_shipment", order, ["S1"]),
                          _step("S3", "issue_refund", order, amount=10)]}
    llm = FakeLlmClient([conflict] * RULES.max_generations)

    _create(_api(database_url, llm), incident, order)

    assert "PlanCreated" not in _types(conn, incident)
    assert "refund_and_reissue_conflict" in llm.requests[1].prompt


def test_refund_above_the_configured_maximum_is_rejected(database_url, conn):
    """Reembolso por encima del máximo: se rechaza y se registra refund_limit"""
    order, incident = _order(conn, amount="10000.00"), _incident()
    too_much = {"steps": [_step("S1", "issue_refund", order, amount=float(RULES.max_refund_amount) + 1)]}
    llm = FakeLlmClient([too_much] * RULES.max_generations)

    _create(_api(database_url, llm), incident, order)

    assert "PlanCreated" not in _types(conn, incident)
    assert "refund_limit" in llm.requests[1].prompt


def test_refund_above_the_order_amount_is_rejected(database_url, conn):
    """Reembolso por encima del importe del pedido: refund_limit con el importe de view_orders"""
    order, incident = _order(conn, amount="40.00"), _incident()
    llm = FakeLlmClient([{"steps": [_step("S1", "issue_refund", order, amount=60)]},
                         {"steps": [_step("S1", "issue_refund", order, amount=40)]}])

    _create(_api(database_url, llm), incident, order)

    assert "refund_limit" in llm.requests[1].prompt
    created = outbox(conn, "planner", incident)[-1]
    assert created["event_type"] == "PlanCreated"
    assert created["payload"]["steps"][0]["params"]["amount"] == 40


def test_repeated_command_returns_the_stored_response(database_url, conn):
    """Comando repetido: devuelve la respuesta guardada y no publica otro PlanCreated"""
    order, incident, command_id = _order(conn), _incident(), uuid.uuid4()
    api = _api(database_url, FakeLlmClient([_loss_plan(order)]))

    first = _create(api, incident, order, command_id=command_id)
    again = _create(api, incident, order, command_id=command_id)

    assert again.json() == first.json()
    assert again.headers["Idempotent-Replayed"] == "true"
    assert _types(conn, incident).count("PlanCreated") == 1


def test_an_incident_already_planned_rejects_another_create_plan(database_url, conn):
    """Plan ya creado: otro CreatePlan se rechaza con plan_exists sin llamar al LLM"""
    order, incident = _order(conn), _incident()
    llm = FakeLlmClient([_loss_plan(order)])
    api = _api(database_url, llm)
    _create(api, incident, order)

    response = _create(api, incident, order)

    assert response.status_code == 422
    assert response.json()["reason"] == "plan_exists"
    assert len(llm.requests) == 1
    assert _types(conn, incident).count("PlanCreated") == 1
