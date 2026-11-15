"""`replanner` contra PostgreSQL real (T8): su command handler REST, sus tablas y su outbox.

El plan vigente llega en el comando, como lo envía la saga; los pedidos se dan de alta directamente en
`views.view_orders`, la proyección que consulta. El LLM es el cliente simulado. Cada test usa su propia
incidencia y su propio pedido (la base de datos es compartida).
"""

from __future__ import annotations

import uuid
from dataclasses import replace

from eda_retail.config import Settings
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.replanner.handler import specs
from eda_retail.rules import BusinessRules

from .component_helpers import body, client, jobs, outbox

RULES = BusinessRules()


def _step(step_id: str, action: str, order: str, depends_on=(), amount=None) -> dict:
    return {"step_id": step_id, "action": action, "params": {"order_id": order, "amount": amount},
            "depends_on": list(depends_on)}


def _current(step_id: str, action: str, order: str, status: str, depends_on=()) -> dict:
    return {**_step(step_id, action, order, depends_on), "status": status, "origin_plan_version": 1}


def _out_of_stock(order: str) -> list[dict]:
    return [_current("S1", "notify_customer", order, "completed"), _current("S2", "file_claim", order, "pending"),
            _current("S3", "reserve_stock", order, "failed"),
            _current("S4", "reissue_shipment", order, "pending", ["S3"])]


def _refund_plan(order: str, amount: float = 40) -> dict:
    return {"steps": [_step("S2", "file_claim", order), _step("S5", "issue_refund", order, amount=amount)]}


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
    return client(database_url, "replanner", specs(settings, llm, RULES))


def _revise(api, incident: str, order: str, *, revisions=0, command_id=None, steps=None):
    payload = {"plan_version": 1, "revisions": revisions, "failed_step_id": "S3", "error": "out_of_stock",
               "steps": steps or _out_of_stock(order)}
    return api.post("/commands/RevisePlan", json=body(incident, payload, command_id))


def _types(conn, incident) -> list[str]:
    return [e["event_type"] for e in outbox(conn, "replanner", incident)]


def _revision(conn, incident) -> dict | None:
    with conn.transaction():
        return conn.execute("SELECT * FROM replanner.revisions WHERE incident_id = %s", (incident,)).fetchone()


def _llm_calls(conn, incident) -> int:
    with conn.transaction():
        return conn.execute("SELECT count(*) AS n FROM replanner.llm_calls WHERE incident_id = %s",
                            (incident,)).fetchone()["n"]


def test_out_of_stock_publishes_plan_revised(database_url, conn):
    """Sin stock en el almacén: PlanRevised con plan_version 2, reembolso en lugar de reenvío y sin notify_customer"""
    order, incident = _order(conn), _incident()

    response = _revise(_api(database_url, FakeLlmClient([_refund_plan(order)])), incident, order)

    assert response.status_code == 202
    assert _types(conn, incident) == ["LlmCallRecorded", "PlanRevised"]
    revised = outbox(conn, "replanner", incident)[-1]["payload"]
    assert revised["plan_version"] == 2
    assert revised["reason"] == "out_of_stock"
    assert [s["action"] for s in revised["steps"]] == ["file_claim", "issue_refund"]
    row = _revision(conn, incident)
    assert (row["status"], row["base_plan_version"], row["plan_version"]) == ("revised", 1, 2)
    assert _llm_calls(conn, incident) == 1


def test_a_kept_step_keeps_its_step_id(database_url, conn):
    """Paso mantenido entre versiones: file_claim conserva el step_id S2 en la versión 2"""
    order, incident = _order(conn), _incident()

    _revise(_api(database_url, FakeLlmClient([_refund_plan(order)])), incident, order)

    steps = outbox(conn, "replanner", incident)[-1]["payload"]["steps"]
    assert steps[0]["step_id"] == "S2"


def test_plan_revised_is_caused_by_the_command(database_url, conn):
    """Comando derivado de un evento: el PlanRevised lleva causation_id = command_id"""
    order, incident, command_id = _order(conn), _incident(), uuid.uuid4()

    _revise(_api(database_url, FakeLlmClient([_refund_plan(order)])), incident, order, command_id=command_id)

    assert {e["causation_id"] for e in outbox(conn, "replanner", incident)} == {command_id}


def test_a_completed_step_never_reaches_plan_revised(database_url, conn):
    """Sin stock en el almacén: un plan que repite notify_customer completado se rechaza y se pide otro"""
    order, incident = _order(conn), _incident()
    llm = FakeLlmClient([{"steps": [_step("S1", "notify_customer", order)]}, _refund_plan(order)])

    _revise(_api(database_url, llm), incident, order)

    assert "completed_step_in_plan" in llm.requests[1].prompt
    revised = outbox(conn, "replanner", incident)[-1]
    assert revised["event_type"] == "PlanRevised"
    assert "S1" not in [s["step_id"] for s in revised["payload"]["steps"]]


def test_refund_above_the_order_amount_is_rejected(database_url, conn):
    """Dato de otro dominio: el importe del pedido sale de view_orders"""
    order, incident = _order(conn, amount="40.00"), _incident()
    llm = FakeLlmClient([_refund_plan(order, amount=60), _refund_plan(order, amount=40)])

    _revise(_api(database_url, llm), incident, order)

    assert "refund_limit" in llm.requests[1].prompt
    assert outbox(conn, "replanner", incident)[-1]["event_type"] == "PlanRevised"


def test_invalid_proposals_escalate(database_url, conn):
    """Escalado con una reserva hecha: el replanner publica IncidentEscalated con llm_output_invalid"""
    order, incident = _order(conn), _incident()
    llm = FakeLlmClient([{"steps": [_step("S9", "delete_order", order)]}] * RULES.max_generations)

    response = _revise(_api(database_url, llm), incident, order)

    assert response.status_code == 202
    assert jobs(conn, "replanner", incident)[-1]["outcome"]["outcome"] == "escalated"
    assert _types(conn, incident) == ["LlmCallRecorded"] * RULES.max_generations + ["IncidentEscalated"]
    assert outbox(conn, "replanner", incident)[-1]["payload"]["reason"] == "llm_output_invalid"
    assert _revision(conn, incident)["status"] == "escalated"


def test_revisions_exhausted_escalate_without_llm(database_url, conn):
    """Revisiones agotadas: IncidentEscalated con max_revisions y sin llamar al LLM"""
    order, incident = _order(conn), _incident()
    llm = FakeLlmClient([])

    _revise(_api(database_url, llm), incident, order, revisions=RULES.max_plan_revisions)

    assert _types(conn, incident) == ["IncidentEscalated"]
    assert outbox(conn, "replanner", incident)[-1]["payload"]["reason"] == "max_revisions"
    assert llm.requests == []


def test_repeated_command_returns_the_stored_response(database_url, conn):
    """Comando repetido: devuelve la respuesta guardada y no publica otro PlanRevised"""
    order, incident, command_id = _order(conn), _incident(), uuid.uuid4()
    api = _api(database_url, FakeLlmClient([_refund_plan(order)]))

    first = _revise(api, incident, order, command_id=command_id)
    again = _revise(api, incident, order, command_id=command_id)

    assert again.json() == first.json()
    assert again.headers["Idempotent-Replayed"] == "true"
    assert _types(conn, incident).count("PlanRevised") == 1


def test_a_version_already_revised_rejects_another_revise_plan(database_url, conn):
    """Revisión ya hecha: otro RevisePlan de la misma versión se rechaza con revision_exists sin LLM"""
    order, incident = _order(conn), _incident()
    llm = FakeLlmClient([_refund_plan(order)])
    api = _api(database_url, llm)
    _revise(api, incident, order)

    response = _revise(api, incident, order)

    assert response.status_code == 422
    assert response.json()["reason"] == "revision_exists"
    assert len(llm.requests) == 1
    assert _types(conn, incident).count("PlanRevised") == 1
