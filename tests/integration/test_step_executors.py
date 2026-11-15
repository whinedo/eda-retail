"""Ejecutores REST contra PostgreSQL real (T9): `ExecuteStep`, sus tablas de efectos, `effect_keys` y outbox.

Los pedidos se dan de alta directamente en `views.view_orders`, la proyección que consultan. El redactor
usa el cliente LLM simulado y los servicios son los simulados, que se pasan para programar fallos. Cada
test usa su propia incidencia, pedido y clave (la base de datos es compartida).
"""

from __future__ import annotations

import uuid
from dataclasses import replace

from eda_retail.config import Settings
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.rules import BusinessRules
from eda_retail.step_executors.component import specs
from eda_retail.step_executors.services import FakeService

from .component_helpers import body, client, outbox

RULES = BusinessRules()
NOTICE = ("Hemos sentido mucho el retraso de tu pedido. El transportista nos ha avisado y "
          "te escribiremos en cuanto tengamos noticias de la entrega.")


def _order(conn, amount: str = "40.00") -> str:
    order = f"P-{uuid.uuid4().hex[:8]}"
    with conn.transaction():
        conn.execute("INSERT INTO views.view_orders (order_id, customer_ref, amount) VALUES (%s, 'CUST-1', %s)",
                     (order, amount))
    return order


def _api(database_url, action, *, llm=None, service=None, attempts=3):
    settings = replace(Settings(), database_url=database_url, step_max_attempts=attempts,
                       step_backoff_base_seconds=0.0, step_backoff_max_seconds=0.0)
    return client(database_url, f"executor_{action}",
                  specs(settings, action, llm or FakeLlmClient([]), RULES, service=service))


def _step(action, order, incident, *, key=None, amount=None, context=True, step_id="S1") -> dict:
    payload = {"incident_id": incident, "plan_version": 1, "step_id": step_id, "action": action,
               "params": {"order_id": order, "amount": amount}, "idempotency_key": key or uuid.uuid4().hex}
    if context and action in ("notify_customer", "file_claim"):
        payload |= {"cause": "delay", "estimated_delay_hours": 48}
    return payload


def _execute(api, payload, *, correlation=None):
    return api.post("/commands/ExecuteStep", json=body(correlation or payload["incident_id"], payload))


def _incident() -> str:
    return f"I-{uuid.uuid4().hex[:8]}"


def _events(conn, action, incident) -> list[dict]:
    return outbox(conn, f"executor_{action}", incident)


def _count(conn, table, incident) -> int:
    with conn.transaction():
        return conn.execute(f"SELECT count(*) AS n FROM {table} WHERE incident_id = %s", (incident,)).fetchone()["n"]


def test_notify_customer_sends_the_drafted_notice(database_url, conn):
    """Contenido redactado por el LLM: aviso enviado, CustomerNotified, StepCompleted y la llamada registrada"""
    order, incident = _order(conn), _incident()
    llm = FakeLlmClient([{"text": NOTICE}])

    response = _execute(_api(database_url, "notify_customer", llm=llm), _step("notify_customer", order, incident))

    assert response.status_code == 202
    events = _events(conn, "notify_customer", incident)
    assert [e["event_type"] for e in events] == ["LlmCallRecorded", "CustomerNotified", "StepCompleted"]
    assert events[1]["payload"]["order_id"] == order
    assert _count(conn, "executor_notify_customer.customer_messages", incident) == 1
    assert _count(conn, "executor_notify_customer.llm_calls", incident) == 1
    assert "Causa: delay. Retraso estimado: 48 h." in llm.requests[0].prompt


def test_a_repeated_step_does_not_repeat_the_effect(database_url, conn):
    """Duplicado tardío / Paso pedido dos veces (otro command_id, misma clave): un solo efecto y el resultado
    guardado. El ejecutor no sabe si la incidencia ya se cerró: effect_keys descarta el duplicado igual."""
    order, incident, key = _order(conn), _incident(), uuid.uuid4().hex
    carrier = FakeService("carrier", "SH")
    api = _api(database_url, "reissue_shipment", service=carrier)

    _execute(api, _step("reissue_shipment", order, incident, key=key))
    _execute(api, _step("reissue_shipment", order, incident, key=key))

    events = _events(conn, "reissue_shipment", incident)
    assert [e["event_type"] for e in events] == ["ShipmentReissued", "StepCompleted", "StepCompleted"]
    assert events[1]["payload"]["result"] == events[2]["payload"]["result"]
    assert _count(conn, "executor_reissue_shipment.shipments", incident) == 1
    assert carrier.calls == 1  # el segundo no llama al servicio


def test_transient_errors_are_retried_within_the_command(database_url, conn):
    """Servicio caído: reintenta con espera y el efecto se ejecuta una sola vez"""
    order, incident = _order(conn), _incident()
    carrier = FakeService("carrier", "SH", timeouts=2)

    _execute(_api(database_url, "reissue_shipment", service=carrier), _step("reissue_shipment", order, incident))

    assert [e["event_type"] for e in _events(conn, "reissue_shipment", incident)] == ["ShipmentReissued",
                                                                                     "StepCompleted"]
    assert carrier.executed == 1


def test_retries_exhausted_publish_a_retryable_failure(database_url, conn):
    """Error transitorio / Reintentos agotados: el servicio no responde a tiempo y, agotados los reintentos,
    StepFailed con retryable verdadero y attempts, sin efecto"""
    order, incident = _order(conn), _incident()
    carrier = FakeService("carrier", "SH", down=True)

    response = _execute(_api(database_url, "reissue_shipment", service=carrier),
                        _step("reissue_shipment", order, incident))

    assert response.status_code == 202
    [failed] = _events(conn, "reissue_shipment", incident)
    assert failed["event_type"] == "StepFailed"
    assert (failed["payload"]["error"], failed["payload"]["retryable"], failed["payload"]["attempts"]) == (
        "timeout", True, 3)
    assert _count(conn, "executor_reissue_shipment.shipments", incident) == 0


def test_carrier_rejection_is_not_retried(database_url, conn):
    order, incident = _order(conn), _incident()
    carrier = FakeService("carrier", "SH", reject="carrier_rejected")

    _execute(_api(database_url, "reissue_shipment", service=carrier), _step("reissue_shipment", order, incident))

    [failed] = _events(conn, "reissue_shipment", incident)
    assert (failed["payload"]["error"], failed["payload"]["retryable"], failed["payload"]["attempts"]) == (
        "carrier_rejected", False, 1)


def test_refund_above_the_maximum_escalates(database_url, conn):
    """Importe por encima del máximo: no se ejecuta, IncidentEscalated refund_limit y StepFailed"""
    order, incident = _order(conn, amount="10000.00"), _incident()
    payments = FakeService("payments", "RF")

    _execute(_api(database_url, "issue_refund", service=payments),
             _step("issue_refund", order, incident, amount=str(RULES.max_refund_amount + 1)))

    events = _events(conn, "issue_refund", incident)
    assert [e["event_type"] for e in events] == ["IncidentEscalated", "StepFailed"]
    assert events[0]["payload"]["reason"] == "refund_limit"
    assert payments.calls == 0
    assert _count(conn, "executor_issue_refund.refunds", incident) == 0


def test_refund_within_the_limit_is_paid(database_url, conn):
    order, incident = _order(conn), _incident()

    _execute(_api(database_url, "issue_refund"), _step("issue_refund", order, incident, amount="40.00"))

    events = _events(conn, "issue_refund", incident)
    assert [e["event_type"] for e in events] == ["RefundIssued", "StepCompleted"]
    assert events[0]["payload"]["amount"] == "40.00"


def test_an_invalid_draft_is_never_sent(database_url, conn):
    """Borrador con datos de otro pedido: no se envía; StepFailed reintentable invalid_draft"""
    order, incident = _order(conn), _incident()
    llm = FakeLlmClient([{"text": NOTICE + " También el pedido P-999."}] * RULES.max_generations)

    _execute(_api(database_url, "file_claim", llm=llm), _step("file_claim", order, incident))

    events = _events(conn, "file_claim", incident)
    assert [e["event_type"] for e in events] == ["LlmCallRecorded"] * RULES.max_generations + ["StepFailed"]
    assert (events[-1]["payload"]["error"], events[-1]["payload"]["retryable"]) == ("invalid_draft", True)
    assert _count(conn, "executor_file_claim.claims", incident) == 0


def test_a_drafted_step_without_context_is_rejected(database_url, conn):
    order, incident = _order(conn), _incident()
    response = _execute(_api(database_url, "notify_customer"),
                        _step("notify_customer", order, incident, context=False))
    assert response.status_code == 422
    assert response.json()["reason"] == "missing_context"
    assert _events(conn, "notify_customer", incident) == []


def test_another_action_or_incident_is_rejected(database_url, conn):
    order, incident = _order(conn), _incident()
    api = _api(database_url, "reissue_shipment")
    assert _execute(api, _step("issue_refund", order, incident, amount="1")).json()["reason"] == "action_not_handled"
    assert _execute(api, _step("reissue_shipment", order, incident),
                    correlation=_incident()).json()["reason"] == "incident_mismatch"


def test_claim_filed(database_url, conn):
    """Reclamación creada: ExecuteStep de file_claim válido -> ClaimFiled con claim_id y StepCompleted con el mismo step_id"""
    order, incident = _order(conn), _incident()
    llm = FakeLlmClient([{"text": NOTICE}])

    response = _execute(_api(database_url, "file_claim", llm=llm), _step("file_claim", order, incident, step_id="S2"))

    assert response.status_code == 202
    events = _events(conn, "file_claim", incident)
    assert [e["event_type"] for e in events] == ["LlmCallRecorded", "ClaimFiled", "StepCompleted"]
    claim_id = events[1]["payload"]["claim_id"]
    assert claim_id.startswith("C-") and events[1]["payload"]["order_id"] == order
    assert events[2]["payload"]["step_id"] == "S2"
    assert events[2]["payload"]["result"] == {"claim_id": claim_id}
    with conn.transaction():
        [row] = conn.execute("SELECT claim_id, step_id FROM executor_file_claim.claims WHERE incident_id = %s",
                             (incident,)).fetchall()
    assert row == {"claim_id": claim_id, "step_id": "S2"}


def test_transient_error_solved_on_the_second_attempt(database_url, conn):
    """Error transitorio resuelto en el segundo intento: un timeout, el segundo intento tiene éxito y el efecto
    se ejecuta una sola vez"""
    order, incident = _order(conn), _incident()
    carrier = FakeService("carrier", "SH", timeouts=1)

    _execute(_api(database_url, "reissue_shipment", service=carrier), _step("reissue_shipment", order, incident))

    assert [e["event_type"] for e in _events(conn, "reissue_shipment", incident)] == ["ShipmentReissued",
                                                                                     "StepCompleted"]
    assert (carrier.calls, carrier.executed) == (2, 1)
    assert _count(conn, "executor_reissue_shipment.shipments", incident) == 1


def test_executor_restarted_mid_plan_runs_only_the_pending_steps(database_url, conn):
    """Caída de un ejecutor a mitad de un plan: tras reiniciarse procesa los dos pasos pendientes y no repite
    los dos completados, aunque la saga vuelva a invocarlos con el mismo command_id"""
    order, incident = _order(conn), _incident()
    steps = [_step("reissue_shipment", order, incident, step_id=f"S{i}") for i in range(1, 5)]
    commands = [uuid.uuid4() for _ in steps]
    before = FakeService("carrier", "SH")
    api = _api(database_url, "reissue_shipment", service=before)
    for payload, command_id in zip(steps[:2], commands[:2], strict=True):
        assert api.post("/commands/ExecuteStep", json=body(incident, payload, command_id)).status_code == 202

    after = FakeService("carrier", "SH")  # el proceso nuevo no recuerda nada: solo su esquema
    restarted = _api(database_url, "reissue_shipment", service=after)
    replies = [restarted.post("/commands/ExecuteStep", json=body(incident, payload, command_id))
               for payload, command_id in zip(steps, commands, strict=True)]

    assert [r.headers.get("Idempotent-Replayed") for r in replies] == ["true", "true", None, None]
    assert after.calls == 2
    assert _count(conn, "executor_reissue_shipment.shipments", incident) == 4
    completed = [e["payload"]["step_id"] for e in _events(conn, "reissue_shipment", incident)
                 if e["event_type"] == "StepCompleted"]
    assert completed == ["S1", "S2", "S3", "S4"]
