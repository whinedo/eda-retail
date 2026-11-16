"""Escenarios de las specs idempotency y resilience contra PostgreSQL y Kafka reales.

Reutilizan los helpers de los tests de los ejecutores (`test_step_executors`, `test_inventory`) y de los
escenarios de los componentes (`test_spec_scenarios_components`). Las claves se calculan como la saga:
`hash(incident_id, step_id, versión del plan en la que se creó el paso)`.
"""

from __future__ import annotations

import uuid

import pytest

from eda_retail.infra.consumer import Outcome
from eda_retail.infra.effect_keys import compute_idempotency_key
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.orchestrator.saga import Saga
from eda_retail.step_executors.services import FakeService

from .component_helpers import body, outbox
from .projection_helpers import publish
from .test_inventory import _api as _inventory_api
from .test_inventory import _available, _reservations, _seed
from .test_inventory import _step as _inventory_step
from .test_spec_scenarios_components import RULES, _create_plans, _delayed, _orchestrator_consumer, _poll
from .test_step_executors import NOTICE, _api, _count, _events, _execute, _incident, _order, _step


def _key(incident: str, step_id: str, origin_version: int = 1) -> str:
    return compute_idempotency_key(incident, step_id, origin_plan_version=origin_version)


# --- idempotency ------------------------------------------------------------------------------


def test_plan_revision_after_a_notice_was_sent(database_url, conn):
    """Revisión del plan tras un aviso enviado"""
    order, incident = _order(conn), _incident()
    key = _key(incident, "S1")  # el paso S1 se creó en la versión 1 del plan
    mail = FakeService("mail", "M")
    api = _api(database_url, "notify_customer", llm=FakeLlmClient([{"text": NOTICE}]), service=mail)
    _execute(api, _step("notify_customer", order, incident, key=key))
    notified = [e for e in _events(conn, "notify_customer", incident) if e["event_type"] == "CustomerNotified"]
    assert [e["payload"]["order_id"] for e in notified] == [order]  # CustomerNotified ya publicado para P1

    # El replanner publica la versión 2, que mantiene notify_customer: el paso conserva su clave.
    kept = _step("notify_customer", order, incident, key=key) | {"plan_version": 2}
    response = _execute(api, kept)

    assert response.status_code == 202
    assert (mail.calls, mail.executed) == (1, 1)  # no se envía otro aviso a ese cliente
    assert _count(conn, "executor_notify_customer.customer_messages", incident) == 1
    events = _events(conn, "notify_customer", incident)
    assert [e["event_type"] for e in events] == ["LlmCallRecorded", "CustomerNotified", "StepCompleted",
                                                 "StepCompleted"]
    assert events[3]["payload"]["plan_version"] == 2
    assert events[3]["payload"]["result"] == events[2]["payload"]["result"]


@pytest.mark.xfail(strict=True, reason=(
    "file_claim solo deduplica por idempotency_key: otro ExecuteStep de file_claim para el mismo pedido con otro "
    "step_id (otra clave) crea una segunda reclamación; executor_file_claim.claims no es única por "
    "(incident_id, order_id) ni el ejecutor busca una reclamación existente"))
def test_claim_requested_twice(database_url, conn):
    """Reclamación solicitada dos veces"""
    order, incident = _order(conn), _incident()
    claims = FakeService("claims", "C")
    api = _api(database_url, "file_claim", llm=FakeLlmClient([{"text": NOTICE}] * 2), service=claims)
    _execute(api, _step("file_claim", order, incident, key=_key(incident, "S2"), step_id="S2"))
    [filed] = [e for e in _events(conn, "file_claim", incident) if e["event_type"] == "ClaimFiled"]
    claim_id = filed["payload"]["claim_id"]  # reclamación ya creada para P1 en I1

    # El replanner vuelve a solicitar file_claim para P1 (paso nuevo de la versión 2).
    again = _step("file_claim", order, incident, key=_key(incident, "S5", 2), step_id="S5") | {"plan_version": 2}
    response = _execute(api, again)

    assert response.status_code == 202
    events = _events(conn, "file_claim", incident)
    completed = [e["payload"] for e in events if e["event_type"] == "StepCompleted"]
    assert completed[-1]["step_id"] == "S5"
    assert completed[-1]["result"] == {"claim_id": claim_id}  # devuelve el claim_id existente
    assert [e["event_type"] for e in events].count("ClaimFiled") == 1  # no crea otra reclamación
    assert _count(conn, "executor_file_claim.claims", incident) == 1
    assert claims.executed == 1


def test_log_replay_with_active_executors(database_url, conn):
    """Reproducción del log con ejecutores activos"""
    order, incident = _order(conn), _incident()
    sku = f"SKU-{uuid.uuid4().hex[:8]}"
    payments = FakeService("payments", "RF")
    refunds = _api(database_url, "issue_refund", service=payments)
    reserve_key, refund_key = _key(incident, "S1"), _key(incident, "S2")
    reserve_command, refund_command = uuid.uuid4(), uuid.uuid4()
    refund = _step("issue_refund", order, incident, key=refund_key, amount="40.00", step_id="S2")
    with _inventory_api(database_url) as inventory:
        _seed(inventory, order=order, sku=sku, quantity=2, available=5)
        _inventory_step(inventory, incident, "S1", "reserve_stock", {"order_id": order}, key=reserve_key,
                        command_id=reserve_command)
        refunds.post("/commands/ExecuteStep", json=body(incident, refund, refund_command))
        [reservation] = _reservations(conn, incident)

        # El log se reproduce por error: los ejecutores reciben de nuevo los mismos ExecuteStep (mismo
        # command_id) y, si la saga los vuelve a emitir, con otro command_id y la misma clave.
        for command_id in (reserve_command, uuid.uuid4()):
            assert _inventory_step(inventory, incident, "S1", "reserve_stock", {"order_id": order},
                                   key=reserve_key, command_id=command_id).status_code == 202
        for command_id in (refund_command, uuid.uuid4()):
            assert refunds.post("/commands/ExecuteStep",
                                json=body(incident, refund, command_id)).status_code == 202

    # Ninguna reserva nueva: la misma reserva activa y el stock descontado una sola vez.
    assert _reservations(conn, incident) == [reservation]
    assert _available(conn, sku) == 3
    assert [e["event_type"] for e in outbox(conn, "inventory", incident)].count("StockReserved") == 1
    # Ningún reembolso nuevo.
    assert _count(conn, "executor_issue_refund.refunds", incident) == 1
    assert payments.executed == 1
    assert [e["event_type"] for e in _events(conn, "issue_refund", incident)].count("RefundIssued") == 1


# --- resilience -------------------------------------------------------------------------------


def _inbox_rows(conn, event_id: uuid.UUID) -> int:
    with conn.transaction():
        return conn.execute("SELECT count(*) AS n FROM orchestrator.inbox WHERE message_id = %s",
                            (event_id,)).fetchone()["n"]


def test_failure_during_processing(conn, connect_db, settings):
    """Fallo durante el procesamiento"""
    incident = _incident()
    event = _delayed(incident, "P-1")
    publish(settings, [event])
    saga = Saga(settings, RULES)
    group = f"test-processing-{uuid.uuid4().hex[:8]}"

    def fails_before_commit(conn_, received):
        saga.handlers()["ShipmentDelayed"](conn_, received)  # escribe su estado en la transacción...
        raise RuntimeError("caída antes de confirmar la transacción")  # ...y falla antes del COMMIT

    first = _orchestrator_consumer(connect_db, settings, group, {"ShipmentDelayed": fails_before_commit})
    msg = _poll(first)
    try:
        with pytest.raises(RuntimeError):
            first.handle_message(msg)
        assert first.committed_offset(msg) < 0  # el offset no se confirmó
    finally:
        first.close()
    assert _create_plans(conn, incident) == 0 and _inbox_rows(conn, event.event_id) == 0  # transacción deshecha

    restarted = _orchestrator_consumer(connect_db, settings, group, saga.handlers())
    again = _poll(restarted)
    try:
        # Kafka le entrega de nuevo el evento porque su offset no se confirmó.
        assert (again.partition(), again.offset()) == (msg.partition(), msg.offset())
        assert restarted.event_id_of(again) == event.event_id
        assert restarted.handle_message(again) is Outcome.PROCESSED
        restarted.commit(again)
    finally:
        restarted.close()
    assert _create_plans(conn, incident) == 1 and _inbox_rows(conn, event.event_id) == 1
