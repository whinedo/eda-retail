"""Escenarios de las specs cqrs-views y event-backbone que necesitan PostgreSQL y Kafka reales.

Cada test usa su propia incidencia y pedido; los que publican en Kafka, el topic propio del test.
"""

from __future__ import annotations

import uuid
from dataclasses import replace

from eda_retail.config import Settings
from eda_retail.event_log.cli import build as build_event_log
from eda_retail.events.catalog import CATALOG, validate_payload
from eda_retail.events.envelope import EventEnvelope
from eda_retail.events.ids import incident_id_for_notice
from eda_retail.infra import command_client
from eda_retail.infra.consumer import Outcome
from eda_retail.infra.kafka import make_producer
from eda_retail.infra.outbox import ComponentRelay
from eda_retail.intake.handler import specs as intake_specs
from eda_retail.interpreter.consumer import build_consumer
from eda_retail.inventory.handler import SAMPLE_DATA_CORRELATION
from eda_retail.inventory.handler import specs as inventory_specs
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.orchestrator.dispatcher import dispatch_once
from eda_retail.orchestrator.saga import Saga, target_url
from eda_retail.planner.handler import specs as planner_specs
from eda_retail.projectors.base import load_event
from eda_retail.rules import BusinessRules

from .component_helpers import body, client
from .kafka_helpers import read_all
from .projection_helpers import LOSS_PLAN, History, publish
from .test_orchestrator_saga import (
    LOSS,
    _commands,
    _deliver,
    _failed,
    _incident_row,
    _open,
    _statuses,
    _step,
    _types,
)
from .test_spec_scenarios_components import NOTICE, _drain, _executor, _view_order

RULES = BusinessRules()
# Comandos del catálogo (docs/README.md, «Catálogo de comandos»): ninguno debe aparecer en Kafka.
COMMAND_TYPES = {"RegisterCarrierNotice", "SeedSampleData", "OpenIncident", "CreatePlan", "RevisePlan",
                 "ExecuteStep", "RecordLlmCall"}


def _db_settings(base_settings: Settings, database_url: str) -> Settings:
    return replace(base_settings, database_url=database_url)


def _events(conn, schema: str, incident: str) -> list[EventEnvelope]:
    """Los eventos del outbox de `schema` de la incidencia, en orden, tal como los publicaría su relay."""
    with conn.transaction():
        rows = conn.execute(f"SELECT * FROM {schema}.outbox WHERE correlation_id = %s ORDER BY id",
                            (incident,)).fetchall()
    return [load_event(r) for r in rows]


def _consume(conn, saga: Saga, event: EventEnvelope) -> None:
    """La saga consume el evento, en una transacción como la del inbox."""
    with conn.transaction():
        saga.handlers()[event.event_type](conn, event)


def _only_due(conn, incident: str, command_type: str | None = None) -> None:
    """El dispatcher solo ve los comandos pendientes de esta incidencia (la base de tests es compartida)."""
    with conn.transaction():
        conn.execute("UPDATE orchestrator.command_outbox SET next_attempt_at = 'infinity' WHERE status = 'pending'"
                     " AND NOT (incident_id = %s AND command_type = coalesce(%s::text, command_type))",
                     (incident, command_type))


def _rest_invoke(apis: dict, replies: list):
    """`invoke` del dispatcher por REST contra los TestClient de los componentes, por su URL."""
    def invoke(url, command_type, *, command_id, correlation_id, causation_id, payload, timeout):
        response = apis[url].post(f"/commands/{command_type}", json={
            "command_id": str(command_id), "correlation_id": correlation_id, "causation_id": str(causation_id),
            "payload": payload})
        replies.append((command_type, response.status_code))
        return command_client.CommandReply(response.status_code, response.json())
    return invoke


# --- cqrs-views: «Un único dueño por estado» ------------------------------------------------------


def test_two_revisions_on_the_same_incident(base_settings, database_url, conn):
    """Dos revisiones sobre la misma incidencia"""
    saga = Saga(_db_settings(base_settings, database_url), RULES)
    incident = _open(conn, saga, LOSS)
    _failed(conn, saga, incident, "S3")  # out_of_stock: RevisePlan de la versión 1
    _deliver(conn, saga, "PlanRevised", incident, {
        "incident_id": incident, "plan_version": 2, "reason": "sin stock",
        "steps": [_step("S1", "notify_customer"), _step("S2", "file_claim"), _step("S5", "issue_refund", amount=10)],
    }, producer="replanner")
    _failed(conn, saga, incident, "S2", plan_version=2, error="claim_rejected")  # RevisePlan de la versión 2
    assert _incident_row(conn, incident)["plan_version"] == 2

    _deliver(conn, saga, "PlanRevised", incident, {
        "incident_id": incident, "plan_version": 3, "reason": "reclamación rechazada",
        "steps": [_step("S1", "notify_customer"), _step("S5", "issue_refund", amount=10)],
    }, producer="replanner")

    assert _incident_row(conn, incident)["plan_version"] == 3
    before = (_incident_row(conn, incident), _statuses(conn, incident), _commands(conn, incident),
              _types(conn, incident))

    _deliver(conn, saga, "PlanRevised", incident, {  # también basado en la versión 2: obsoleto
        "incident_id": incident, "plan_version": 3, "reason": "otra revisión",
        "steps": [_step("S1", "notify_customer"), _step("S9", "file_claim")],
    }, producer="replanner")

    after = (_incident_row(conn, incident), _statuses(conn, incident), _commands(conn, incident),
             _types(conn, incident))
    assert after == before
    assert after[0]["plan_version"] == 3
    assert "S9" not in after[1]


# --- event-backbone: «Kafka como registro de eventos» ---------------------------------------------


def _logged(conn, topic: str) -> dict[uuid.UUID, int]:
    """event_id -> filas de la vista del log con ese evento, de los mensajes del topic del test."""
    with conn.transaction():
        rows = conn.execute("SELECT event_id, count(*) AS n FROM event_log.events WHERE kafka_topic = %s"
                            " GROUP BY event_id", (topic,)).fetchall()
    return {r["event_id"]: r["n"] for r in rows}


def _event_log_catch_up(settings: Settings) -> None:
    consumer = build_event_log(settings)
    try:
        consumer.catch_up()
    finally:
        consumer.close()


def test_view_rebuilt_from_kafka(settings, conn, topic):
    """Vista reconstruida desde Kafka"""
    # `event_log.events` es solo de añadir (no admite DELETE ni TRUNCATE): la vista vacía es la de un topic
    # nuevo, del que aún no tiene ninguna fila ni posición.
    history = History()
    history.delayed()
    history.plan_created(LOSS_PLAN[:1])
    history.requested(LOSS_PLAN[0])
    history.customer_notified()
    history.completed("S1", {"message_id": "M1"})
    history.resolved()
    events = history.take()
    publish(settings, events)
    assert _logged(conn, topic) == {}

    _event_log_catch_up(settings)

    assert _logged(conn, topic) == {e.event_id: 1 for e in events}

    # Otra vez desde el principio (sin posición guardada): cada evento sigue una sola vez.
    with conn.transaction():
        conn.execute("DELETE FROM event_log.offsets WHERE consumer = 'event_log' AND kafka_topic = %s", (topic,))
    _event_log_catch_up(settings)

    assert _logged(conn, topic) == {e.event_id: 1 for e in events}


# --- event-backbone: «Invocación de comandos» -----------------------------------------------------


def _seed_inventory(api, order: str) -> None:
    sku = f"SKU-{uuid.uuid4().hex[:8]}"
    response = api.post("/commands/SeedSampleData", json=body(SAMPLE_DATA_CORRELATION, {
        "orders": [{"order_id": order, "customer_ref": "CUST-1", "sku": sku, "quantity": 1, "amount": "40.00",
                    "carrier": "demo-express"}],
        "stock": [{"warehouse": "W1", "sku": sku, "available": 5}]}))
    assert response.status_code == 202


def test_the_result_arrives_as_an_event(base_settings, database_url, conn):
    """El resultado llega como evento"""
    settings = _db_settings(base_settings, database_url)
    saga = Saga(settings, RULES)
    order, incident = f"P-{uuid.uuid4().hex[:8]}", f"I-{uuid.uuid4().hex[:8]}"
    _deliver(conn, saga, "ShipmentDelayed", incident, {"incident_id": incident, "order_ids": [order],
                                                        "cause": "loss", "estimated_delay_hours": 24,
                                                        "severity": "high"}, producer="interpreter")
    _deliver(conn, saga, "PlanCreated", incident, {"incident_id": incident, "plan_version": 1,
                                                    "steps": [_step("S1", "reserve_stock", order=order)]})
    assert _statuses(conn, incident) == {"S1": "requested"}
    replies: list = []
    with client(database_url, "inventory", inventory_specs(Settings())) as inventory:
        _seed_inventory(inventory, order)
        _only_due(conn, incident, "ExecuteStep")

        assert dispatch_once(conn, settings, RULES,
                             invoke=_rest_invoke({target_url(settings, "inventory"): inventory}, replies))

    assert replies == [("ExecuteStep", 202)]
    [command] = _commands(conn, incident, "ExecuteStep")
    assert command["status"] == "delivered"
    assert _statuses(conn, incident) == {"S1": "requested"}  # la respuesta no cambia el paso

    [completed] = [e for e in _events(conn, "inventory", incident) if e.event_type == "StepCompleted"]
    assert completed.producer == "inventory"
    _consume(conn, saga, completed)

    assert _statuses(conn, incident) == {"S1": "completed"}


class _NoKafka:
    """El intérprete se usa con `process`: sin Kafka."""

    def subscribe(self, topics) -> None:
        pass


SCHEMAS = ("intake", "interpreter", "orchestrator", "planner", "executor_notify_customer")


def _complete_incident(settings: Settings, database_url: str, conn) -> str:
    """Una incidencia completa, del aviso a `IncidentResolved`: comandos por REST y eventos en los outbox."""
    order = _view_order(conn)
    notice = f"N-{uuid.uuid4().hex[:8]}"
    incident = incident_id_for_notice(notice)
    with client(database_url, "intake", intake_specs()) as intake:
        assert intake.post("/commands/RegisterCarrierNotice", json=body(incident, {
            "carrier_notice_id": notice, "carrier": "demo-express",
            "raw_text": f"Retraso de 24 horas en el pedido {order}."})).status_code == 202
    [received] = _events(conn, "intake", incident)
    interpretation = {"order_ids": [order], "cause": "delay", "estimated_delay_hours": 24, "severity": "low"}
    interpreter = build_consumer(conn, _NoKafka(), settings, FakeLlmClient([interpretation]), RULES)
    assert interpreter.process(received) is Outcome.PROCESSED
    [delayed] = [e for e in _events(conn, "interpreter", incident) if e.event_type == "ShipmentDelayed"]

    saga = Saga(settings, RULES)
    _consume(conn, saga, delayed)
    planner = client(database_url, "planner", planner_specs(replace(Settings(), database_url=database_url),
                                                             FakeLlmClient([]), RULES))
    executor = _executor(database_url, "notify_customer", llm=FakeLlmClient([{"text": NOTICE}]))
    invoke = _rest_invoke({target_url(settings, "planner"): planner,
                           target_url(settings, "executor_notify_customer"): executor}, [])
    consumed: set[uuid.UUID] = set()
    for _ in range(10):
        _only_due(conn, incident)
        if not dispatch_once(conn, settings, RULES, invoke=invoke):
            break
        for schema in ("planner", "executor_notify_customer"):
            for event in _events(conn, schema, incident):
                if event.event_id not in consumed and event.event_type in saga.handlers():
                    consumed.add(event.event_id)
                    _consume(conn, saga, event)
    assert _incident_row(conn, incident)["status"] == "resolved"
    return incident


def test_only_events_in_kafka(settings, database_url, conn, topic):
    """Solo eventos en Kafka"""
    _drain(conn, *SCHEMAS)
    incident = _complete_incident(settings, database_url, conn)
    with conn.transaction():  # otros trabajos que hayan corrido entre medias no van a este topic
        for schema in SCHEMAS:
            conn.execute(f"UPDATE {schema}.outbox SET published_at = now()"
                         " WHERE published_at IS NULL AND correlation_id <> %s", (incident,))
    written = [e for schema in SCHEMAS for e in _events(conn, schema, incident)]
    for schema in SCHEMAS:
        relay = ComponentRelay(conn, make_producer(settings.kafka_bootstrap_servers), schema, topic)
        while relay.run_once():
            pass

    messages = read_all(settings.kafka_bootstrap_servers, topic, expected=len(written))

    assert len(messages) == len(written)
    assert {m.value["event_id"] for m in messages} == {str(e.event_id) for e in written}
    assert {"CarrierNoticeReceived", "ShipmentDelayed", "PlanCreated", "StepRequested", "StepCompleted",
            "IncidentResolved"} <= {m.event_type for m in messages}
    for m in messages:
        assert m.event_type in CATALOG and m.event_type not in COMMAND_TYPES
        assert "command_id" not in m.value and "command_type" not in m.value
        event = EventEnvelope.model_validate(m.value)
        assert event.event_type == m.event_type
        validate_payload(event.event_type, event.payload)
