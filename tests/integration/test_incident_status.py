"""cqrs-views: proyector de incident_status sobre Kafka, posición en views.offsets y consulta."""

from __future__ import annotations

import uuid

from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.event_store import append_events
from eda_retail.projectors.base import load_event
from eda_retail.projectors.incident_status import PROJECTOR_NAME
from eda_retail.queries.incident_status import get_incident_status

from .command_helpers import open_incident
from .projection_helpers import catch_up, project, publish, publish_log
from .test_plans import LOSS_PLAN, create, revise
from .test_steps import record, request

VIEW = "incident_status"


def _catch_up(settings, conn, *incidents: str) -> None:
    project(settings, conn, VIEW, *incidents)


def _view(conn, incident: str) -> dict:
    row = conn.execute("SELECT * FROM views.view_incident_status WHERE incident_id = %s", (incident,)).fetchone()
    conn.commit()
    return row


def _status(settings, conn, incident: str):
    return get_incident_status(conn, incident, topic=settings.events_topic)


def _statuses(status) -> dict[str, str]:
    return {s["step_id"]: s["status"] for s in status.steps}


def _escalation(incident: str) -> EventEnvelope:
    return build_event("IncidentEscalated", {"incident_id": incident, "reason": "test"},
                       correlation_id=incident, causation_id=None, producer="test")


def _offsets(settings, conn) -> dict[int, int]:
    rows = conn.execute(
        "SELECT kafka_partition, kafka_offset FROM views.offsets WHERE consumer = %s AND kafka_topic = %s",
        (PROJECTOR_NAME, settings.events_topic),
    ).fetchall()
    conn.commit()
    return {r["kafka_partition"]: r["kafka_offset"] for r in rows}


def test_incident_with_plan_in_progress(conn, settings):
    """Incidencia con plan en curso"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN[:3])
    assert request(conn, incident, "S1").accepted
    assert record(conn, incident, "S1", result={"message_id": "M1"},
                  effect={"order_id": "P1", "message_id": "M1"}).accepted
    _catch_up(settings, conn, incident)
    status = _status(settings, conn, incident)
    assert status.status == "open" and status.plan_version == 1
    assert _statuses(status) == {"S1": "completed", "S2": "pending", "S3": "pending"}
    assert [s["action"] for s in status.steps] == ["notify_customer", "file_claim", "reserve_stock"]


def test_revised_plan_keeps_step_status(conn, settings):
    """Una revisión conserva el estado de los pasos mantenidos y vuelve a pending los fallidos"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    assert request(conn, incident, "S1").accepted
    assert record(conn, incident, "S1", result={"message_id": "M1"},
                  effect={"order_id": "P1", "message_id": "M1"}).accepted
    assert request(conn, incident, "S3").accepted
    assert record(conn, incident, "S3", outcome="failed", error="out_of_stock", retryable=False).accepted
    assert revise(conn, incident, [s for s in LOSS_PLAN if s["step_id"] != "S1"]).accepted
    _catch_up(settings, conn, incident)
    status = _status(settings, conn, incident)
    assert status.plan_version == 2
    assert _statuses(status) == {"S1": "completed", "S2": "pending", "S3": "pending", "S4": "pending"}


def test_duplicate_event_in_projector(conn, settings):
    """Evento duplicado en un proyector"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN[:1])
    assert request(conn, incident, "S1").accepted
    assert record(conn, incident, "S1", result={"message_id": "M1"},
                  effect={"order_id": "P1", "message_id": "M1"}).accepted
    _catch_up(settings, conn, incident)
    before = _view(conn, incident)
    row = conn.execute(
        "SELECT * FROM events WHERE correlation_id = %s AND event_type = 'StepCompleted'", (incident,)
    ).fetchone()
    conn.commit()
    # El mismo StepCompleted vuelto a publicar (offset nuevo): views.applied_events lo descarta.
    publish(settings, [load_event(row)])
    catch_up(settings, conn, VIEW)
    after = _view(conn, incident)
    assert {k: v for k, v in after.items() if k != "updated_at"} == {
        k: v for k, v in before.items() if k != "updated_at"
    }


def test_projector_restart(conn, settings, connect_db):
    """Reinicio de un proyector: continúa desde el offset siguiente y no aplica dos veces lo anterior"""
    first, second = f"I-{uuid.uuid4().hex[:8]}", f"I-{uuid.uuid4().hex[:8]}"
    [p1] = publish(settings, [_escalation(first)])
    assert catch_up(settings, connect_db(), VIEW) == 1
    assert _offsets(settings, conn)[p1.partition] == p1.offset

    [p2] = publish(settings, [_escalation(second)])
    assert catch_up(settings, connect_db(), VIEW) == 1  # proceso nuevo, misma posición guardada
    assert _offsets(settings, conn)[p2.partition] == p2.offset
    assert _status(settings, conn, first).status == "escalated"
    assert _status(settings, conn, second).status == "escalated"


def test_status_query(conn, settings):
    """Consulta del estado"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    _catch_up(settings, conn, incident)
    events_before = conn.execute("SELECT count(*) AS n FROM events").fetchone()["n"]
    conn.commit()
    status = _status(settings, conn, incident)
    view = _view(conn, incident)
    assert (status.status, status.plan_version, status.steps) == (view["status"], view["plan_version"], view["steps"])
    assert conn.execute("SELECT count(*) AS n FROM events").fetchone()["n"] == events_before
    conn.commit()


def test_read_right_after_write(conn, settings):
    """Lectura justo después de escribir"""
    incident, _ = open_incident(conn)
    _catch_up(settings, conn, incident)
    assert create(conn, incident, LOSS_PLAN).accepted
    e1 = publish_log(settings, conn, incident)[0]  # el PlanCreated que escribió el comando
    status = _status(settings, conn, incident)
    assert status.applied.get(e1.partition, -1) < e1.offset
    assert status.plan_version == 0  # la vista aún no refleja E1
    catch_up(settings, conn, VIEW)
    assert _status(settings, conn, incident).applied[e1.partition] >= e1.offset


def test_escalated_incident(conn, settings):
    """Incidencia escalada"""
    incident, _ = open_incident(conn, orders=["P-no-existe"])
    _catch_up(settings, conn, incident)
    status = _status(settings, conn, incident)
    assert (status.status, status.escalation_reason) == ("escalated", "no_valid_orders")


def test_unknown_incident_query(conn):
    """La consulta de una incidencia que la vista no conoce devuelve None con la posición"""
    assert get_incident_status(conn, "I-no-existe") is None


def test_invalid_event_does_not_block_the_view(conn, settings):
    """Un evento con payload fuera del catálogo se salta: no bloquea el proyector"""
    incident, _ = open_incident(conn)
    _catch_up(settings, conn, incident)
    poison = EventEnvelope.model_validate({
        "event_id": uuid.uuid4(), "event_type": "PlanCreated", "event_version": 1,
        "occurred_at": "2026-11-13T00:00:00Z", "correlation_id": incident, "causation_id": None,
        "producer": "test", "payload": {"incident_id": incident},
    })
    [poisoned] = publish(settings, [poison])
    result = create(conn, incident, LOSS_PLAN[:1])
    _catch_up(settings, conn, incident)
    status = _status(settings, conn, incident)
    assert status.applied[poisoned.partition] > poisoned.offset
    assert status.plan_version == 1 and [s["step_id"] for s in status.steps] == ["S1"]
    assert result.accepted


def test_projector_does_not_read_the_shared_log(conn, settings):
    """Vistas con posición en Kafka: un evento que solo está en el log compartido no llega a la vista"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    with conn.transaction():
        append_events(conn, [_escalation(incident)])
    catch_up(settings, conn, VIEW)
    assert _status(settings, conn, incident) is None
