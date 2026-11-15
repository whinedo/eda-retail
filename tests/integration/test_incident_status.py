"""cqrs-views: proyector de incident_status sobre Kafka, posición en views.offsets y consulta.

Las historias se publican en el topic del test como eventos del catálogo (`History`), como los
publicarían los relays del intérprete, el planificador, el orquestador y los ejecutores.
"""

from __future__ import annotations

import uuid

from eda_retail.events.envelope import EventEnvelope
from eda_retail.projectors.incident_status import PROJECTOR_NAME
from eda_retail.queries.incident_status import get_incident_status

from .projection_helpers import LOSS_PLAN, History, catch_up, publish

VIEW = "incident_status"


def _project(settings, conn, history: History) -> None:
    publish(settings, history.take())
    catch_up(settings, conn, VIEW)


def _view(conn, incident: str) -> dict:
    row = conn.execute("SELECT * FROM views.view_incident_status WHERE incident_id = %s", (incident,)).fetchone()
    conn.commit()
    return row


def _status(settings, conn, incident: str):
    return get_incident_status(conn, incident, topic=settings.events_topic)


def _statuses(status) -> dict[str, str]:
    return {s["step_id"]: s["status"] for s in status.steps}


def _offsets(settings, conn) -> dict[int, int]:
    rows = conn.execute(
        "SELECT kafka_partition, kafka_offset FROM views.offsets WHERE consumer = %s AND kafka_topic = %s",
        (PROJECTOR_NAME, settings.events_topic),
    ).fetchall()
    conn.commit()
    return {r["kafka_partition"]: r["kafka_offset"] for r in rows}


def _notify_done(history: History) -> None:
    history.requested(LOSS_PLAN[0])
    history.customer_notified()
    history.completed("S1", {"message_id": "M1"})


def test_incident_with_plan_in_progress(conn, settings):
    """Incidencia con plan en curso"""
    history = History()
    history.delayed()
    history.plan_created(LOSS_PLAN[:3])
    _notify_done(history)
    _project(settings, conn, history)
    status = _status(settings, conn, history.incident)
    assert status.status == "open" and status.plan_version == 1
    assert _statuses(status) == {"S1": "completed", "S2": "pending", "S3": "pending"}
    assert [s["action"] for s in status.steps] == ["notify_customer", "file_claim", "reserve_stock"]


def test_revised_plan_keeps_step_status(conn, settings):
    """Una revisión conserva el estado de los pasos mantenidos y vuelve a pending los fallidos"""
    history = History()
    history.delayed()
    history.plan_created(LOSS_PLAN)
    _notify_done(history)
    history.requested(LOSS_PLAN[2])
    history.failed("S3", "out_of_stock")
    history.plan_revised([s for s in LOSS_PLAN if s["step_id"] != "S1"])
    _project(settings, conn, history)
    status = _status(settings, conn, history.incident)
    assert status.plan_version == 2
    assert _statuses(status) == {"S1": "completed", "S2": "pending", "S3": "pending", "S4": "pending"}


def test_states_after_a_compensation(conn, settings):
    """Estados tras una compensación: el paso aparece compensating y pasa a compensated cuando release_stock termina"""
    history = History()
    history.delayed()
    history.plan_created(LOSS_PLAN)
    history.requested(LOSS_PLAN[2])
    history.completed("S3", {"reservation_id": "R1", "warehouse": "W1"}, producer="inventory")
    history.requested(LOSS_PLAN[3])
    history.failed("S4", "carrier_rejected", producer="executor-reissue_shipment")
    refund = {"step_id": "S5", "action": "issue_refund", "params": {"order_id": "P1", "amount": "10"},
              "depends_on": []}
    history.plan_revised([*LOSS_PLAN[:2], refund])
    history.requested({"step_id": "S3-comp", "action": "release_stock",
                       "params": {"order_id": "P1", "reservation_id": "R1"}}, plan_version=2, origin_plan_version=2)
    _project(settings, conn, history)

    status = _status(settings, conn, history.incident)
    assert status.step("S3")["status"] == "compensating"
    assert status.step("S3-comp")["status"] == "requested"

    history.completed("S3-comp", {"released": True, "reservation_id": "R1"}, plan_version=2, producer="inventory")
    _project(settings, conn, history)

    status = _status(settings, conn, history.incident)
    assert status.step("S3")["status"] == "compensated"
    assert status.step("S3-comp")["status"] == "completed"


def test_duplicate_event_in_projector(conn, settings):
    """Evento duplicado en un proyector"""
    history = History()
    history.delayed()
    history.plan_created(LOSS_PLAN[:1])
    history.requested(LOSS_PLAN[0])
    completed = history.completed("S1", {"message_id": "M1"})
    _project(settings, conn, history)
    before = _view(conn, history.incident)
    # El mismo StepCompleted vuelto a publicar (offset nuevo): views.applied_events lo descarta.
    publish(settings, [completed])
    catch_up(settings, conn, VIEW)
    after = _view(conn, history.incident)
    assert {k: v for k, v in after.items() if k != "updated_at"} == {
        k: v for k, v in before.items() if k != "updated_at"
    }


def test_projector_restart(conn, settings, connect_db):
    """Reinicio de un proyector: continúa desde el offset siguiente y no aplica dos veces lo anterior"""
    first, second = History(), History()
    [p1] = publish(settings, [first.escalated("no_valid_orders", producer="interpreter")])
    assert catch_up(settings, connect_db(), VIEW) == 1
    assert _offsets(settings, conn)[p1.partition] == p1.offset

    [p2] = publish(settings, [second.escalated("no_valid_orders", producer="interpreter")])
    assert catch_up(settings, connect_db(), VIEW) == 1  # proceso nuevo, misma posición guardada
    assert _offsets(settings, conn)[p2.partition] == p2.offset
    assert _status(settings, conn, first.incident).status == "escalated"
    assert _status(settings, conn, second.incident).status == "escalated"


def test_status_query(conn, settings):
    """Consulta del estado"""
    history = History()
    history.delayed()
    history.plan_created(LOSS_PLAN)
    _project(settings, conn, history)
    status = _status(settings, conn, history.incident)
    view = _view(conn, history.incident)
    assert (status.status, status.plan_version, status.steps) == (view["status"], view["plan_version"], view["steps"])
    assert status.applied == {p: o for p, o in _offsets(settings, conn).items() if o >= 0}


def test_read_right_after_write(conn, settings):
    """Lectura justo después de escribir"""
    history = History()
    history.delayed()
    _project(settings, conn, history)
    [e1] = publish(settings, [history.plan_created(LOSS_PLAN)])  # el PlanCreated que acaba de publicar el planificador
    status = _status(settings, conn, history.incident)
    assert status.applied.get(e1.partition, -1) < e1.offset
    assert status.plan_version == 0  # la vista aún no refleja E1
    catch_up(settings, conn, VIEW)
    assert _status(settings, conn, history.incident).applied[e1.partition] >= e1.offset


def test_escalated_incident(conn, settings):
    """Incidencia escalada"""
    history = History()
    history.escalated("no_valid_orders", producer="interpreter")  # el intérprete escala sin ShipmentDelayed
    _project(settings, conn, history)
    status = _status(settings, conn, history.incident)
    assert (status.status, status.escalation_reason) == ("escalated", "no_valid_orders")


def test_unknown_incident_query(conn):
    """La consulta de una incidencia que la vista no conoce devuelve None con la posición"""
    assert get_incident_status(conn, "I-no-existe") is None


def test_invalid_event_does_not_block_the_view(conn, settings):
    """Un evento con payload fuera del catálogo se salta: no bloquea el proyector"""
    history = History()
    history.delayed()
    _project(settings, conn, history)
    poison = EventEnvelope.model_validate({
        "event_id": uuid.uuid4(), "event_type": "PlanCreated", "event_version": 1,
        "occurred_at": "2026-11-13T00:00:00Z", "correlation_id": history.incident, "causation_id": None,
        "producer": "test", "payload": {"incident_id": history.incident},
    })
    [poisoned] = publish(settings, [poison])
    history.plan_created(LOSS_PLAN[:1])
    _project(settings, conn, history)
    status = _status(settings, conn, history.incident)
    assert status.applied[poisoned.partition] > poisoned.offset
    assert status.plan_version == 1 and [s["step_id"] for s in status.steps] == ["S1"]
