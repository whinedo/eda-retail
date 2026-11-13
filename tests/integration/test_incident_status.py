"""cqrs-views: proyector de incident_status, posición en projector_offsets y consulta."""

from __future__ import annotations

import uuid
from datetime import timedelta

from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.event_store import append_events
from eda_retail.projectors.base import Projector, load_event
from eda_retail.projectors.incident_status import build_projector
from eda_retail.queries.incident_status import get_incident_status

from .command_helpers import open_incident
from .test_plans import LOSS_PLAN, create, revise
from .test_steps import record, request


def _catch_up(conn, projector: Projector) -> None:
    while projector.run_once(conn):
        pass


def _status_projector() -> Projector:
    # Sin espera en los huecos: la base de tests es compartida y otros tests dejan rollbacks.
    return build_projector(gap_timeout=timedelta(0))


def _view(conn, incident: str) -> dict:
    row = conn.execute("SELECT * FROM view_incident_status WHERE incident_id = %s", (incident,)).fetchone()
    conn.commit()
    return row


def _statuses(status) -> dict[str, str]:
    return {s["step_id"]: s["status"] for s in status.steps}


def _escalation(incident: str) -> EventEnvelope:
    return build_event("IncidentEscalated", {"incident_id": incident, "reason": "test"},
                       correlation_id=incident, causation_id=None, producer="test")


class _Counting:
    """Proyector de prueba: anota qué seq aplica de los eventos de sus incidencias."""

    def __init__(self, name: str, incidents: set[str]) -> None:
        self.applied: list[int] = []
        self.incidents = incidents
        self.projector = Projector(name, {"IncidentEscalated": self._apply}, gap_timeout=timedelta(seconds=30))

    def start_at_end(self, conn) -> _Counting:
        """Empieza tras el último evento: el resto del log es de otros tests."""
        with conn.transaction():
            conn.execute(
                "INSERT INTO projector_offsets (projector, last_seq) SELECT %s, coalesce(max(seq), 0) FROM events",
                (self.projector.name,),
            )
        return self

    def _apply(self, conn, seq: int, event: EventEnvelope) -> None:
        if event.correlation_id in self.incidents:
            self.applied.append(seq)


def test_incident_with_plan_in_progress(conn):
    """Incidencia con plan en curso"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN[:3])
    assert request(conn, incident, "S1").accepted
    assert record(conn, incident, "S1", result={"message_id": "M1"},
                  effect={"order_id": "P1", "message_id": "M1"}).accepted
    _catch_up(conn, _status_projector())
    status = get_incident_status(conn, incident)
    assert status.status == "open" and status.plan_version == 1
    assert _statuses(status) == {"S1": "completed", "S2": "pending", "S3": "pending"}
    assert [s["action"] for s in status.steps] == ["notify_customer", "file_claim", "reserve_stock"]


def test_revised_plan_keeps_step_status(conn):
    """Una revisión conserva el estado de los pasos mantenidos y vuelve a pending los fallidos"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    assert request(conn, incident, "S1").accepted
    assert record(conn, incident, "S1", result={"message_id": "M1"},
                  effect={"order_id": "P1", "message_id": "M1"}).accepted
    assert request(conn, incident, "S3").accepted
    assert record(conn, incident, "S3", outcome="failed", error="out_of_stock", retryable=False).accepted
    assert revise(conn, incident, [s for s in LOSS_PLAN if s["step_id"] != "S1"]).accepted
    _catch_up(conn, _status_projector())
    status = get_incident_status(conn, incident)
    assert status.plan_version == 2
    assert _statuses(status) == {"S1": "completed", "S2": "pending", "S3": "pending", "S4": "pending"}


def test_duplicate_event_in_projector(conn):
    """Evento duplicado en un proyector"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN[:1])
    assert request(conn, incident, "S1").accepted
    assert record(conn, incident, "S1", result={"message_id": "M1"},
                  effect={"order_id": "P1", "message_id": "M1"}).accepted
    projector = _status_projector()
    _catch_up(conn, projector)
    before = _view(conn, incident)
    row = conn.execute(
        "SELECT * FROM events WHERE correlation_id = %s AND event_type = 'StepCompleted'", (incident,)
    ).fetchone()
    with conn.transaction():
        projector.apply(conn, row["seq"], load_event(row))
    after = _view(conn, incident)
    assert {k: v for k, v in after.items() if k != "updated_at"} == {
        k: v for k, v in before.items() if k != "updated_at"
    }


def test_projector_restart(conn):
    """Reinicio de un proyector"""
    name = f"test-{uuid.uuid4().hex[:8]}"
    first, second = f"I-{uuid.uuid4().hex[:8]}", f"I-{uuid.uuid4().hex[:8]}"
    counting = _Counting(name, {first, second}).start_at_end(conn)
    with conn.transaction():
        append_events(conn, [_escalation(first)])
    _catch_up(conn, counting.projector)
    last_seq = conn.execute("SELECT last_seq FROM projector_offsets WHERE projector = %s", (name,)).fetchone()["last_seq"]
    conn.commit()
    assert len(counting.applied) == 1 and counting.applied[0] <= last_seq

    with conn.transaction():
        [new_seq] = append_events(conn, [_escalation(second)])
    restarted = _Counting(name, {first, second})  # proceso nuevo, misma posición guardada
    _catch_up(conn, restarted.projector)
    assert restarted.applied == [new_seq]  # continúa tras last_seq y no repite el anterior
    assert new_seq > last_seq


def test_projector_waits_for_uncommitted_lower_seq(conn, connect_db):
    """Un proyector no salta un seq menor aún sin confirmar (se perdería ese evento)"""
    slow, fast = f"I-{uuid.uuid4().hex[:8]}", f"I-{uuid.uuid4().hex[:8]}"
    counting = _Counting(f"test-{uuid.uuid4().hex[:8]}", {slow, fast}).start_at_end(conn)
    _catch_up(conn, counting.projector)
    other = connect_db()
    with other.transaction():
        [slow_seq] = append_events(other, [_escalation(slow)])  # seq menor, sin confirmar todavía
        with conn.transaction():
            [fast_seq] = append_events(conn, [_escalation(fast)])
        _catch_up(conn, counting.projector)
        assert counting.applied == []  # se detiene en el hueco
    _catch_up(conn, counting.projector)
    assert counting.applied == [slow_seq, fast_seq]


def test_projector_skips_expired_gap(conn, connect_db):
    """Un hueco que deja un rollback se salta pasado el tiempo de espera"""
    lost, kept = f"I-{uuid.uuid4().hex[:8]}", f"I-{uuid.uuid4().hex[:8]}"
    projector_name = f"test-{uuid.uuid4().hex[:8]}"
    counting = _Counting(projector_name, {lost, kept}).start_at_end(conn)
    _catch_up(conn, counting.projector)
    other = connect_db()
    other.execute("BEGIN")
    append_events(other, [_escalation(lost)])
    with conn.transaction():
        [kept_seq] = append_events(conn, [_escalation(kept)])
    other.rollback()
    impatient = _Counting(projector_name, {lost, kept})
    impatient.projector.gap_timeout = timedelta(0)
    _catch_up(conn, impatient.projector)
    assert impatient.applied == [kept_seq]


def test_status_query(conn):
    """Consulta del estado"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    _catch_up(conn, _status_projector())
    events_before = conn.execute("SELECT count(*) AS n FROM events").fetchone()["n"]
    conn.commit()
    status = get_incident_status(conn, incident)
    view = _view(conn, incident)
    assert (status.status, status.plan_version, status.steps) == (view["status"], view["plan_version"], view["steps"])
    assert conn.execute("SELECT count(*) AS n FROM events").fetchone()["n"] == events_before
    conn.commit()


def test_read_right_after_write(conn):
    """Lectura justo después de escribir"""
    incident, _ = open_incident(conn)
    projector = _status_projector()
    _catch_up(conn, projector)
    result = create(conn, incident, LOSS_PLAN)
    seq = conn.execute("SELECT seq FROM events WHERE event_id = %s", (result.events[0].event_id,)).fetchone()["seq"]
    conn.commit()
    status = get_incident_status(conn, incident)
    assert status.last_applied_seq < seq
    assert status.plan_version == 0  # la vista aún no refleja E1
    _catch_up(conn, projector)
    assert get_incident_status(conn, incident).last_applied_seq >= seq


def test_escalated_incident(conn):
    """Incidencia escalada"""
    incident, _ = open_incident(conn, orders=["P-no-existe"])
    _catch_up(conn, _status_projector())
    status = get_incident_status(conn, incident)
    assert (status.status, status.escalation_reason) == ("escalated", "no_valid_orders")


def test_unknown_incident_query(conn):
    """La consulta de una incidencia que la vista no conoce devuelve None con la posición"""
    assert get_incident_status(conn, "I-no-existe") is None
