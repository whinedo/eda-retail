"""cqrs-views (comandos, concurrencia optimista) e incident-interpretation (avisos e incidencias)."""

from __future__ import annotations

import threading
import time
import uuid

from eda_retail.command.handler import incident_id_for_notice
from eda_retail.command.models import (
    CloseIncident,
    EscalateIncident,
    OpenIncident,
    RegisterCarrierNotice,
    RequestStep,
)

from .command_helpers import add_order, events_of, handler, open_incident, register_notice, version_of


def test_carrier_notice_arrives(conn):
    """Llega un aviso"""
    notice, result = register_notice(conn)
    assert result.accepted
    [ev] = result.events
    assert ev.event_type == "CarrierNoticeReceived"
    assert ev.payload["carrier_notice_id"] == notice
    assert ev.payload["raw_text"] == "el pedido llega con un día de retraso"
    assert ev.correlation_id == incident_id_for_notice(notice)
    assert [e["event_type"] for e in events_of(conn, ev.correlation_id)] == ["CarrierNoticeReceived"]


def test_repeated_notice(conn):
    """Aviso repetido"""
    notice, _ = register_notice(conn)
    incident = incident_id_for_notice(notice)
    again = handler().handle(
        conn,
        RegisterCarrierNotice(
            command_id=uuid.uuid4(), expected_version=0, producer="intake",
            carrier_notice_id=notice, carrier="demo", raw_text="reenviado",
        ),
    )
    assert (again.accepted, again.reason) == (False, "duplicate_notice")

    order = add_order(conn)
    cmd = dict(
        expected_version=0, producer="interpreter", incident_id=incident, carrier_notice_id=notice,
        order_ids=[order], cause="delay", estimated_delay_hours=24, severity="low",
    )
    assert handler().handle(conn, OpenIncident(command_id=uuid.uuid4(), **cmd)).accepted
    conn.commit()
    dup = handler().handle(conn, OpenIncident(command_id=uuid.uuid4(), **cmd))
    conn.commit()
    assert (dup.accepted, dup.reason) == (False, "duplicate_notice")
    assert [e["event_type"] for e in events_of(conn, incident)].count("ShipmentDelayed") == 1


def test_derived_command_and_causation_chain(conn):
    """Comando derivado de un evento / Cadena de causalidad"""
    notice, registered = register_notice(conn)
    e1 = registered.events[0].event_id
    incident = incident_id_for_notice(notice)
    result = handler().handle(
        conn,
        OpenIncident(
            command_id=e1, expected_version=0, producer="interpreter", incident_id=incident,
            carrier_notice_id=notice, order_ids=[add_order(conn)], cause="delay",
            estimated_delay_hours=24, severity="low",
        ),
    )
    [delayed] = result.events
    assert delayed.event_type == "ShipmentDelayed"
    assert (delayed.correlation_id, delayed.causation_id) == (incident, e1)


def test_unknown_order(conn):
    """Pedido inexistente"""
    incident, result = open_incident(conn, orders=["P9-no-existe"])
    assert result.accepted
    assert [e.event_type for e in result.events] == ["IncidentEscalated"]
    assert result.events[0].payload["reason"] == "no_valid_orders"
    assert "ShipmentDelayed" not in [e["event_type"] for e in events_of(conn, incident)]


def test_unknown_orders_are_excluded(conn):
    """Los pedidos inexistentes se excluyen de order_ids"""
    good = add_order(conn)
    _, result = open_incident(conn, orders=[good, "P9-no-existe"])
    assert result.events[0].payload["order_ids"] == [good]


def test_ambiguous_notice_needs_medium_severity(conn):
    """Aviso ambiguo: cause unknown exige severity igual o superior a medium"""
    _, low = open_incident(conn, cause="unknown", severity="low")
    assert (low.accepted, low.reason) == (False, "invalid_severity")
    _, medium = open_incident(conn, cause="unknown", severity="medium")
    assert medium.accepted


def test_command_on_closed_incident(conn):
    """Comando sobre una incidencia cerrada"""
    incident, _ = open_incident(conn)
    v = version_of(conn, incident)
    closed = handler().handle(
        conn,
        EscalateIncident(command_id=uuid.uuid4(), expected_version=v, producer="ops", incident_id=incident, reason="manual"),
    )
    conn.commit()
    assert closed.accepted
    before = len(events_of(conn, incident))
    result = handler().handle(
        conn,
        RequestStep(command_id=uuid.uuid4(), expected_version=v + 1, producer="orchestrator", incident_id=incident, step_id="S1"),
    )
    conn.commit()
    assert (result.accepted, result.reason) == (False, "incident_closed")
    assert len(events_of(conn, incident)) == before


def test_stale_expected_version_is_rejected(conn):
    """Un comando con expected_version distinta se rechaza con concurrency_conflict"""
    incident, _ = open_incident(conn)
    v = version_of(conn, incident)
    result = handler().handle(
        conn,
        EscalateIncident(command_id=uuid.uuid4(), expected_version=v - 1, producer="ops", incident_id=incident, reason="x"),
    )
    conn.commit()
    assert (result.accepted, result.reason) == (False, "concurrency_conflict")
    assert version_of(conn, incident) == v


def test_two_simultaneous_commands_on_same_incident(connect_db):
    """Dos comandos simultáneos sobre la misma incidencia"""
    setup = connect_db()
    incident, _ = open_incident(setup)
    with setup.transaction():
        setup.execute("UPDATE incidents SET version = 4 WHERE incident_id = %s", (incident,))
    a, b = connect_db(), connect_db()
    before = len(events_of(setup, incident))

    def cmd():
        return EscalateIncident(command_id=uuid.uuid4(), expected_version=4, producer="ops", incident_id=incident, reason="r")

    results = {}
    with a.transaction():
        results["a"] = handler().handle(a, cmd())

        def other():
            with b.transaction():
                results["b"] = handler().handle(b, cmd())

        t = threading.Thread(target=other)
        t.start()
        time.sleep(0.5)
        assert t.is_alive()  # B espera al bloqueo de la fila de A
    t.join(timeout=10)

    assert results["a"].accepted and results["a"].version == 5
    assert (results["b"].accepted, results["b"].reason) == (False, "concurrency_conflict")
    assert version_of(setup, incident) == 5
    assert len(events_of(setup, incident)) == before + 1  # solo los eventos de A


def test_rejected_command_writes_nothing_even_inside_caller_transaction(conn):
    """Un comando rechazado no escribe eventos aunque la transacción del llamante continúe"""
    incident, _ = open_incident(conn)
    before = len(events_of(conn, incident))
    with conn.transaction():
        result = handler().handle(
            conn,
            EscalateIncident(command_id=uuid.uuid4(), expected_version=999, producer="ops", incident_id=incident, reason="x"),
        )
        assert not result.accepted
    assert len(events_of(conn, incident)) == before


def test_close_requires_finished_steps(conn):
    """CloseIncident se rechaza si la incidencia no tiene un plan terminado"""
    incident, _ = open_incident(conn)
    result = handler().handle(
        conn,
        CloseIncident(command_id=uuid.uuid4(), expected_version=version_of(conn, incident), producer="o", incident_id=incident, outcome="resolved"),
    )
    conn.commit()
    assert (result.accepted, result.reason) == (False, "no_plan")


def test_invalid_command_payload_is_rejected(conn):
    """Un comando cuyo evento no cumple el catálogo se rechaza con invalid_command"""
    notice, registered = register_notice(conn)
    result = handler().handle(
        conn,
        OpenIncident(
            command_id=registered.events[0].event_id, expected_version=0, producer="interpreter",
            incident_id=incident_id_for_notice(notice), carrier_notice_id=notice,
            order_ids=[add_order(conn)], cause="aliens", estimated_delay_hours=1, severity="high",
        ),
    )
    conn.commit()
    assert (result.accepted, result.reason) == (False, "invalid_command")
    assert "cause" in result.detail


def test_concurrent_open_incident_for_same_notice(connect_db):
    """Aviso repetido (concurrente): varias instancias abren la misma incidencia a la vez"""
    setup = connect_db()
    notice, registered = register_notice(setup)
    incident = incident_id_for_notice(notice)
    order = add_order(setup)
    barrier = threading.Barrier(6)
    results, errors = [], []

    def worker():
        c = connect_db()
        cmd = OpenIncident(
            command_id=registered.events[0].event_id, expected_version=0, producer="interpreter",
            incident_id=incident, carrier_notice_id=notice, order_ids=[order], cause="delay",
            estimated_delay_hours=24, severity="low",
        )
        barrier.wait()
        try:
            with c.transaction():
                results.append(handler().handle(c, cmd))
        except Exception as exc:  # noqa: BLE001 - el test comprueba que no hay ninguna
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)
    assert errors == []
    assert sum(r.accepted for r in results) == 1
    assert {r.reason for r in results if not r.accepted} == {"duplicate_notice"}
    assert [e["event_type"] for e in events_of(setup, incident)].count("ShipmentDelayed") == 1
