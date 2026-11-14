"""Orquestador: del evento al comando, sin estado propio (supuesto A1)."""

from __future__ import annotations

from eda_retail.infra.consumer import Outcome
from eda_retail.orchestrator.runner import Orchestrator

from .command_helpers import events_of, handler, open_incident
from .test_plans import LOSS_PLAN, create, plan_steps, revise, step
from .test_steps import record


def orchestrate(conn, event, orchestrator: Orchestrator | None = None) -> list:
    """Pasa el evento al orquestador y devuelve los eventos que produjeron sus comandos."""
    o = orchestrator or Orchestrator(handler())
    assert o.process(conn, event) == Outcome.PROCESSED
    return [e for r in o.results for e in r.events]


def requested(events) -> list[str]:
    return [e.payload["step_id"] for e in events if e.event_type == "StepRequested"]


def ok(result):
    assert result.accepted, result.reason
    return result.events[0]


def started(conn):
    """LOSS_PLAN con S1, S2 y S3 pedidos por el orquestador; devuelve la incidencia."""
    incident, _ = open_incident(conn)
    plan = ok(create(conn, incident, LOSS_PLAN))
    assert requested(orchestrate(conn, plan)) == ["S1", "S2", "S3"]
    return incident


def finish_first_three(conn, incident):
    """Completa S1, S2 y S3 (orquestando cada resultado) y devuelve el reissue pedido."""
    out = []
    for sid, result, effect in (
        ("S1", {"message_id": "M1"}, {"order_id": "P1", "message_id": "M1"}),
        ("S2", {"claim_id": "C1"}, {"order_id": "P1", "claim_id": "C1"}),
        ("S3", {"reservation_id": "R1"}, {"order_id": "P1", "warehouse": "W1", "reservation_id": "R1"}),
    ):
        out += orchestrate(conn, ok(record(conn, incident, sid, result=result, effect=effect)))
    assert requested(out) == ["S4"]


def reissue_failed_and_revised(conn, incident):
    finish_first_three(conn, incident)
    failed = ok(record(conn, incident, "S4", outcome="failed", error="carrier_rejected", retryable=False))
    assert orchestrate(conn, failed) == []  # no reintentable: lo replanifica el replanner
    revised = ok(revise(conn, incident, [step("S5", "issue_refund", order_id="P1", amount="30")],
                        reason="carrier_rejected"))
    return orchestrate(conn, revised)


def test_independent_steps_requested_together(conn):
    """Pasos sin dependencias"""
    started(conn)


def test_dependent_step_waits(conn):
    """Paso con dependencia"""
    incident = started(conn)
    claim = ok(record(conn, incident, "S2", result={"claim_id": "C1"}, effect={"order_id": "P1", "claim_id": "C1"}))
    assert requested(orchestrate(conn, claim)) == []
    reserved = ok(record(conn, incident, "S3", result={"reservation_id": "R1"},
                         effect={"order_id": "P1", "warehouse": "W1", "reservation_id": "R1"}))
    assert requested(orchestrate(conn, reserved)) == ["S4"]


def test_reissue_failure_after_reserving_stock(conn):
    """Fallo del reenvío tras reservar stock"""
    incident = started(conn)
    events = reissue_failed_and_revised(conn, incident)
    assert [(e.event_type, e.payload["step_id"]) for e in events] == [
        ("StepCancelled", "S4"), ("StepRequested", "S3-comp"),
    ]
    [comp] = [e for e in events if e.event_type == "StepRequested"]
    assert comp.payload["action"] == "release_stock"
    assert comp.producer == "orchestrator"


def test_refund_after_compensation(conn):
    """Reembolso con una compensación pendiente"""
    incident = started(conn)
    assert "S5" not in requested(reissue_failed_and_revised(conn, incident))
    released = ok(record(conn, incident, "S3-comp", result={"released": True}, effect={"reservation_id": "R1"}))
    assert requested(orchestrate(conn, released)) == ["S5"]


def test_stock_release_fails(conn):
    """La liberación del stock falla"""
    incident = started(conn)
    reissue_failed_and_revised(conn, incident)
    failed = ok(record(conn, incident, "S3-comp", outcome="failed", error="timeout", retryable=True, attempts=3))
    [ev] = orchestrate(conn, failed)
    assert (ev.event_type, ev.payload["reason"]) == ("IncidentEscalated", "compensation_failed")
    assert "RefundIssued" not in [e["event_type"] for e in events_of(conn, incident)]
    assert plan_steps(conn, incident)["S5"]["status"] == "pending"


def test_retries_exhausted_escalates(conn):
    """Un paso que agota sus reintentos escala la incidencia con retries_exhausted"""
    incident = started(conn)
    failed = ok(record(conn, incident, "S2", outcome="failed", error="timeout", retryable=True, attempts=3))
    [ev] = orchestrate(conn, failed)
    assert (ev.event_type, ev.payload["reason"]) == ("IncidentEscalated", "retries_exhausted")


def test_plan_completed(conn):
    """Plan completado"""
    incident = started(conn)
    finish_first_three(conn, incident)
    shipped = ok(record(conn, incident, "S4", result={"new_shipment_id": "SH1"},
                        effect={"order_id": "P1", "new_shipment_id": "SH1"}))
    [ev] = orchestrate(conn, shipped)
    assert (ev.event_type, ev.payload["outcome"]) == ("IncidentResolved", "resolved")


def test_result_of_an_old_version(conn, spans):
    """Resultado de una versión antigua"""
    incident = started(conn)
    ok(record(conn, incident, "S3", outcome="failed", error="out_of_stock", retryable=False))
    revised = ok(revise(conn, incident, [step("S2", "file_claim", order_id="P1")]))
    orchestrate(conn, revised)
    stale = ok(record(conn, incident, "S2", outcome="failed", error="timeout", retryable=True, attempts=3,
                      plan_version=1))
    spans.clear()
    assert orchestrate(conn, stale) == []  # no lo usa para decidir
    assert [s.attributes.get("eda.stale") for s in spans.get_finished_spans()
            if s.name == "orchestrator StepFailed"] == [True]
    assert "IncidentEscalated" not in [e["event_type"] for e in events_of(conn, incident)]


def test_duplicate_event_is_discarded(conn):
    """Un evento ya procesado por el orquestador se descarta sin comandos"""
    incident, _ = open_incident(conn)
    plan = ok(create(conn, incident, LOSS_PLAN))
    o = Orchestrator(handler())
    orchestrate(conn, plan, o)
    assert o.process(conn, plan) == Outcome.DUPLICATE


def test_orchestrator_restart(conn):
    """Reinicio del orquestador"""
    incident, _ = open_incident(conn)
    plan = ok(create(conn, incident, LOSS_PLAN))
    orchestrate(conn, plan, Orchestrator(handler()))
    reserved = ok(record(conn, incident, "S3", result={"reservation_id": "R1"},
                         effect={"order_id": "P1", "warehouse": "W1", "reservation_id": "R1"}))
    # Una instancia nueva, sin memoria de la anterior, recupera la versión y los pasos pendientes.
    assert requested(orchestrate(conn, reserved, Orchestrator(handler()))) == ["S4"]


def test_conflict_with_a_parallel_writer_is_retried(conn):
    """Si otro escritor cambia la versión a la vez, el orquestador vuelve a decidir con la nueva"""
    incident, _ = open_incident(conn)
    plan = ok(create(conn, incident, LOSS_PLAN))

    class StaleOnce(type(handler())):
        calls = 0

        def current_version(self, c, incident_id):
            StaleOnce.calls += 1
            version = super().current_version(c, incident_id)
            return version - 1 if StaleOnce.calls == 1 else version

    o = Orchestrator(StaleOnce(handler().rules))
    assert requested(orchestrate(conn, plan, o)) == ["S1", "S2", "S3"]
    assert StaleOnce.calls == 2
