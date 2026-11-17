"""E11 Entrega retrasada y planes revisados (spec experiments): clasificación de resultados obsoletos y H11.

Sin Kafka ni PostgreSQL: el log se construye a mano. La ejecución de punta a punta está en
tests/integration/test_e11_late_results.py.
"""

from __future__ import annotations

import uuid

from eda_retail.experiments.e11_late_results import classify, h11, revisions


class Log:
    def __init__(self, incident: str = "I-1"):
        self.incident = incident
        self.events: list[dict] = []

    def add(self, event_type: str, producer: str, cause=None, **payload) -> uuid.UUID:
        event_id = uuid.uuid4()
        self.events.append({"event_id": event_id, "event_type": event_type, "correlation_id": self.incident,
                            "causation_id": cause, "producer": producer, "payload": payload})
        return event_id

    def plan(self, version: int, kind: str = "PlanCreated") -> uuid.UUID:
        return self.add(kind, "planner" if kind == "PlanCreated" else "replanner", plan_version=version)

    def requested(self, step_id: str, action: str, version: int, cause=None) -> uuid.UUID:
        return self.add("StepRequested", "orchestrator", cause, step_id=step_id, action=action, plan_version=version)


def _clean_invariants() -> dict:
    return {f"I{i}": {"violations": 0} for i in range(1, 6)}


def test_result_of_an_old_version_after_a_revision():
    """Resultado de una versión antigua tras una revisión: el orquestador lo descarta sin cambiar el estado"""
    log = Log()
    log.plan(1)
    log.requested("S2", "file_claim", 1)
    log.requested("S3", "reserve_stock", 1)
    log.add("StepFailed", "inventory", step_id="S3", plan_version=1, error="injected_failure")
    log.plan(2, "PlanRevised")
    log.add("StepCompleted", "executor-file_claim", step_id="S2", plan_version=1)  # llega tarde
    m = classify(log.events, [])
    assert m["stale_results"] == {"total": 1, "by_version": 1, "after_deadline": 0, "completed": 1, "failed": 0}
    assert m["stale_state_changes"] == 0


def test_result_of_a_compensation_requested_before_a_revision_is_not_stale():
    """Compensación pedida antes de una revisión: su resultado de la versión anterior no es obsoleto y lo que el
    orquestador hace con él no cuenta como cambio por un obsoleto"""
    log = Log()
    log.plan(1)
    log.requested("S3", "reserve_stock", 1)
    log.requested("S3-comp", "release_stock", 1)
    log.plan(2, "PlanRevised")
    done = log.add("StepCompleted", "inventory", step_id="S3-comp", plan_version=1)
    log.requested("S5", "issue_refund", 2, cause=done)
    m = classify(log.events, [])
    assert m["stale_results"]["total"] == 0
    assert m["stale_state_changes"] == 0
    assert m["compensations"] == {"requested": 1, "completed": 1, "requested_more_than_once": 0}


def test_stale_result_that_changes_the_saga_is_counted():
    log = Log()
    log.plan(1)
    log.requested("S2", "file_claim", 1)
    log.plan(2, "PlanRevised")
    stale = log.add("StepCompleted", "executor-file_claim", step_id="S2", plan_version=1)
    log.requested("S4", "reissue_shipment", 2, cause=stale)  # el orquestador avanzó por un obsoleto
    m = classify(log.events, [])
    assert m["stale_state_changes"] == 1
    assert not h11({**m, "invariants": _clean_invariants()})


def test_result_after_the_deadline():
    """Resultado después del plazo: la saga ya trató el paso como timeout y descarta el resultado; la compensación
    de la reserva se pide una sola vez"""
    log = Log()
    log.plan(1)
    log.requested("S3", "reserve_stock", 1)
    log.requested("S3-comp", "release_stock", 1)  # pedida al vencer el plazo
    log.add("StepCompleted", "inventory", step_id="S3", plan_version=1)  # tardío
    log.add("StepCompleted", "inventory", step_id="S3-comp", plan_version=1)
    m = classify(log.events, [{"incident_id": "I-1", "step_id": "S3", "plan_version": 1}])
    assert m["stale_results"]["after_deadline"] == 1 and m["stale_results"]["by_version"] == 0
    assert m["compensations"] == {"requested": 1, "completed": 1, "requested_more_than_once": 0}
    assert h11({**m, "invariants": _clean_invariants()})


def test_cancelled_reservation_completed_late_assumption_a4():
    """Reserva cancelada completada tarde (supuesto A4): pide su release_stock una sola vez y no es un cambio indebido"""
    log = Log()
    log.plan(1)
    log.requested("S3", "reserve_stock", 1)
    log.plan(2, "PlanRevised")
    late = log.add("StepCompleted", "inventory", step_id="S3", plan_version=1)
    log.requested("S3-comp", "release_stock", 2, cause=late)
    m = classify(log.events, [])
    assert m["a4_compensations"] == 1 and m["stale_state_changes"] == 0
    assert h11({**m, "invariants": _clean_invariants()})


def test_compensation_requested_twice_fails_h11():
    log = Log()
    log.plan(1)
    log.requested("S3-comp", "release_stock", 1)
    log.requested("S3-comp", "release_stock", 2)
    m = classify(log.events, [])
    assert m["compensations"]["requested_more_than_once"] == 1
    assert not h11({**m, "invariants": _clean_invariants()})


def test_h11_holds():
    """Cumplimiento de H11: I1 a I5 sin violaciones y ningún obsoleto cambió el estado de la saga fuera de A4"""
    log = Log()
    log.plan(1)
    m = classify(log.events, [])
    assert h11({**m, "invariants": _clean_invariants()})
    assert not h11({**m, "invariants": _clean_invariants() | {"I3": {"violations": 1}}})


def test_plan_revised_per_incident():
    events = [{"event_type": "PlanRevised", "correlation_id": i} for i in ("I-1", "I-1", "I-2")]
    assert revisions(events, 4) == {"total": 3, "incidents_with_revisions": 2, "mean": 0.75, "max": 2}


def test_expexecutor_checks_of_e11():
    from eda_retail.expexecutor.catalog import CATALOG

    ok = {"metrics": {"invariants": _clean_invariants(), "stale_state_changes": 0,
                      "compensations": {"requested_more_than_once": 0}}}
    assert all(c.ok for c in CATALOG["e11"].checks(ok))
    bad = {"metrics": {**ok["metrics"], "invariants": _clean_invariants() | {"I4": {"violations": 2}}}}
    assert [c.metric for c in CATALOG["e11"].checks(bad) if not c.ok] == ["metrics.invariants (I1 a I5)"]
