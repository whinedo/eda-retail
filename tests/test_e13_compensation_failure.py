"""E13 Fallo de compensación (spec experiments): métricas del log y H13.

Sin Kafka ni PostgreSQL: el log se construye a mano. La ejecución de punta a punta está en
tests/integration/test_e13_compensation_failure.py.
"""

from __future__ import annotations

from eda_retail.experiments.e13_compensation_failure import h13, summarize


class Log:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def add(self, incident: str, event_type: str, producer: str = "orchestrator", **payload) -> None:
        self.events.append({"event_type": event_type, "correlation_id": incident, "producer": producer,
                            "payload": payload})

    def compensation(self, incident: str, *, failed: bool) -> None:
        """Reserva completada, reenvío fallido, revisión con reembolso y su compensación."""
        self.add(incident, "StepRequested", step_id="S3", action="reserve_stock", plan_version=1)
        self.add(incident, "StepCompleted", "inventory", step_id="S3", plan_version=1)
        self.add(incident, "StepRequested", step_id="S4", action="reissue_shipment", plan_version=1)
        self.add(incident, "StepFailed", "executor-reissue_shipment", step_id="S4", plan_version=1,
                 error="injected_failure", retryable=False, attempts=1)
        self.add(incident, "PlanRevised", "replanner", plan_version=2)
        self.add(incident, "StepRequested", step_id="S3-comp", action="release_stock", plan_version=2)
        if failed:
            self.add(incident, "StepFailed", "inventory", step_id="S3-comp", plan_version=2,
                     error="injected_failure", retryable=False, attempts=1)
            self.add(incident, "IncidentEscalated", reason="compensation_failed")
        else:
            self.add(incident, "StepCompleted", "inventory", step_id="S3-comp", plan_version=2)
            self.add(incident, "StepRequested", step_id="S5", action="issue_refund", plan_version=2)
            self.add(incident, "StepCompleted", "executor-issue_refund", step_id="S5", plan_version=2)
            self.add(incident, "IncidentResolved", outcome="refunded")


def _clean_invariants() -> dict:
    return {f"I{i}": {"violations": 0} for i in range(1, 6)}


def test_stock_release_fails():
    """La liberación del stock falla: la saga escala con compensation_failed y no pide ningún issue_refund"""
    log = Log()
    log.compensation("I-1", failed=True)
    m = summarize(log.events)
    assert m["compensations"] == {"requested": 1, "completed": 0, "failed": 1}
    assert m["failed_incidents"] == {"total": 1, "escalated_compensation_failed": 1, "refunds_requested": 0,
                                     "compensations_after_escalation": 0, "unexpected": []}


def test_refund_requested_after_a_failed_compensation_is_reported():
    """La liberación del stock falla: un issue_refund pedido en esa incidencia se cuenta y la incidencia se señala"""
    log = Log()
    log.compensation("I-1", failed=True)
    log.add("I-1", "StepRequested", step_id="S5", action="issue_refund", plan_version=2)
    m = summarize(log.events)
    assert m["failed_incidents"]["refunds_requested"] == 1
    assert m["failed_incidents"]["unexpected"] == ["I-1"]
    assert not h13({**m, "invariants": _clean_invariants()})


def test_failed_compensation_not_escalated_is_reported():
    """La liberación del stock falla: una incidencia sin IncidentEscalated compensation_failed se señala"""
    log = Log()
    log.compensation("I-1", failed=True)
    log.events.pop()  # sin escalado
    log.add("I-1", "IncidentEscalated", reason="max_revisions")
    m = summarize(log.events)
    assert m["failed_incidents"]["escalated_compensation_failed"] == 0
    assert m["failed_incidents"]["unexpected"] == ["I-1"]
    assert not h13({**m, "invariants": _clean_invariants()})


def test_no_compensations_after_the_escalation():
    """Sin compensaciones tras el escalado: una compensación pedida después de compensation_failed se cuenta"""
    log = Log()
    log.compensation("I-1", failed=True)
    log.add("I-1", "StepRequested", step_id="S6-comp", action="release_stock", plan_version=2)
    m = summarize(log.events)
    assert m["failed_incidents"]["compensations_after_escalation"] == 1
    assert m["failed_incidents"]["unexpected"] == ["I-1"]
    assert not h13({**m, "invariants": _clean_invariants()})


def test_compensation_requested_before_the_escalation_does_not_count_as_after():
    """Sin compensaciones tras el escalado: las pedidas antes del escalado no cuentan"""
    log = Log()
    log.compensation("I-1", failed=True)
    m = summarize(log.events)
    assert m["failed_incidents"]["compensations_after_escalation"] == 0


def test_compensation_completes():
    """La compensación se completa: se pide el issue_refund retenido y la incidencia termina reembolsada"""
    log = Log()
    log.compensation("I-1", failed=False)
    m = summarize(log.events)
    assert m["compensations"] == {"requested": 1, "completed": 1, "failed": 0}
    assert m["completed_incidents"] == {"total": 1, "refunded": 1}
    assert m["failed_incidents"]["total"] == 0


def test_h13_holds():
    """Cumplimiento de H13: compensaciones fallidas escaladas sin reembolso ni más compensaciones, e I1 a I5 limpios"""
    log = Log()
    log.compensation("I-1", failed=True)
    log.compensation("I-2", failed=False)
    m = summarize(log.events)
    assert h13({**m, "invariants": _clean_invariants()})
    assert not h13({**m, "invariants": _clean_invariants() | {"I3": {"violations": 1}}})


def test_h13_needs_a_failed_compensation():
    """Cumplimiento de H13: sin ninguna compensación fallida el experimento no cuenta como cumplido"""
    log = Log()
    log.compensation("I-1", failed=False)
    m = summarize(log.events)
    assert not h13({**m, "invariants": _clean_invariants()})


def test_expexecutor_checks_of_e13():
    from eda_retail.expexecutor.catalog import CATALOG

    log = Log()
    log.compensation("I-1", failed=True)
    ok = {"metrics": {**summarize(log.events), "invariants": _clean_invariants()}}
    assert all(c.ok for c in CATALOG["e13"].checks(ok))
    bad = {"metrics": {**ok["metrics"], "invariants": _clean_invariants() | {"I4": {"violations": 2}}}}
    assert [c.metric for c in CATALOG["e13"].checks(bad) if not c.ok] == ["metrics.invariants (I1 a I5)"]
    none = {"metrics": {**ok["metrics"], "compensations": {"requested": 1, "completed": 1, "failed": 0}}}
    assert "metrics.compensations.failed" in [c.metric for c in CATALOG["e13"].checks(none) if not c.ok]


def test_cli_runs_e13():
    from eda_retail.experiments.cli import DEFAULT_INCIDENTS, EXPERIMENTS

    assert EXPERIMENTS["e13"].__name__ == "run_e13"
    assert DEFAULT_INCIDENTS["e13"] == 50
