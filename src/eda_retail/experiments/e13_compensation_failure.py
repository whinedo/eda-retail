"""E13: fallo de compensación (spec experiments, «E13 Fallo de compensación (H13)»; spec incident-planning,
«Fallo de una compensación» y «Escalado de la incidencia»).

50 incidencias con `fail_probability` 1,0 en `reissue_shipment` y 0,5 en `release_stock`. Cada reenvío falla, el
replanner lo sustituye por `issue_refund` y la saga deriva el `release_stock` `-comp` de la reserva completada,
que retiene el reembolso. La mitad de esas compensaciones falla (`StepFailed` no reintentable, `attempts` 1).

La saga no reintenta pasos: el ejecutor publica `StepFailed` cuando agota sus reintentos (`STEP_MAX_ATTEMPTS`
para un error reintentable), así que todo `StepFailed` de una compensación es definitivo y escala con
`compensation_failed` (`rules.on_failure`). Las métricas salen del log: por incidencia con una compensación
fallida, si escaló con `compensation_failed`, si se pidió algún `issue_refund` y si se pidió alguna compensación
después del escalado; las compensaciones completadas son la rama de control y deben terminar reembolsadas.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.harness import RunConfig, started
from eda_retail.experiments.report import Report
from eda_retail.orchestrator.rules import COMPENSATION_FAILED, COMPENSATION_SUFFIX
from eda_retail.synthetic.faults import FaultPlan

FAIL_PROBABILITY = {"reissue_shipment": 1.0, "release_stock": 0.5}


def _is_compensation(event: Mapping[str, Any]) -> bool:
    return str(event["payload"].get("step_id", "")).endswith(COMPENSATION_SUFFIX)


def summarize(events: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Compensaciones y lo que la saga hizo con cada incidencia. `events`: el log en orden de `seq`
    (`event_type`, `correlation_id`, `producer`, `payload`)."""
    requested: set[tuple[str, str]] = set()
    completed: set[tuple[str, str]] = set()
    failed: set[tuple[str, str]] = set()
    escalated: set[str] = set()  # con compensation_failed
    refunds: dict[str, int] = {}
    after: dict[str, int] = {}  # compensaciones pedidas tras el escalado por compensation_failed
    outcome: dict[str, str] = {}
    for e in events:
        incident, kind, p = e["correlation_id"], e["event_type"], e["payload"]
        if kind == "StepRequested" and _is_compensation(e):
            requested.add((incident, p["step_id"]))
            if incident in escalated:
                after[incident] = after.get(incident, 0) + 1
        elif kind == "StepRequested" and p.get("action") == "issue_refund":
            refunds[incident] = refunds.get(incident, 0) + 1
        elif kind == "StepCompleted" and _is_compensation(e):
            completed.add((incident, p["step_id"]))
        elif kind == "StepFailed" and _is_compensation(e):
            failed.add((incident, p["step_id"]))
        elif kind == "IncidentEscalated" and p.get("reason") == COMPENSATION_FAILED:
            escalated.add(incident)
        elif kind == "IncidentResolved":
            outcome[incident] = p.get("outcome")
    with_failure = sorted({i for i, _ in failed})
    with_completion = sorted({i for i, _ in completed} - set(with_failure))
    unexpected = [i for i in with_failure if i not in escalated or refunds.get(i, 0) or after.get(i, 0)]
    return {
        "compensations": {"requested": len(requested), "completed": len(completed), "failed": len(failed)},
        "failed_incidents": {
            "total": len(with_failure),
            "escalated_compensation_failed": sum(i in escalated for i in with_failure),
            "refunds_requested": sum(refunds.get(i, 0) for i in with_failure),
            "compensations_after_escalation": sum(after.get(i, 0) for i in with_failure),
            "unexpected": unexpected[:20],
        },
        "completed_incidents": {"total": len(with_completion),
                                "refunded": sum(outcome.get(i) == "refunded" for i in with_completion)},
    }


def h13(metrics: Mapping[str, Any]) -> bool:
    """Al menos una compensación fallida; cada incidencia con una fallida escaló con `compensation_failed`, sin
    `issue_refund` pedido ni compensaciones tras el escalado; e I1 a I5 sin violaciones."""
    f = metrics["failed_incidents"]
    return (metrics["compensations"]["failed"] > 0 and f["total"] > 0
            and f["escalated_compensation_failed"] == f["total"]
            and f["refunds_requested"] == 0 and f["compensations_after_escalation"] == 0
            and all(inv["violations"] == 0 for inv in metrics["invariants"].values()))


def run_e13(base: Settings, *, seed: int = 42, incidents: int = 50) -> Report:
    faults = FaultPlan(seed=seed, fail_probability=FAIL_PROBABILITY)
    config = RunConfig(seed=seed, incidents=incidents, faults=faults)
    with started(base, config) as h:
        result = h.result(h.elapsed)
        events = [dict(r) for r in h.conn.execute(
            "SELECT event_type, correlation_id, producer, payload FROM event_log.events ORDER BY seq").fetchall()]
        h.conn.commit()
        checked = invariants.check(h)
    metrics = {
        "closed": result.closed, "open": result.open, "resolved": result.resolved, "escalated": result.escalated,
        "escalation_reasons": result.escalation_reasons,
        **summarize(events),
        "invariants": checked,
    }
    return Report(
        experiment="E13", seed=seed, model=result.model,
        config={"incidents": incidents, "fail_probability": FAIL_PROBABILITY},
        metrics=metrics, hypothesis={"H13": h13(metrics) and invariants.gate(checked)},
    )
