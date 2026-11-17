"""E11: entrega retrasada y planes revisados (spec experiments, «E11 Entrega retrasada y planes revisados (H11)»;
spec synthetic-data, «Inyección de entrega retrasada por acción»).

50 incidencias con un 50 % de fallos en `reserve_stock` y resultados retrasados en `reserve_stock`,
`reissue_shipment` y `file_claim`, con un plazo de `ExecuteStep` corto para que también haya resultados tras el
plazo. El orquestador descarta los resultados obsoletos sin registrarlos, así que se cuentan desde el log:

- obsoleto por versión: un `StepCompleted` o `StepFailed` cuya `plan_version` es anterior a la vigente cuando el
  orquestador lo consume (la de los `PlanCreated` y `PlanRevised` anteriores de su incidencia; todos van por la
  misma partición, así que el orden del log es el de consumo);
- tardío tras el plazo: el resultado de un paso cuyo plazo venció con el paso pedido (`Harness.timed_out`).

Un obsoleto no debe cambiar el estado de la saga: ningún evento del orquestador puede tener como causa un
resultado obsoleto, salvo la excepción del supuesto A4: el `StepCompleted` tardío de un `reserve_stock` cancelado
se marca completado y pide su `release_stock`, una sola vez.
"""

from __future__ import annotations

import statistics
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.harness import RunConfig, started
from eda_retail.experiments.report import Report
from eda_retail.orchestrator.rules import COMPENSATION_SUFFIX
from eda_retail.synthetic.faults import FaultPlan

FAIL_RESERVE = 0.5
STEP_DELAYS = {"reserve_stock": 1.0, "reissue_shipment": 2.0, "file_claim": 6.0}  # base; jitter en [0,5, 1,5)
STEP_TIMEOUT_S = 4.0  # plazo de ExecuteStep: parte de los file_claim llegan después
ORCHESTRATOR = "orchestrator"
RESULTS = ("StepCompleted", "StepFailed")


def classify(events: Iterable[Mapping[str, Any]], timed_out: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Resultados obsoletos y lo que el orquestador hizo por ellos.

    `events`: el log en orden de `seq` (`event_id`, `event_type`, `correlation_id`, `causation_id`, `producer`,
    `payload`); `timed_out`: `incident_id`, `step_id` y `plan_version` de los pasos cuyo plazo venció."""
    events = list(events)
    expired = {(t["incident_id"], t["step_id"], t["plan_version"]) for t in timed_out}
    version: dict[str, int] = {}
    action: dict[tuple[str, str], str] = {}
    stale: dict[Any, dict[str, Any]] = {}
    for e in events:
        incident, p = e["correlation_id"], e["payload"]
        if e["event_type"] in ("PlanCreated", "PlanRevised"):
            version[incident] = max(version.get(incident, 0), p["plan_version"])
        elif e["event_type"] == "StepRequested":
            action[(incident, p["step_id"])] = p["action"]
        elif e["event_type"] in RESULTS and e["producer"] != ORCHESTRATOR:
            # Una compensación aún pedida no es obsoleta por la versión (spec incident-planning, «Eventos obsoletos»).
            old = p["plan_version"] < version.get(incident, 0) and not p["step_id"].endswith(COMPENSATION_SUFFIX)
            late = (incident, p["step_id"], p["plan_version"]) in expired
            if old or late:
                stale[e["event_id"]] = {"incident_id": incident, "step_id": p["step_id"], "event_type": e["event_type"],
                                        "plan_version": p["plan_version"], "by_version": old, "after_deadline": late,
                                        "action": action.get((incident, p["step_id"]))}
    allowed, changes = [], []
    for e in events:
        cause = stale.get(e["causation_id"])
        if cause is None or e["producer"] != ORCHESTRATOR:
            continue
        a4 = (cause["event_type"] == "StepCompleted" and cause["action"] == "reserve_stock"
              and e["event_type"] == "StepRequested"
              and e["payload"]["step_id"] == cause["step_id"] + COMPENSATION_SUFFIX)
        (allowed if a4 else changes).append({"incident_id": cause["incident_id"], "cause": cause["step_id"],
                                             "event_type": e["event_type"]})
    comp_requested = Counter((e["correlation_id"], e["payload"]["step_id"]) for e in events
                             if e["event_type"] == "StepRequested"
                             and e["payload"]["step_id"].endswith(COMPENSATION_SUFFIX))
    comp_completed = {(e["correlation_id"], e["payload"]["step_id"]) for e in events
                      if e["event_type"] == "StepCompleted" and e["payload"]["step_id"].endswith(COMPENSATION_SUFFIX)}
    values = list(stale.values())
    return {
        "stale_results": {
            "total": len(values), "by_version": sum(s["by_version"] for s in values),
            "after_deadline": sum(s["after_deadline"] for s in values),
            "completed": sum(s["event_type"] == "StepCompleted" for s in values),
            "failed": sum(s["event_type"] == "StepFailed" for s in values),
        },
        "a4_compensations": len(allowed),
        "stale_state_changes": len(changes), "stale_state_change_detail": changes[:20],
        "compensations": {"requested": len(comp_requested), "completed": len(comp_completed & set(comp_requested)),
                          "requested_more_than_once": sum(1 for n in comp_requested.values() if n > 1)},
    }


def revisions(events: Iterable[Mapping[str, Any]], incidents: int) -> dict[str, Any]:
    """`PlanRevised` por incidencia."""
    per = Counter(e["correlation_id"] for e in events if e["event_type"] == "PlanRevised")
    counts = list(per.values()) + [0] * max(0, incidents - len(per))
    return {"total": sum(per.values()), "incidents_with_revisions": len(per),
            "mean": statistics.fmean(counts) if counts else 0.0, "max": max(counts, default=0)}


def h11(metrics: Mapping[str, Any]) -> bool:
    """I1 a I5 sin violaciones, ningún obsoleto cambió el estado de la saga fuera de A4 y cada compensación se pidió
    una sola vez."""
    return (all(inv["violations"] == 0 for inv in metrics["invariants"].values())
            and metrics["stale_state_changes"] == 0 and metrics["compensations"]["requested_more_than_once"] == 0)


def run_e11(base: Settings, *, seed: int = 42, incidents: int = 50) -> Report:
    faults = FaultPlan(seed=seed, fail_probability={"reserve_stock": FAIL_RESERVE}, step_delay_seconds=STEP_DELAYS)
    config = RunConfig(seed=seed, incidents=incidents, faults=faults, step_timeout_seconds=STEP_TIMEOUT_S,
                       track_timeouts=True)
    with started(base, config) as h:
        result = h.result(h.elapsed)
        events = [dict(r) for r in h.conn.execute(
            "SELECT event_id, event_type, correlation_id, causation_id, producer, payload FROM event_log.events"
            " ORDER BY seq").fetchall()]
        h.conn.commit()
        timed_out, delayed = list(h.timed_out), h.delayed_results
        checked = invariants.check(h)
    metrics = {
        "closed": result.closed, "open": result.open, "resolved": result.resolved, "escalated": result.escalated,
        "escalation_reasons": result.escalation_reasons, "delayed_results": delayed, "timeouts": len(timed_out),
        "plan_revised": revisions(events, incidents),
        **classify(events, timed_out),
        "invariants": checked,
    }
    return Report(
        experiment="E11", seed=seed, model=result.model,
        config={"incidents": incidents, "fail_probability": {"reserve_stock": FAIL_RESERVE},
                "step_delay_seconds": STEP_DELAYS, "step_timeout_s": STEP_TIMEOUT_S},
        metrics=metrics, hypothesis={"H11": h11(metrics) and invariants.gate(checked)},
    )
