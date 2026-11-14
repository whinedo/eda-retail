"""E7 Auditabilidad (hipótesis H7, spec experiments).

Ejecuta las incidencias con un 50 % de fallos en `reserve_stock`, para que haya planes
revisados (supuesto A7), y elige al azar, con la semilla, las incidencias a explicar. Cada
decisión es un `StepRequested` de `audit_trail`; su explicación sale solo del log y de la
traza (la cadena de `causation_id`):

- agente: el `actor` del `PlanCreated` o `PlanRevised` con la misma versión del plan;
- versión del plan: la del `StepRequested`;
- motivo: el `reason` del `PlanRevised`, o, en un plan creado, la causa y la gravedad del
  `ShipmentDelayed` del que procede.

H7: todas las decisiones de las incidencias elegidas tienen agente, versión y motivo.
"""

from __future__ import annotations

import random

from eda_retail.config import Settings
from eda_retail.experiments.harness import Harness, RunConfig, started
from eda_retail.experiments.report import Report
from eda_retail.queries.audit_trail import get_audit_trail
from eda_retail.synthetic.faults import FaultPlan

FAILING = {"reserve_stock": 0.5}
_PLANS = ("PlanCreated", "PlanRevised")


def _origin(events: dict[str, dict], event: dict) -> str | None:
    """Sube por `causation_id` hasta el `ShipmentDelayed` y devuelve su causa y gravedad."""
    seen = set()
    while event is not None and event["event_id"] not in seen:
        if event["event_type"] == "ShipmentDelayed":
            return f"{event['payload']['cause']}/{event['payload']['severity']}"
        seen.add(event["event_id"])
        event = events.get(str(event["causation_id"]))
    return None


def explain(h: Harness, incident_id: str) -> list[dict]:
    trail = get_audit_trail(h.conn, incident_id)
    rows = h.conn.execute(
        "SELECT seq, event_id, event_type, causation_id, payload FROM events WHERE correlation_id = %s",
        (incident_id,),
    ).fetchall()
    h.conn.commit()
    by_id = {str(r["event_id"]): r for r in rows}
    by_seq = {r["seq"]: r for r in rows}
    plans = {}
    for e in trail.entries:
        if e.decision in _PLANS:
            reason = e.reason if e.decision == "PlanRevised" else _origin(by_id, by_seq[e.seq])
            plans[e.plan_version] = {"agent": e.actor, "reason": reason}
    decisions = []
    for e in trail.entries:
        if e.decision.startswith("StepRequested:"):
            plan = plans.get(e.plan_version, {})
            decisions.append({"seq": e.seq, "action": e.decision.split(":", 1)[1], "plan_version": e.plan_version,
                              "agent": plan.get("agent"), "reason": plan.get("reason")})
    return decisions


def run_e7(base: Settings, *, seed: int = 42, incidents: int = 50, sample: int = 10) -> Report:
    config = RunConfig(seed=seed, incidents=incidents, faults=FaultPlan(seed=seed, fail_probability=FAILING))
    with started(base, config) as h:
        model = h.result(h.elapsed).model
        ids = sorted(r["incident_id"] for r in h.conn.execute("SELECT incident_id FROM view_incident_status"))
        h.conn.commit()
        chosen = random.Random(seed).sample(ids, min(sample, len(ids)))
        explained = [{"incident_id": i, "decisions": explain(h, i)} for i in chosen]
    decisions = [d for inc in explained for d in inc["decisions"]]
    complete = [d for d in decisions if d["agent"] and d["plan_version"] and d["reason"]]
    revised = {(inc["incident_id"], d["plan_version"]) for inc in explained for d in inc["decisions"]
               if d["plan_version"] > 1}
    h7 = bool(explained) and all(inc["decisions"] for inc in explained) and len(complete) == len(decisions)
    return Report(
        experiment="E7", seed=seed, model=model,
        config={"incidents": incidents, "sample": sample, "fail_probability": FAILING},
        metrics={"decisions": len(decisions), "complete": len(complete), "revised_plans": len(revised),
                 "incidents": explained},
        hypothesis={"H7": h7},
    )
