"""E15: degradación del LLM (spec experiments, «E15 Degradación del LLM (H15)»; spec synthetic-data, «Degradación del
LLM»; spec llm-boundaries).

50 incidencias en proceso, con un 30 % de fallos en `reissue_shipment` para que el replanner también trabaje, y cuatro
variantes del cliente oráculo del planner y del replanner (`DegradedLlm`):

- `baseline`: sin degradación.
- `invalid`: un 60 % de las respuestas no son JSON o les faltan campos; `generate` reintenta hasta
  `LLM_MAX_GENERATIONS` y, si las agota, el planner escala con `llm_output_invalid`.
- `slow`: 0,5 s por llamada y un plazo de `CreatePlan` y `RevisePlan` de 4 s; la cola del planner supera el plazo, la
  saga escala con `plan_timeout` y los planes que llegan después no deben pedir pasos.
- `error`: un 30 % de las llamadas lanzan una excepción; el worker reintenta el trabajo con backoff.

Las métricas salen del log (`event_log.events`, en orden de `seq`) y de los `command_jobs` del planner y del replanner.
H15: en todas las variantes, ninguna incidencia abierta, ningún evento de plan cuya última llamada al LLM (misma
causa) no pasó la validación, ningún paso pedido tras el primer escalado de su incidencia (salvo las compensaciones
`-comp`), I1 a I5 sin violaciones, y cada variante con degradación provoca su efecto.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.report import Report, summary
from eda_retail.orchestrator.rules import COMPENSATION_SUFFIX
from eda_retail.orchestrator.saga import PLAN_TIMEOUT
from eda_retail.synthetic.faults import FaultPlan
from eda_retail.synthetic.generator import generate
from eda_retail.synthetic.llm_faults import DegradedLlm

FAIL_PROBABILITY = {"reissue_shipment": 0.3}
PLAN_TIMEOUT_S = 4.0  # plazo de CreatePlan y RevisePlan en `slow`
VARIANTS: dict[str, dict[str, Any] | None] = {
    "baseline": None,
    "invalid": {"mode": "invalid", "probability": 0.6},
    "slow": {"mode": "slow", "delay_seconds": 0.5},
    "error": {"mode": "error", "probability": 0.3},
}
PLAN_EVENTS = ("PlanCreated", "PlanRevised")
LLM_COMPONENTS = ("planner", "replanner")


def summarize(events: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Llamadas al LLM, planes que vienen de una llamada inválida, planes y pasos tras el escalado. `events`: el log en
    orden de `seq` (`event_type`, `correlation_id`, `causation_id`, `payload`)."""
    calls: Counter[str] = Counter()
    invalid_calls: Counter[str] = Counter()
    last_call: dict[Any, bool] = {}  # causa (command_id) -> la última llamada pasó la validación
    escalated: set[str] = set()
    from_invalid: list[dict[str, Any]] = []
    late_plans: list[dict[str, Any]] = []
    steps_after: list[dict[str, Any]] = []
    for e in events:
        incident, kind, p = e["correlation_id"], e["event_type"], e["payload"]
        if kind == "LlmCallRecorded":
            calls[p["agent"]] += 1
            if not p["validation_passed"]:
                invalid_calls[p["agent"]] += 1
            last_call[e["causation_id"]] = bool(p["validation_passed"])
        elif kind in PLAN_EVENTS:
            brief = {"incident_id": incident, "event_type": kind, "plan_version": p.get("plan_version")}
            if last_call.get(e["causation_id"]) is False:
                from_invalid.append(brief)
            if incident in escalated:
                late_plans.append(brief)
        elif kind == "IncidentEscalated":
            escalated.add(incident)
        elif (kind == "StepRequested" and incident in escalated
              and not str(p.get("step_id", "")).endswith(COMPENSATION_SUFFIX)):
            steps_after.append({"incident_id": incident, "step_id": p.get("step_id"), "action": p.get("action")})
    return {
        "llm_calls": dict(calls), "llm_invalid_calls": dict(invalid_calls),
        "plans_from_invalid_output": from_invalid[:20], "plans_after_escalation": len(late_plans),
        "steps_after_escalation": steps_after[:20],
    }


def provoked(variant: str, metrics: Mapping[str, Any]) -> bool:
    """La variante provocó su efecto: `invalid`, alguna llamada inválida; `slow`, algún `plan_timeout`; `error`, algún
    trabajo reintentado. `baseline` no tiene efecto que provocar."""
    if variant == "invalid":
        return sum(metrics["llm_invalid_calls"].values()) > 0
    if variant == "slow":
        return metrics["escalation_reasons"].get(PLAN_TIMEOUT, 0) > 0
    if variant == "error":
        return sum(j["retried"] for j in metrics["jobs"].values()) > 0
    return True


def h15_variant(variant: str, metrics: Mapping[str, Any]) -> bool:
    return (metrics["open"] == 0 and not metrics["plans_from_invalid_output"]
            and not metrics["steps_after_escalation"] and provoked(variant, metrics)
            and all(inv["violations"] == 0 for inv in metrics["invariants"].values()))


def job_attempts(h: Harness) -> dict[str, dict[str, int]]:
    """Trabajos del planner y del replanner: reintentados (más de un intento), máximo de intentos y `failed`."""
    out = {}
    for component in LLM_COMPONENTS:
        row = h.conn.execute(
            f"SELECT count(*) FILTER (WHERE attempts > 1) AS retried, coalesce(max(attempts), 0) AS max_attempts,"
            f" count(*) FILTER (WHERE status = 'failed') AS failed FROM {component}.command_jobs").fetchone()
        out[component] = dict(row)
    h.conn.commit()
    return out


def run_variant(base: Settings, variant: str, *, seed: int, incidents: int,
                plan_timeout_s: float = PLAN_TIMEOUT_S) -> dict[str, Any]:
    data = generate(seed, incidents)
    degradation = VARIANTS[variant]
    llm = Oracle(data).client()
    if degradation is not None:
        llm = DegradedLlm(llm, seed=seed, **degradation)
    config = RunConfig(seed=seed, incidents=incidents, faults=FaultPlan(seed=seed, fail_probability=FAIL_PROBABILITY),
                       plan_timeout_seconds=plan_timeout_s if variant == "slow" else None)
    with Harness(base, config, data=data, llm=llm) as h:
        h.send_notices()
        result = h.result(h.run_until_closed())
        events = [dict(r) for r in h.conn.execute(
            "SELECT event_type, correlation_id, causation_id, payload FROM event_log.events ORDER BY seq").fetchall()]
        h.conn.commit()
        metrics: dict[str, Any] = {
            "closed": result.closed, "open": result.open, "resolved": result.resolved,
            "escalated": result.escalated, "escalation_reasons": result.escalation_reasons,
            "injected": dict(getattr(llm, "injected", {})),
            **summarize(events),
            "jobs": job_attempts(h),
            "latency_ms": summary(result.latencies_ms),
        }
        metrics["invariants"] = invariants.check(h)
    metrics["h15"] = h15_variant(variant, metrics)
    return metrics


def run_e15(base: Settings, *, seed: int = 42, incidents: int = 50, plan_timeout_s: float = PLAN_TIMEOUT_S) -> Report:
    results = {v: run_variant(base, v, seed=seed, incidents=incidents, plan_timeout_s=plan_timeout_s)
               for v in VARIANTS}
    holds = all(m["h15"] and invariants.gate(m["invariants"]) for m in results.values())
    return Report(
        experiment="E15", seed=seed, model=Oracle.model,
        config={"incidents": incidents, "mode": "in_process", "fail_probability": FAIL_PROBABILITY,
                "plan_timeout_s": plan_timeout_s, "variants": {k: v or {} for k, v in VARIANTS.items()}},
        metrics={"variants": results},
        hypothesis={"H15": holds},
    )
