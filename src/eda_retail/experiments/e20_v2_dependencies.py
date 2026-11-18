"""E20: dependencias de V2 (spec experiments, «E20 Dependencias de V2 (H20)»; spec synthetic-data, «Fallos de las
dependencias de V2»; specs mcp-mail y a2a-carrier).

50 incidencias en proceso con `POC_VERSION=2`: `notify_customer` envía con su cliente MCP al servidor de correo en
proceso y el planificador consulta por A2A al agente del transportista en proceso, con `A2A_TIMEOUT_S` de 1 s. Seis
variantes, con un solo fallo cada una (`synthetic.v2_faults`):

- `baseline`: sin fallos.
- `mail_down`: un 50 % de las llamadas a `send_email` terminan en un error de la herramienta (`mail_unavailable`,
  transitorio: el ejecutor reintenta con espera exponencial hasta `STEP_MAX_ATTEMPTS`).
- `mail_invalid`: un 30 % devuelven una salida sin `message_id` o con la dirección (`mcp_output_invalid`).
- `carrier_slow`, `carrier_401` y `carrier_bad_card`: un 50 % de las consultas encuentran un agente que tarda 2 s, que
  responde 401 al token o cuya Agent Card anuncia otra skill (`timeout`, `failed` y `rejected_by_guardrail`).

Las métricas salen del log (`event_log.events`, en orden de `seq`), del servicio simulado `mail` (ejecuciones por
`idempotency_key`) y de los propios fallos (inyecciones y tareas que recibió el agente). H20: en todas las variantes,
ninguna incidencia abierta, todo `outcome` de `CarrierConsulted` es el del fallo inyectado (o `answered`), la consulta
no supera `A2A_TIMEOUT_S` más 1,5 s, ninguna consulta con un 401 o con la Agent Card distinta llega al agente, los
planes coinciden con los de `baseline`, ninguna clave tiene más de un correo, ningún evento contiene una dirección de
correo, I1 a I5 sin violaciones y cada variante con fallos provoca su efecto.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import replace
from typing import Any

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.report import Report, summary
from eda_retail.synthetic.generator import generate
from eda_retail.synthetic.v2_faults import FaultyCarrier, FaultyMail

A2A_TIMEOUT_S = 1.0
LATENCY_MARGIN_S = 1.5  # la cancelación de la tarea (1 s como mucho) y un margen
MAIL_UNAVAILABLE = "mail_unavailable"
MCP_OUTPUT_INVALID = "mcp_output_invalid"
VARIANTS: dict[str, dict[str, Any] | None] = {
    "baseline": None,
    "mail_down": {"mail": "down", "probability": 0.5},
    "mail_invalid": {"mail": "invalid", "probability": 0.3},
    "carrier_slow": {"carrier": "slow", "probability": 0.5},
    "carrier_401": {"carrier": "unauthorized", "probability": 0.5},
    "carrier_bad_card": {"carrier": "bad_card", "probability": 0.5},
}
NO_TASKS = ("unauthorized", "bad_card")  # consultas que no deben llegar al agente
NOTIFY_PRODUCER = "executor-notify_customer"
_EMAIL = re.compile(r"[^\s@\"]+@[^\s@\"]+\.[A-Za-z]{2,}")


def summarize(events: Iterable[Mapping[str, Any]], expected_outcome) -> dict[str, Any]:
    """Consultas al transportista, primeros planes, fallos de `notify_customer` y direcciones en el log. `events`: el log
    en orden de `seq` (`event_type`, `correlation_id`, `producer`, `payload`); `expected_outcome(incident_id)`: el
    `outcome` esperado. `planned` da, por incidencia, los pasos de su `PlanCreated` (acción y parámetros)."""
    outcomes: Counter[str] = Counter()
    mismatches: list[dict[str, Any]] = []
    latencies: list[int] = []
    planned: dict[str, list[list[Any]]] = {}
    errors: Counter[str] = Counter()
    unavailable_not_retryable = 0
    notified = 0
    with_address = 0
    for e in events:
        incident, kind, p = e["correlation_id"], e["event_type"], e["payload"]
        if _EMAIL.search(json.dumps(p, ensure_ascii=False, default=str)):
            with_address += 1
        if kind == "CarrierConsulted":
            outcomes[p["outcome"]] += 1
            latencies.append(int(p["latency_ms"]))
            expected = expected_outcome(incident)
            if p["outcome"] != expected:
                mismatches.append({"incident_id": incident, "outcome": p["outcome"], "expected": expected})
        elif kind == "PlanCreated":
            planned.setdefault(incident, [[s.get("step_id"), s.get("action"), s.get("params")]
                                          for s in p.get("steps", [])])
        elif kind == "StepFailed" and e.get("producer") == NOTIFY_PRODUCER:
            errors[p["error"]] += 1
            if p["error"] == MAIL_UNAVAILABLE and not p.get("retryable"):
                unavailable_not_retryable += 1
        elif kind == "CustomerNotified":
            notified += 1
    return {
        "carrier_outcomes": dict(outcomes), "outcome_mismatches": mismatches[:20],
        "max_consult_latency_ms": max(latencies, default=0), "planned": dict(sorted(planned.items())),
        "step_failed_errors": dict(errors), "mail_unavailable_not_retryable": unavailable_not_retryable,
        "customer_notified": notified, "events_with_address": with_address,
    }


def provoked(variant: str, metrics: Mapping[str, Any]) -> bool:
    """La variante provocó su efecto: algún fallo inyectado; en `mail_down`, algún reintento de `send_email`; en
    `mail_invalid`, un `StepFailed` con `mcp_output_invalid` por cada salida inválida. `baseline` no tiene efecto."""
    spec = VARIANTS[variant]
    if spec is None:
        return True
    injected = metrics["injected"]
    if spec.get("mail") == "down":
        return injected["mail"] > 0 and metrics["mail_calls"] > metrics["mail_keys"]
    if spec.get("mail") == "invalid":
        return injected["mail"] > 0 and metrics["step_failed_errors"].get(MCP_OUTPUT_INVALID, 0) == injected["mail"]
    return injected["carrier"] > 0


def h20_variant(variant: str, metrics: Mapping[str, Any], baseline_planned: Mapping[str, Any]) -> bool:
    spec = VARIANTS[variant] or {}
    no_tasks = spec.get("carrier") not in NO_TASKS or metrics["carrier"]["tasks_on_injected"] == 0
    return (metrics["open"] == 0 and not metrics["outcome_mismatches"]
            and metrics["max_consult_latency_ms"] <= (A2A_TIMEOUT_S + LATENCY_MARGIN_S) * 1000
            and no_tasks and metrics["planned"] == baseline_planned
            and metrics["max_mail_executions_per_key"] <= 1 and metrics["events_with_address"] == 0
            and metrics["mail_unavailable_not_retryable"] == 0 and provoked(variant, metrics)
            and all(inv["violations"] == 0 for inv in metrics["invariants"].values()))


def settings_for(base: Settings) -> Settings:
    """La versión 2 con el plazo de A2A del experimento."""
    return replace(base, poc_version=2, a2a_timeout_s=A2A_TIMEOUT_S)


def run_variant(base: Settings, variant: str, *, seed: int, incidents: int) -> dict[str, Any]:
    settings = settings_for(base)
    spec = VARIANTS[variant] or {}
    data = generate(seed, incidents)
    probability = spec.get("probability", 0.0)
    carrier = FaultyCarrier(settings, spec.get("carrier"), probability=probability, seed=seed)
    mail = FaultyMail(spec["mail"], probability=probability, seed=seed) if "mail" in spec else None
    kwargs = {"mail_server": mail.build} if mail else {}
    config = RunConfig(seed=seed, incidents=incidents)
    with Harness(settings, config, data=data, llm=Oracle(data).client(), carrier=carrier, **kwargs) as h:
        h.send_notices()
        result = h.result(h.run_until_closed())
        events = [dict(r) for r in h.conn.execute(
            "SELECT event_type, correlation_id, producer, payload FROM event_log.events ORDER BY seq").fetchall()]
        h.conn.commit()
        executions = h.services.mail.executions
        metrics: dict[str, Any] = {
            "closed": result.closed, "open": result.open, "resolved": result.resolved,
            "escalated": result.escalated, "escalation_reasons": result.escalation_reasons,
            "injected": {"mail": mail.injected if mail else 0, "carrier": len(carrier.injected)},
            "carrier": carrier.report(),
            "mail_calls": mail.calls if mail else 0, "mail_keys": mail.keys if mail else 0,
            "max_mail_executions_per_key": max(executions.values(), default=0),
            **summarize(events, carrier.expected_outcome),
            "latency_ms": summary(result.latencies_ms),
        }
        metrics["invariants"] = invariants.check(h)
    return metrics


def run_e20(base: Settings, *, seed: int = 42, incidents: int = 50) -> Report:
    results = {v: run_variant(base, v, seed=seed, incidents=incidents) for v in VARIANTS}
    baseline_planned = results["baseline"]["planned"]
    for variant, metrics in results.items():
        planned = metrics["planned"]
        metrics["plans_differing_from_baseline"] = sorted(
            i for i in set(planned) | set(baseline_planned) if planned.get(i) != baseline_planned.get(i))
        metrics["h20"] = h20_variant(variant, metrics, baseline_planned)
    holds = all(m["h20"] and invariants.gate(m["invariants"]) for m in results.values())
    return Report(
        experiment="E20", seed=seed, model=Oracle.model,
        config={"incidents": incidents, "mode": "in_process", "poc_version": 2, "a2a_timeout_s": A2A_TIMEOUT_S,
                "variants": {k: v or {} for k, v in VARIANTS.items()}},
        metrics={"variants": results},
        hypothesis={"H20": holds},
    )
