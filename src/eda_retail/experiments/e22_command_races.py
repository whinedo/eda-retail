"""E22: carreras de comandos y ventana efecto–registro (spec experiments, «E22 Carreras de comandos y ventana
efecto–registro (H22)»; spec synthetic-data, «Entrega concurrente de comandos»).

Ejecuciones de 50 incidencias con un 50 % de fallos en `reserve_stock` (fuerzan los replanes que piden `RevisePlan` y
`issue_refund`), una por variante:

- (a) `execute_step_<N>`: cada `ExecuteStep` llega desde N hilos a la vez (2, 4 y 8), cada uno con su conexión y el
  mismo `command_id`;
- (b) `other_commands`: lo mismo con `CreatePlan`, `RevisePlan`, `RegisterCarrierNotice` y `SeedSampleData`; al final,
  la observación «Mismo command_id con otro payload»;
- (c) `same_key`: el `ExecuteStep` de `notify_customer`, `file_claim`, `reserve_stock` e `issue_refund` llega a la vez
  con dos `command_id` distintos y la misma `idempotency_key`, con dos workers de trabajos por componente para que
  los dos trabajos compitan por la clave;
- (d) `effect_record_crash`: `notify_customer` e `issue_refund` caen entre el efecto y el registro de su clave en el
  primer intento de cada clave, y el trabajo se reintenta.

`ExecuteStep`, `CreatePlan` y `RevisePlan` son comandos con trabajo largo: la petición solo registra el inbox y el
trabajo, así que las entregas con el mismo `command_id` compiten en el inbox y solo un trabajo llega a `effect_keys`.
La carrera de `effect_keys` la ejerce (c).

Recuentos por variante: ejecuciones del servicio simulado por clave (para `reserve_stock`, que no tiene servicio
externo, los `StockReserved` por clave), respuestas distintas por `command_id`, eventos escritos (efectos y
`StepCompleted` por clave, `PlanCreated` por incidencia, trabajos por `command_id`) y trabajos que encontraron su
clave `in_progress` (`busy_claims`).
"""

from __future__ import annotations

import uuid
from collections import Counter
from collections.abc import Iterable, Mapping
from typing import Any

from eda_retail.config import Settings
from eda_retail.events.catalog import EFFECT_EVENT, Action
from eda_retail.events.ids import incident_id_for_notice
from eda_retail.experiments import invariants
from eda_retail.experiments.harness import SERVICE_OF, CommandDelivery, Harness, RunConfig, distinct_responses
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.report import Report
from eda_retail.infra.commands import CommandEnvelope, execute_command
from eda_retail.synthetic.faults import FaultPlan

FAIL_RESERVE = 0.5
STEP_THREADS = (2, 4, 8)  # (a)
OTHER_THREADS = 4  # (b)
OTHER_COMMANDS = ("CreatePlan", "RevisePlan", "RegisterCarrierNotice", "SeedSampleData")
SAME_KEY_ACTIONS = ("notify_customer", "file_claim", "reserve_stock", "issue_refund")  # (c)
CRASH_ACTIONS = ("notify_customer", "issue_refund")  # (d)
SAME_KEY_WORKERS = 2
EFFECT_ACTIONS = ("notify_customer", "file_claim", "reserve_stock", "reissue_shipment", "issue_refund")
JOB_SCHEMAS = ("planner", "replanner", *(f"executor_{a}" for a in SERVICE_OF))


def variants(step_threads: Iterable[int] = STEP_THREADS) -> dict[str, dict[str, Any]]:
    """Fallos y workers de cada variante."""
    out: dict[str, dict[str, Any]] = {
        f"execute_step_{n}": {"faults": {"concurrent_commands": {"ExecuteStep": n}}} for n in step_threads
    }
    out["other_commands"] = {"faults": {"concurrent_commands": dict.fromkeys(OTHER_COMMANDS, OTHER_THREADS)}}
    out["same_key"] = {"faults": {"reinvoke_steps": SAME_KEY_ACTIONS}, "job_workers": SAME_KEY_WORKERS}
    out["effect_record_crash"] = {"faults": {"crash_between_effect_and_record": CRASH_ACTIONS}}
    return out


# -- recuentos -----------------------------------------------------------------------------------------
def _max(counter: Mapping[Any, int]) -> int:
    return max(counter.values(), default=0)


def summarize(deliveries: Iterable[CommandDelivery], executions: Mapping[str, Mapping[str, int]],
              events: Iterable[Mapping[str, Any]], jobs: Iterable[Mapping[str, Any]], *, busy_claims: int,
              crashes: Mapping[str, str]) -> dict[str, Any]:
    """Recuentos del informe de una variante.

    `deliveries`: las entregas de comandos del arnés; `executions`: ejecuciones del servicio simulado por clave, por
    acción; `events`: filas de `event_log.events` (`event_type`, `correlation_id`, `causation_id`, `payload`);
    `jobs`: filas de `command_jobs` (`command_id`) de los componentes con trabajo largo; `busy_claims`: trabajos que
    encontraron su clave `in_progress`; `crashes`: claves que cayeron entre el efecto y el registro, con su acción."""
    deliveries, events = list(deliveries), list(events)
    key_of = {d.command_id: (d.action, d.idempotency_key) for d in deliveries if d.command_type == "ExecuteStep"}
    effect_type = {EFFECT_EVENT[Action(a)]: a for a in EFFECT_ACTIONS}
    effects: dict[str, Counter[str]] = {a: Counter() for a in EFFECT_ACTIONS}
    completed: Counter[str] = Counter()
    for e in events:
        _, key = key_of.get(_uuid(e["causation_id"]), (None, None))
        if key is None:
            continue
        if e["event_type"] in effect_type:
            effects[effect_type[e["event_type"]]][key] += 1
        elif e["event_type"] == "StepCompleted":
            completed[key] += 1
    per_key = {a: _max(executions.get(a, {})) for a in SERVICE_OF}
    per_key["reserve_stock"] = _max(effects["reserve_stock"])  # sin servicio externo: sus StockReserved
    concurrent = Counter(d.command_type for d in deliveries if len(d.responses) > 1)
    ids_per_key: dict[str, set[uuid.UUID]] = {}
    for d in deliveries:
        if d.command_type == "ExecuteStep":
            ids_per_key.setdefault(d.idempotency_key, set()).add(d.command_id)
    reinvoked = Counter(key_of[next(iter(ids))][0] for ids in ids_per_key.values() if len(ids) > 1)
    return {
        "executions_per_key": per_key,
        "distinct_responses_per_command_id": distinct_responses(deliveries),
        "concurrent_deliveries": {t: concurrent[t] for t in ("ExecuteStep", *OTHER_COMMANDS)},
        "reinvoked_keys": {a: reinvoked[a] for a in SAME_KEY_ACTIONS},
        "effect_record_crashes": {a: sum(1 for x in crashes.values() if x == a) for a in CRASH_ACTIONS},
        "events": {
            "effect_events_per_key": {a: _max(c) for a, c in effects.items()},
            "step_completed_per_key": _max(completed),
            "plan_created_per_incident": _max(Counter(e["correlation_id"] for e in events
                                                      if e["event_type"] == "PlanCreated")),
            "jobs_per_command_id": _max(Counter(j["command_id"] for j in jobs)),
        },
        "busy_claims": busy_claims,
    }


def _uuid(value: Any) -> uuid.UUID | None:
    return value if value is None or isinstance(value, uuid.UUID) else uuid.UUID(str(value))


EXERCISED = {  # lo que cada variante debe haber provocado para que su resultado cuente
    "other_commands": lambda m: all(m["concurrent_deliveries"][t] > 0 for t in OTHER_COMMANDS),
    "same_key": lambda m: all(n > 0 for n in m["reinvoked_keys"].values()),
    "effect_record_crash": lambda m: all(n > 0 for n in m["effect_record_crashes"].values()),
}


def exercised(variant: str, m: Mapping[str, Any]) -> bool:
    if variant.startswith("execute_step_"):
        return m["concurrent_deliveries"]["ExecuteStep"] > 0
    return EXERCISED[variant](m)


def h22(variant: str, m: Mapping[str, Any]) -> bool:
    """Ninguna ejecución del servicio por encima de una por clave, una sola respuesta por `command_id`, la variante
    ejercida e I1 a I5 sin violaciones."""
    return (exercised(variant, m)
            and all(n <= 1 for n in m["executions_per_key"].values())
            and m["distinct_responses_per_command_id"] <= 1
            and all(inv["violations"] == 0 for inv in m["invariants"].values()))


# -- ejecución -----------------------------------------------------------------------------------------
def _rows(h: Harness, sql: str) -> list[dict[str, Any]]:
    rows = [dict(r) for r in h.conn.execute(sql).fetchall()]
    h.conn.commit()
    return rows


def same_command_other_payload(h: Harness) -> dict[str, Any]:
    """Observación «Mismo command_id con otro payload» (fuera de H22): el aviso 0 otra vez con `command_id` C1 (un
    duplicado de negocio) y después C1 con el payload de un aviso nuevo. Anota las dos respuestas."""
    n, intake = h.data.notices[0], h.targets["intake"]
    c1 = uuid.uuid4()

    def send(notice_id: str, raw_text: str) -> Any:
        command = CommandEnvelope(command_id=c1, command_type="RegisterCarrierNotice",
                                  correlation_id=incident_id_for_notice(notice_id), causation_id=None,
                                  payload={"carrier_notice_id": notice_id, "carrier": n.carrier, "raw_text": raw_text})
        return execute_command(h.conn, intake.schema, command, intake.specs["RegisterCarrierNotice"])

    first = send(n.carrier_notice_id, n.raw_text(h.config.fmt))
    second = send(f"{n.carrier_notice_id}-OTHER", h.data.notices[-1].raw_text(h.config.fmt))
    return {"first": {"status": first.status_code, "reason": first.body.get("reason")},
            "second": {"status": second.status_code, "reason": second.body.get("reason"),
                       "replayed": second.replayed},
            "same_response": (first.status_code, first.body) == (second.status_code, second.body)}


def _measure(h: Harness) -> dict[str, Any]:
    events = _rows(h, "SELECT event_type, correlation_id, causation_id, payload FROM event_log.events ORDER BY seq")
    jobs = [r for schema in JOB_SCHEMAS for r in _rows(h, f"SELECT command_id FROM {schema}.command_jobs")]
    executions = {a: dict(getattr(h.services, s).executions) for a, s in SERVICE_OF.items()}
    return summarize(h.commands.deliveries, executions, events, jobs, busy_claims=h.busy_claims,
                     crashes=h.effect_crashes.crashed)


def run_variant(base: Settings, variant: str, setup: Mapping[str, Any], *, seed: int,
                incidents: int) -> dict[str, Any]:
    faults = FaultPlan(seed=seed, fail_probability={"reserve_stock": FAIL_RESERVE}, **setup["faults"])
    config = RunConfig(seed=seed, incidents=incidents, faults=faults, job_workers=setup.get("job_workers", 1))
    with Harness(base, config) as h:
        h.send_notices()
        result = h.result(h.run_until_closed())
        metrics = {"closed": result.closed, "open": result.open, "escalated": result.escalated,
                   "escalation_reasons": result.escalation_reasons, **_measure(h)}
        if variant == "other_commands":
            metrics["same_command_other_payload"] = same_command_other_payload(h)
        metrics["invariants"] = invariants.check(h)
    metrics["h22"] = h22(variant, metrics) and invariants.gate(metrics["invariants"])
    return metrics


def run_e22(base: Settings, *, seed: int = 42, incidents: int = 50,
            step_threads: Iterable[int] = STEP_THREADS) -> Report:
    setups = variants(step_threads)
    results = {v: run_variant(base, v, setup, seed=seed, incidents=incidents) for v, setup in setups.items()}
    return Report(
        experiment="E22", seed=seed, model=Oracle.model,
        config={"incidents": incidents, "fail_probability": {"reserve_stock": FAIL_RESERVE},
                "variants": {v: {"faults": {k: (dict(x) if isinstance(x, Mapping) else list(x))
                                            for k, x in s["faults"].items()},
                                 "job_workers": s.get("job_workers", 1)} for v, s in setups.items()}},
        metrics={"variants": results}, hypothesis={"H22": all(r["h22"] for r in results.values())},
    )
