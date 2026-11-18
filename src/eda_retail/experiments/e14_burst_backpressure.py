"""E14: ráfaga y contrapresión (spec experiments, «E14 Ráfaga y contrapresión (H14)»; spec resilience, «Pico de
incidencias»; spec synthetic-data, «Capacidad limitada de un ejecutor y trabajo lento»).

200 avisos enviados de golpe, en proceso, con dos variantes:

- `baseline`: sin límite de capacidad; la referencia de la latencia.
- `backpressure`: capacidad 3 en cada ejecutor. Con 3 trabajos abiertos (`pending` o `running` en su
  `command_jobs`), el ejecutor responde 503 a un `ExecuteStep` nuevo y el dispatcher lo reprograma con backoff y el
  mismo `command_id`.

En las dos, el `prepare` del planner tarda 0,05 s (el LLM) y el de los ejecutores 0,1 s (el servicio externo): los
pasos llegan a cada ejecutor más deprisa de lo que los atiende y las colas de trabajos crecen. `QueueSampler` mide en cada turno (como mucho cada `SAMPLE_EVERY_S`) los trabajos
abiertos por componente y los comandos pendientes del dispatcher, y anota cuándo estuvieron vacías por última vez.

H14 (sobre `backpressure`): al menos un 503, ningún ejecutor por encima de su capacidad, colas vacías al terminar,
ningún paso con `ExecuteStep` de `command_id` distinto en la misma versión del plan, todas las incidencias cerradas e
I1 a I5 sin violaciones. La latencia frente a `baseline` es un dato, no parte de H14.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.e16_zombie_consumer import duplicate_commands
from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.report import Report, summary
from eda_retail.infra.schemas import JOB_COMPONENTS, qualified
from eda_retail.step_executors.actions import ACTIONS as EXECUTOR_ACTIONS
from eda_retail.synthetic.faults import FaultPlan

EXECUTORS = tuple(f"executor_{action}" for action in EXECUTOR_ACTIONS)
CAPACITY = 3  # trabajos abiertos como máximo por ejecutor en `backpressure`
PREPARE_DELAY_S = {"planner": 0.05, **{executor: 0.1 for executor in EXECUTORS}}
VARIANTS = ("baseline", "backpressure")
SAMPLE_EVERY_S = 0.05
RUN_TIMEOUT_S = 900.0


class QueueSampler:
    """Trabajos abiertos por componente y comandos pendientes del dispatcher, con sus máximos y el último instante en
    que alguna cola tenía algo (desde `start`)."""

    def __init__(self, h: Harness, start: float) -> None:
        self._h = h
        self._start = start
        self._next = 0.0
        self.max_open_jobs: dict[str, int] = dict.fromkeys(JOB_COMPONENTS, 0)
        self.max_pending_commands = 0
        self.last_busy_s = 0.0

    def __call__(self) -> None:
        now = time.monotonic()
        if now < self._next:
            return
        self._next = now + SAMPLE_EVERY_S
        self.observe(queue_depths(self._h), now - self._start)

    def observe(self, depths: Mapping[str, Any], at: float) -> None:
        for component, count in depths["open_jobs"].items():
            self.max_open_jobs[component] = max(self.max_open_jobs.get(component, 0), count)
        self.max_pending_commands = max(self.max_pending_commands, depths["pending_commands"])
        if depths["pending_commands"] or any(depths["open_jobs"].values()):
            self.last_busy_s = at


def queue_depths(h: Harness) -> dict[str, Any]:
    """Comandos `pending` en `orchestrator.command_outbox` y trabajos `pending` o `running` por componente."""
    union = " UNION ALL ".join(
        f"SELECT '{c}' AS component, count(*) AS n FROM {qualified(c, 'command_jobs')}"
        " WHERE status IN ('pending', 'running')" for c in JOB_COMPONENTS)
    jobs = {r["component"]: r["n"] for r in h.conn.execute(union).fetchall()}
    pending = h.conn.execute(
        "SELECT count(*) AS n FROM orchestrator.command_outbox WHERE status = 'pending'").fetchone()["n"]
    h.conn.commit()
    return {"open_jobs": jobs, "pending_commands": pending}


def command_attempts(h: Harness) -> dict[str, Any]:
    """Intentos del dispatcher por comando: el máximo y cuántos se reintentaron (más de un intento)."""
    row = h.conn.execute(
        "SELECT coalesce(max(attempts), 0) AS max, count(*) FILTER (WHERE attempts > 1) AS retried,"
        " count(*) AS total FROM orchestrator.command_outbox").fetchone()
    h.conn.commit()
    return {"max": row["max"], "retried": row["retried"], "total": row["total"]}


def within_capacity(max_open_jobs: Mapping[str, int], capacity: Mapping[str, int]) -> dict[str, int]:
    """Ejecutores cuyo máximo de trabajos abiertos superó su capacidad (vacío si ninguno)."""
    return {e: n for e, n in max_open_jobs.items() if e in capacity and n > capacity[e]}


def h14(metrics: Mapping[str, Any]) -> bool:
    """Sobre `backpressure`: al menos un 503, ningún ejecutor por encima de su capacidad, colas vacías al terminar, sin
    comandos duplicados, todas las incidencias cerradas e I1 a I5 sin violaciones."""
    final = metrics["final_queues"]
    return (sum(metrics["unavailable_503"].values()) > 0 and not metrics["over_capacity"]
            and final["pending_commands"] == 0 and not any(final["open_jobs"].values())
            and not metrics["duplicate_commands"] and metrics["open"] == 0
            and all(inv["violations"] == 0 for inv in metrics["invariants"].values()))


def run_variant(base: Settings, variant: str, *, seed: int, incidents: int, capacity: int = CAPACITY) -> dict[str, Any]:
    limits = {executor: capacity for executor in EXECUTORS} if variant == "backpressure" else {}
    faults = FaultPlan(seed=seed, prepare_delay_seconds=PREPARE_DELAY_S, executor_capacity=limits)
    config = RunConfig(seed=seed, incidents=incidents, faults=faults, timeout_seconds=RUN_TIMEOUT_S)
    with Harness(base, config) as h:
        start = time.monotonic()
        h.send_notices()
        sampler = QueueSampler(h, start)
        result = h.result(h.run_until_closed(on_turn=sampler))
        final = queue_depths(h)
        sampler.observe(final, time.monotonic() - start)
        metrics: dict[str, Any] = {
            "closed": result.closed, "open": result.open, "resolved": result.resolved,
            "escalated": result.escalated, "escalation_reasons": result.escalation_reasons,
            "unavailable_503": dict(h.commands.unavailable_503),
            "command_attempts": command_attempts(h),
            "max_open_jobs": sampler.max_open_jobs,
            "max_pending_commands": sampler.max_pending_commands,
            "drained_after_s": sampler.last_busy_s,
            "final_queues": final,
            "over_capacity": within_capacity(sampler.max_open_jobs, faults.executor_capacity),
            "duplicate_commands": duplicate_commands(h)[:20],
            "latency_ms": summary(result.latencies_ms),
            "elapsed_s": result.elapsed_seconds,
        }
        metrics["invariants"] = invariants.check(h)
    return metrics


def run_e14(base: Settings, *, seed: int = 42, incidents: int = 200, capacity: int = CAPACITY) -> Report:
    results = {v: run_variant(base, v, seed=seed, incidents=incidents, capacity=capacity) for v in VARIANTS}
    backpressure = results["backpressure"]
    gated = invariants.gate(backpressure["invariants"]) and invariants.gate(results["baseline"]["invariants"])
    before, now = results["baseline"]["latency_ms"]["p95"], backpressure["latency_ms"]["p95"]
    return Report(
        experiment="E14", seed=seed, model=Oracle.model,
        config={"incidents": incidents, "mode": "in_process", "capacity": capacity,
                "prepare_delay_s": PREPARE_DELAY_S, "variants": list(VARIANTS)},
        metrics={"variants": results,
                 "p95_ratio": (now / before) if before and now is not None else None},
        hypothesis={"H14": h14(backpressure) and gated},
    )
