"""E8 Matriz de puntos de caída (spec experiments, «E8 Matriz de puntos de caída (H8)»; spec synthetic-data, «Caída en
un punto de la transacción»).

En proceso, con el oráculo como LLM y un 30 % de fallos en `reserve_stock` (para que haya planes revisados). Primero
`baseline`, sin caídas, que cuenta las unidades que cada componente atiende en cada punto (`Harness.crash_points.units`)
y las llamadas al LLM en el cliente (`Harness.llm_client_calls`). Después, una ejecución por celda (componente, punto)
con `FaultPlan(crash_at={celda: K})`, K = ⌈n/2⌉ de las n unidades de esa celda en `baseline` con la misma semilla; una
celda con n = 0 no puede caer: no se ejecuta y consta como no provocada. Las 23 celdas salen de `CRASH_POINTS`.

Una celda cuenta si su caída se produjo y se ve el mecanismo que la salva (`mechanism`):

- `before_tx`: el componente reiniciado vuelve a atender la unidad y el cliente del LLM recibe al menos una llamada más
  que en `baseline` para su agente.
- `after_commit_before_offset`: al menos un descarte del inbox del consumidor que cayó.
- `after_commit_before_response`: al menos una respuesta repetida por el inbox de ese sistema con el mismo
  `command_id` (la saga reintenta; en `RegisterCarrierNotice`, el harness).
- `relay_after_publish_before_mark`: al menos un `event_id` publicado dos veces en `eda.events`.

Y está bien (`ok`) si además todas las incidencias se cierran, ninguna `idempotency_key` se ejecuta más de una vez,
cada evento tiene una sola fila en `event_log`, no queda outbox pendiente e I1 a I5 no tienen violaciones. H8 se cumple
si las 23 celdas están bien. `cells` ejecuta solo algunas (el test de integración, una por punto): H8 no se cumple
entonces, porque la matriz no está completa, pero cada celda lleva su `ok`.
"""

from __future__ import annotations

import json
import math
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from confluent_kafka import TopicPartition

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.e9_kafka_down import log_counts, pending_outbox
from eda_retail.experiments.harness import PUBLISHERS, SERVICE_OF, CommandDelivery, Harness, RunConfig
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.report import Report, summary
from eda_retail.infra.kafka import make_consumer
from eda_retail.infra.positions import watermark_offsets
from eda_retail.projectors.registry import build_all
from eda_retail.synthetic.faults import (
    AFTER_COMMIT_BEFORE_OFFSET,
    AFTER_COMMIT_BEFORE_RESPONSE,
    BEFORE_TX,
    CRASH_POINTS,
    RELAY_AFTER_PUBLISH_BEFORE_MARK,
    CrashRecord,
    FaultPlan,
)

Cell = tuple[str, str]  # (componente, punto)

FAIL_RESERVE = 0.3
# Lease de los trabajos en E8. Con el de siempre (600 s), una celda `before_tx` del planificador o del replanificador
# esperaría 10 min a que el worker reiniciado retome el trabajo. 5 s basta: con un solo worker por componente nadie le
# quita un trabajo vivo a otro, los trabajos con el oráculo duran milisegundos (E8 no retrasa ningún `prepare`), el plazo
# de la saga para un plan (300 s) queda lejos, y mientras el trabajo siga `running` el harness no da la ejecución por
# quieta (`Harness._working` cuenta los trabajos abiertos), así que `idle_seconds` no la corta antes de tiempo.
JOB_LEASE_S = 5.0
POINTS = (BEFORE_TX, AFTER_COMMIT_BEFORE_OFFSET, AFTER_COMMIT_BEFORE_RESPONSE, RELAY_AFTER_PUBLISH_BEFORE_MARK)
AGENT_OF = {"interpreter": "interpreter", "planner": "planner", "replanner": "replanner"}  # agente del LLM (before_tx)
SCHEMA_OF_COMMAND = {"RegisterCarrierNotice": "intake", "SeedSampleData": "inventory", "CreatePlan": "planner",
                     "RevisePlan": "replanner"}
INVENTORY_ACTIONS = frozenset({"reserve_stock", "release_stock"})
TOPIC_READ_TIMEOUT_S = 30.0
# Métricas de cada celda que pide la spec (y `baseline` tiene las mismas, salvo la caída y el mecanismo).
CELL_METRICS = ("crash", "closed", "open", "resolved", "escalated", "escalation_reasons", "executions_per_key_max",
                "repeated_steps", "published_more_than_once", "max_publications_per_event_id",
                "log_rows_per_event_id_max", "missing_in_log", "pending_outbox", "pending_outbox_total",
                "inbox_discards", "projector_applied_events_hits", "replayed_responses",
                "max_replayed_per_command_id", "llm_client_calls", "llm_extra_calls", "latency_ms", "invariants",
                "mechanism", "counts", "ok")


# -- celdas --------------------------------------------------------------------------------------------------
def cells() -> list[Cell]:
    """Las 23 celdas de la matriz, por punto y, dentro de cada punto, en el orden de los componentes del harness."""
    order = {name: i for i, name in enumerate(PUBLISHERS)}
    return [(component, point) for point in POINTS for component in sorted(CRASH_POINTS[point], key=order.get)]


_matrix = cells  # `run_e8` tiene un parámetro `cells` (el de la CLI)


def cell_name(cell: Cell) -> str:
    return f"{cell[0]}@{cell[1]}"


def parse_cells(names: Iterable[str]) -> list[Cell]:
    """Celdas por su nombre `<componente>@<punto>`; ValueError con una que no está en la matriz."""
    known = {cell_name(c): c for c in _matrix()}
    out = []
    for name in names:
        if name not in known:
            raise ValueError(f"celda desconocida: {name} (disponibles: {', '.join(known)})")
        out.append(known[name])
    return out


def crash_unit(n: int) -> int | None:
    """La unidad K de la caída: la del medio (⌈n/2⌉) de las n de `baseline`; None si no hubo ninguna."""
    return math.ceil(n / 2) if n > 0 else None


# -- recuentos -----------------------------------------------------------------------------------------------
def command_schema(command_type: str, action: str | None) -> str:
    """El sistema que atiende el comando."""
    if command_type == "ExecuteStep":
        return "inventory" if action in INVENTORY_ACTIONS else f"executor_{action}"
    return SCHEMA_OF_COMMAND.get(command_type, command_type)


def replayed_responses(deliveries: Iterable[CommandDelivery]) -> tuple[dict[str, int], int]:
    """Respuestas que el inbox devolvió guardadas, por sistema, y el máximo para un mismo `command_id`."""
    per_schema: Counter[str] = Counter()
    per_command: Counter[uuid.UUID] = Counter()
    for d in deliveries:
        if d.replayed:
            per_schema[command_schema(d.command_type, d.action)] += d.replayed
            per_command[d.command_id] += d.replayed
    return dict(per_schema), max(per_command.values(), default=0)


def inbox_discards(outcomes: Mapping[tuple[str, str], int]) -> dict[str, int]:
    """Mensajes que el inbox de cada consumidor de Kafka descartó (resultado `duplicate`, como E9)."""
    return {name: n for (name, outcome), n in outcomes.items() if outcome == "duplicate" and n}


def publication_counts(publications: Sequence[tuple[str, str | None]],
                       projectors_attending: Callable[[str | None], int]) -> dict[str, int]:
    """Recuentos de los mensajes de `eda.events` (`event_id`, `event_type`). Una publicación repetida de un evento llega
    a cada proyector que atiende su tipo con un offset nuevo y la descarta `views.applied_events`: cada una cuenta en
    `projector_applied_events_hits`."""
    per_id = Counter(event_id for event_id, _ in publications)
    types = dict(publications)
    return {
        "messages": len(publications), "event_ids": len(per_id),
        "published_more_than_once": sum(1 for n in per_id.values() if n > 1),
        "max_publications_per_event_id": max(per_id.values(), default=0),
        "projector_applied_events_hits": sum((n - 1) * projectors_attending(types[e]) for e, n in per_id.items()),
    }


def llm_extra(calls: Mapping[str, int], baseline: Mapping[str, int]) -> dict[str, int]:
    """Llamadas al LLM de más frente a `baseline`, por agente, contadas en el cliente."""
    return {agent: n - baseline.get(agent, 0) for agent, n in calls.items() if n > baseline.get(agent, 0)}


def _iso(at: float | None) -> str | None:
    return None if at is None else datetime.fromtimestamp(at, UTC).isoformat()


def crash_summary(records: Iterable[CrashRecord], cell: Cell, *, baseline_units: int) -> dict[str, Any]:
    """Si la caída de la celda se produjo, en qué unidad y a qué hora, cuándo atendió el componente reiniciado su unidad
    siguiente y el tiempo de recuperación."""
    record = next((r for r in records if (r.component, r.point) == cell), None)
    if record is None:
        return {"provoked": False, "unit": None, "at": None, "resumed_at": None, "recovery_seconds": None,
                "baseline_units": baseline_units}
    recovery = None if record.resumed_at is None else round(record.resumed_at - record.at, 3)
    return {"provoked": True, "unit": record.unit, "at": _iso(record.at), "resumed_at": _iso(record.resumed_at),
            "recovery_seconds": recovery, "baseline_units": baseline_units}


def mechanism(cell: Cell, m: Mapping[str, Any]) -> bool:
    """Se ve el mecanismo que salva la caída de la celda (spec, por punto)."""
    component, point = cell
    if point == BEFORE_TX:
        return m["crash"]["resumed_at"] is not None and m["llm_extra_calls"].get(AGENT_OF[component], 0) >= 1
    if point == AFTER_COMMIT_BEFORE_OFFSET:
        return m["inbox_discards"].get(component, 0) >= 1
    if point == AFTER_COMMIT_BEFORE_RESPONSE:
        return m["replayed_responses"].get(component, 0) >= 1
    return m["published_more_than_once"] >= 1


def guarantees(m: Mapping[str, Any]) -> bool:
    """Todas las incidencias cerradas, ninguna clave ejecutada dos veces, una fila de `event_log` por evento, ninguno
    perdido, ninguna fila de outbox pendiente e I1 a I5 sin violaciones."""
    return (m["open"] == 0 and m["executions_per_key_max"] <= 1 and m["repeated_steps"] == 0
            and m["log_rows_per_event_id_max"] <= 1 and m["missing_in_log"] == 0 and m["pending_outbox_total"] == 0
            and all(inv["violations"] == 0 for inv in m["invariants"].values()))


def summarize(cell: Cell | None, *, result: Any, records: Iterable[CrashRecord], baseline_units: int,
              executions: Mapping[str, Mapping[str, int]], publications: Sequence[tuple[str, str | None]],
              projectors_attending: Callable[[str | None], int], log_rows_max: int, missing_in_log: int,
              pending: Mapping[str, int], outcomes: Mapping[tuple[str, str], int],
              deliveries: Iterable[CommandDelivery], llm_calls: Mapping[str, int], baseline_llm: Mapping[str, int],
              invariants: Mapping[str, Mapping[str, Any]], plan_created_max: int,
              saga_repeats: Mapping[str, int]) -> dict[str, Any]:
    """Métricas de una ejecución (`cell` None: `baseline`). `result`: el `RunResult` del harness; `executions`:
    ejecuciones por clave de cada servicio simulado; `publications`: (`event_id`, `event_type`) de cada mensaje del
    topic; `log_rows_max`: el máximo de filas de `event_log` por `event_id`; `pending`: outbox sin publicar por
    componente; `outcomes`: (consumidor, resultado) de cada mensaje; `deliveries`: las entregas de comandos."""
    replayed, max_replayed = replayed_responses(deliveries)
    m: dict[str, Any] = {
        "crash": None if cell is None else crash_summary(records, cell, baseline_units=baseline_units),
        "closed": result.closed, "open": result.open, "resolved": result.resolved, "escalated": result.escalated,
        "escalation_reasons": dict(result.escalation_reasons),
        "executions_per_key_max": max((n for per_key in executions.values() for n in per_key.values()), default=0),
        "repeated_steps": result.repeated_steps,
        **publication_counts(publications, projectors_attending),
        "log_rows_per_event_id_max": log_rows_max, "missing_in_log": missing_in_log,
        "pending_outbox": {k: v for k, v in pending.items() if v}, "pending_outbox_total": sum(pending.values()),
        "inbox_discards": inbox_discards(outcomes),
        "replayed_responses": replayed, "max_replayed_per_command_id": max_replayed,
        "llm_client_calls": dict(llm_calls), "llm_extra_calls": llm_extra(llm_calls, baseline_llm),
        "plan_created_per_incident_max": plan_created_max, "saga_repeats": dict(saga_repeats),
        "latency_ms": summary(list(result.latencies_ms)),
        "invariants": dict(invariants),
    }
    if cell is None:
        m["mechanism"] = m["counts"] = None
    else:
        m["mechanism"] = mechanism(cell, m)
        m["counts"] = m["crash"]["provoked"] and m["mechanism"]
    m["ok"] = guarantees(m) and m["counts"] is not False
    return m


def not_provoked(cell: Cell) -> dict[str, Any]:
    """Una celda sin unidades en `baseline` (n = 0): no puede caer, no se ejecuta y no cuenta."""
    return {"crash": crash_summary([], cell, baseline_units=0), "mechanism": False, "counts": False, "ok": False,
            "reason": "sin unidades en baseline: la caída no se puede producir"}


def h8(results: Mapping[str, Mapping[str, Any]]) -> bool:
    """Las 23 celdas ejecutadas y todas bien: la caída se produjo, se vio su mecanismo y se mantienen las garantías."""
    return set(results) == {cell_name(c) for c in _matrix()} and all(m["ok"] for m in results.values())


# -- ejecución -----------------------------------------------------------------------------------------------
def _rows(h: Harness, sql: str) -> list[dict[str, Any]]:
    rows = [dict(r) for r in h.conn.execute(sql).fetchall()]
    h.conn.commit()
    return rows


def _scalar(h: Harness, sql: str) -> int:
    return int(_rows(h, sql)[0]["n"])


def topic_publications(h: Harness, timeout: float = TOPIC_READ_TIMEOUT_S) -> list[tuple[str, str | None]]:
    """(`event_id`, `event_type`) de cada mensaje del topic de la ejecución, desde el principio hasta las marcas de agua
    altas de ahora, con un consumidor propio sin grupo que confirme offsets."""
    s = h.settings
    kafka = make_consumer(s.kafka_bootstrap_servers, f"e8-reader-{uuid.uuid4().hex[:8]}")
    try:
        highs = {p: watermark_offsets(kafka, TopicPartition(s.events_topic, p))[1] for p in h.partitions()}
        kafka.assign([TopicPartition(s.events_topic, p, 0) for p, high in highs.items() if high > 0])
        last = dict.fromkeys(highs, -1)
        out: list[tuple[str, str | None]] = []
        deadline = time.monotonic() + timeout
        while any(last[p] < high - 1 for p, high in highs.items()) and time.monotonic() < deadline:
            for msg in kafka.consume(500, 0.5):
                if msg.error() or msg.offset() >= highs[msg.partition()]:
                    continue
                last[msg.partition()] = msg.offset()
                headers = dict(msg.headers() or [])
                event_type = headers.get("event_type")
                try:
                    event_id = str(json.loads(msg.value())["event_id"])
                except (ValueError, KeyError, TypeError):
                    continue
                out.append((event_id, event_type.decode() if event_type else None))
        return out
    finally:
        kafka.close()


def measure(h: Harness, cell: Cell | None, baseline: Mapping[str, Any] | None) -> dict[str, Any]:
    result = h.result(h.elapsed)
    h.catch_up_log()
    projectors = build_all()
    raw = {
        "publications": topic_publications(h),
        "log_rows_max": _scalar(h, "SELECT coalesce(max(n), 0) AS n FROM"
                                   " (SELECT count(*) AS n FROM event_log.events GROUP BY event_id) x"),
        "missing_in_log": log_counts(h)["missing_in_log"],
        "pending": pending_outbox(h),
        "plan_created_max": _scalar(h, "SELECT coalesce(max(n), 0) AS n FROM (SELECT count(*) AS n FROM"
                                       " event_log.events WHERE event_type = 'PlanCreated' GROUP BY correlation_id) x"),
        # Salidas de la saga repetidas: comandos con el mismo tipo, incidencia y payload, y eventos del orquestador con
        # el mismo tipo, incidencia y payload. Se comparan con `baseline` (donde puede haber repeticiones legítimas).
        "saga_repeats": {
            "commands": _scalar(h, "SELECT coalesce(sum(n - 1), 0) AS n FROM (SELECT count(*) AS n FROM"
                                   " orchestrator.command_outbox GROUP BY command_type, incident_id, payload) x"),
            "events": _scalar(h, "SELECT coalesce(sum(n - 1), 0) AS n FROM (SELECT count(*) AS n FROM"
                                 " event_log.events WHERE producer = 'orchestrator'"
                                 " GROUP BY event_type, correlation_id, payload) x"),
        },
    }
    executions = {action: dict(getattr(h.services, attr).executions) for action, attr in SERVICE_OF.items()}
    return summarize(
        cell, result=result, records=list(h.crash_points.records),
        baseline_units=0 if baseline is None or cell is None else baseline["units"].get(cell, 0),
        executions=executions, projectors_attending=lambda t: sum(p.attends(t) for p in projectors),
        outcomes=Counter(h.outcomes), deliveries=list(h.commands.deliveries), llm_calls=h.llm_client_calls,
        baseline_llm={} if baseline is None else baseline["llm_calls"],
        invariants=invariants.check(h), **raw)


def run_once(base: Settings, *, seed: int, incidents: int, crash_at: Mapping[Cell, int],
             baseline: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Una ejecución (`baseline` sin `crash_at`): sus métricas, las unidades por celda y las llamadas al LLM."""
    faults = FaultPlan(seed=seed, fail_probability={"reserve_stock": FAIL_RESERVE}, crash_at=dict(crash_at))
    config = RunConfig(seed=seed, incidents=incidents, faults=faults, job_lease_seconds=JOB_LEASE_S)
    cell = next(iter(crash_at), None)
    with Harness(base, config) as h:
        h.send_notices()
        h.elapsed = h.run_until_closed()
        metrics = measure(h, cell, baseline)
        return {"metrics": metrics, "units": dict(h.crash_points.units), "llm_calls": h.llm_client_calls}


def _cells(selected: Iterable[str | Cell] | None) -> list[Cell]:
    if selected is None:
        return _matrix()
    names = [s if isinstance(s, str) else cell_name(s) for s in selected]
    return parse_cells(names)


def run_e8(base: Settings, *, seed: int = 42, incidents: int = 20,
           cells: Iterable[str | Cell] | None = None) -> Report:
    """E8: `baseline` y una ejecución por celda (todas, o las de `cells`, por nombre `<componente>@<punto>`)."""
    selected = _cells(cells)
    baseline = run_once(base, seed=seed, incidents=incidents, crash_at={})
    results: dict[str, dict[str, Any]] = {}
    for cell in selected:
        k = crash_unit(baseline["units"].get(cell, 0))
        results[cell_name(cell)] = (not_provoked(cell) if k is None else
                                    run_once(base, seed=seed, incidents=incidents, crash_at={cell: k},
                                             baseline=baseline)["metrics"])
    return Report(
        experiment="E8", seed=seed, model=Oracle.model,
        config={"incidents": incidents, "fail_probability": {"reserve_stock": FAIL_RESERVE},
                "job_lease_seconds": JOB_LEASE_S, "cells": [cell_name(c) for c in selected]},
        metrics={"baseline": {**baseline["metrics"],
                              "units": {cell_name(c): n for c, n in baseline["units"].items()}},
                 "cells": results, "complete": len(results) == len(_matrix()),
                 "cells_ok": {name: m["ok"] for name, m in results.items()}},
        hypothesis={"H8": h8(results)},
    )
