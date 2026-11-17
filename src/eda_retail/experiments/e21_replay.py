"""E21 Reproducción total del log (hipótesis H21, spec experiments).

Cierra las incidencias con un 30 % de fallos en `reserve_stock` (para que haya revisiones y compensaciones).
Después, con el sistema parado, lleva los offsets de todos los grupos al principio de `eda.events`, rebobina el
lado de lectura y arranca otra vez hasta que el lag sea 0 (`Harness.replay`). Con el sistema en marcha, entrega
de nuevo cada `ExecuteStep` ya atendido con su `command_id` original (`Harness.redeliver`): las incidencias están
cerradas y sus plazos vencidos, así que es el «Duplicado tardío» de la spec `idempotency`.

Compara una foto de antes y otra de después: las ejecuciones de cada servicio simulado por `idempotency_key`,
las filas de cada `outbox`, `inbox` y `command_outbox` (y de las reservas, los reembolsos y el log), y la huella
de `orchestrator.plan_steps` y de cada vista. Cuenta además los mensajes que el inbox de cada consumidor
descartó frente a los que tenía registrados.

Observaciones (fuera de H21): reproduce otra vez sin el inbox del orquestador y después sin el del intérprete.
No repiten nada: la saga descarta por estado los eventos de incidencias cerradas y los resultados obsoletos, y el
intérprete deduplica el aviso con el inbox de comandos de `OpenIncident`.

Control: reentrega los `ExecuteStep` antiguos a los ejecutores en el modo `no_effect_keys` de E1 (sin inbox de
comandos ni `effect_keys`, servicios sin deduplicar); deben aparecer efectos repetidos, para demostrar que el
experimento detecta la falta de idempotencia. Va el último porque quita las restricciones de unicidad de negocio.
H21 se cumple si la reproducción con inbox no cambia nada y el control repite algún efecto.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from typing import Any

import psycopg

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.harness import SERVICE_OF, Harness, RunConfig, started
from eda_retail.experiments.report import Report
from eda_retail.infra.consumer import Outcome
from eda_retail.infra.schemas import qualified
from eda_retail.interpreter import consumer as interpreter
from eda_retail.orchestrator import consumer as orchestrator
from eda_retail.projectors.rebuild import fingerprint
from eda_retail.projectors.registry import PROJECTORS
from eda_retail.synthetic.faults import FaultPlan

FAIL_RESERVE = 0.3  # fallos en reserve_stock: revisiones del plan y compensaciones (spec E21)
OBSERVATIONS = (orchestrator.SCHEMA, interpreter.SCHEMA)  # reproducciones sin el inbox de un consumidor
CONTROL = "no_effect_keys"  # modo de E1 de los ejecutores en la reentrega de control
LOG_TABLES = ("outbox", "inbox", "command_outbox")
EFFECT_TABLES = ("inventory.reservations", "executor_issue_refund.refunds", "event_log.events")
PLAN_STEPS = "orchestrator.plan_steps"


@dataclass(frozen=True)
class Snapshot:
    executions: dict[str, int]  # "<servicio>:<idempotency_key>" -> ejecuciones
    rows: dict[str, int]  # "<esquema>.<tabla>" -> filas
    fingerprints: dict[str, str]  # tabla o vista -> huella


def changes(before: Snapshot, after: Snapshot) -> dict[str, Any]:
    """Lo que cambió entre dos fotos: `[antes, después]` por clave y las huellas distintas."""

    def moved(a: dict[str, Any], b: dict[str, Any]) -> dict[str, list[Any]]:
        return {k: [a.get(k), b.get(k)] for k in sorted(a.keys() | b.keys()) if a.get(k) != b.get(k)}

    out = {
        "executions": moved(before.executions, after.executions),
        "rows": moved(before.rows, after.rows),
        "fingerprints": sorted(moved(before.fingerprints, after.fingerprints)),
    }
    return {k: v for k, v in out.items() if v}


def summarize(diff: dict[str, Any]) -> dict[str, Any]:
    """Resumen de un control: ejecuciones nuevas, filas y huellas que cambiaron."""
    extra = sum((after or 0) - (before or 0) for before, after in diff.get("executions", {}).values())
    return {"changed": bool(diff), "new_executions": extra, "rows": diff.get("rows", {}),
            "fingerprints": diff.get("fingerprints", [])}


def h21(metrics: dict[str, Any]) -> bool:
    replay, old = metrics["replay"], metrics["old_commands"]
    return (metrics["open"] == 0 and not replay["changed"] and replay["lag"] == 0 and replay["inbox_matches"]
            and old["not_replayed"] == 0 and metrics["control"]["new_executions"] > 0)


def _table_fingerprint(conn: psycopg.Connection, table: str) -> str:
    digest = hashlib.sha256()
    for row in sorted(json.dumps(dict(r), sort_keys=True, default=str) for r in conn.execute(f"SELECT * FROM {table}")):
        digest.update(row.encode())
    return digest.hexdigest()


def snapshot(h: Harness) -> Snapshot:
    c = h.conn
    c.commit()
    executions = {f"{service}:{key}": n for service in sorted(set(SERVICE_OF.values()))
                  for key, n in getattr(h.services, service).executions.items()}
    tables = [r["t"] for r in c.execute(
        "SELECT table_schema || '.' || table_name AS t FROM information_schema.tables"
        " WHERE table_name = ANY(%s) AND table_type = 'BASE TABLE' ORDER BY 1", (list(LOG_TABLES),)).fetchall()]
    rows = {t: c.execute(f"SELECT count(*) AS n FROM {t}").fetchone()["n"] for t in [*tables, *EFFECT_TABLES]}
    prints = {PLAN_STEPS: _table_fingerprint(c, PLAN_STEPS)}
    c.commit()
    prints |= {view: fingerprint(c, view) for view in PROJECTORS}
    return Snapshot(executions, rows, prints)


def inbox_registered(h: Harness) -> dict[str, int]:
    """Mensajes registrados en el inbox de cada consumidor de Kafka. Todas las filas, no solo `kind = 'event'`:
    el intérprete registra cada aviso como el comando `OpenIncident` con el `event_id` como `command_id`, y
    ni él ni el orquestador atienden otros comandos."""
    out = {name: h.conn.execute(f"SELECT count(*) AS n FROM {qualified(schema, 'inbox')}").fetchone()["n"]
           for name, schema in h.consumers()}
    h.conn.commit()
    return out


def received(before: Counter[tuple[str, str]], after: Counter[tuple[str, str]], outcome: str | None = None
             ) -> dict[str, int]:
    """Mensajes que recibió cada consumidor entre dos recuentos (de un resultado, o de todos)."""
    out: Counter[str] = Counter()
    for (name, result), n in (after - before).items():
        if outcome is None or result == outcome:
            out[name] += n
    return dict(sorted(out.items()))


def run_e21(base: Settings, *, seed: int = 42, incidents: int = 50) -> Report:
    faults = FaultPlan(seed=seed, fail_probability={"reserve_stock": FAIL_RESERVE})
    with started(base, RunConfig(seed=seed, incidents=incidents, faults=faults)) as h:
        result = h.result(h.elapsed)
        replans = h.conn.execute("SELECT count(*) AS n FROM event_log.events WHERE event_type = 'PlanRevised'"
                                 ).fetchone()["n"]
        h.conn.commit()
        before, registered, outcomes, messages = snapshot(h), inbox_registered(h), Counter(h.outcomes), h.messages()
        seconds = h.replay()
        lag = h.lag()
        got = received(outcomes, h.outcomes)
        discarded = {name: received(outcomes, h.outcomes, Outcome.DUPLICATE.value).get(name, 0) for name in registered}
        replies = h.redeliver("ExecuteStep")
        replay_changes = changes(before, snapshot(h))
        checked = invariants.check(h)  # antes de las observaciones y del control, que repiten efectos a propósito
        observations = {}
        for group in OBSERVATIONS:
            prev = snapshot(h)
            h.replay(inbox_off={group})
            observations[group] = summarize(changes(prev, snapshot(h)))
        prev = snapshot(h)
        h.redeliver("ExecuteStep", idempotency=CONTROL)
        control = summarize(changes(prev, snapshot(h)))
    metrics = {
        "closed": result.closed, "open": result.open, "resolved": result.resolved, "escalated": result.escalated,
        "replans": replans,
        "replay": {
            "seconds": seconds, "lag": lag, "messages": messages, "received": got,
            "changed": bool(replay_changes), "changes": replay_changes,
            "inbox_registered": registered, "inbox_discarded": discarded, "inbox_matches": registered == discarded,
        },
        "old_commands": {"redelivered": len(replies), "replayed": sum(r.replayed for r in replies),
                         "not_replayed": sum(not r.replayed for r in replies)},
        "control": control,
        "observations": observations,
        "invariants": checked,
    }
    return Report(
        experiment="E21", seed=seed, model=result.model,
        config={"incidents": incidents, "fail_probability": {"reserve_stock": FAIL_RESERVE},
                "control": f"redeliver:{CONTROL}", "observations": [f"inbox_off:{g}" for g in OBSERVATIONS]},
        metrics=metrics, hypothesis={"H21": h21(metrics) and invariants.gate(checked)},
    )
