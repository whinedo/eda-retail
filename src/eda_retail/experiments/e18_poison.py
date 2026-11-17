"""E18: mensajes veneno en mitad de la partición (spec experiments, «E18 Mensajes veneno en mitad de la
partición (H18)»; spec synthetic-data, «Mensajes veneno inyectados»).

Se envían los avisos, se avanza hasta que haya incidencias abiertas en el orquestador y entonces se
publica un veneno de cada tipo directamente en el topic, repartidos por las particiones con carga. Los
de tipo legible llevan el `correlation_id` de una incidencia abierta. Después se ejecuta hasta cerrar las
incidencias y hasta que el lag de todos los grupos sea 0.

Los consumidores de un veneno son los que atienden su `event_type` (spec resilience, «Dead letter
queue»): todos para el JSON inválido; `event_log`, el orquestador y los proyectores con handler para
`ShipmentDelayed` en los demás. El `event_type` desconocido no es veneno para nadie: no debe llegar a
ninguna dead letter y `event_log` lo guarda. La reconciliación compara lo inyectado con las filas de
cada `<componente>.dead_letters`; `published_at` dice que el relay lo llevó a la DLQ.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.experiments.report import Report
from eda_retail.infra.dead_letters import INVALID_SCHEMA
from eda_retail.infra.kafka import make_producer
from eda_retail.infra.schemas import qualified
from eda_retail.synthetic import poison
from eda_retail.synthetic.poison import InjectedPoison

Consumer = tuple[str, str, Callable[[str | None], bool]]  # (nombre, esquema, si atiende un event_type)
WARMUP_SECONDS = 120.0  # espera máxima a que haya incidencias abiertas a las que apuntar los venenos


def expected(item: InjectedPoison, attends: Callable[[str | None], bool]) -> bool:
    """True si el consumidor debe llevar este veneno a su dead letter."""
    return item.poison.kind != poison.UNKNOWN_TYPE and attends(item.poison.event_type)


def _matches(item: InjectedPoison, row: Mapping[str, Any]) -> bool:
    if item.poison.event_id is not None:
        return row["event_id"] == str(item.poison.event_id)
    return row["event_id"] is None and row["raw_message"] == poison.raw_dead_letter(item.poison)


def reconcile(injected: Sequence[InjectedPoison], consumers: Sequence[Consumer], rows: Iterable[Mapping[str, Any]],
              *, retries: Mapping[tuple[int, int], Mapping[str, int]], in_log: set[str],
              statuses: Mapping[str, tuple[str, str | None]]) -> dict[str, Any]:
    """Por veneno y por consumidor: si debía y si llegó a su dead letter y a la DLQ, el motivo y los reintentos.

    `rows`: las filas de todas las dead letters (`consumer`, `event_id` como texto, `reason`, `raw_message`,
    `published`); `in_log`: los `event_id` que hay en `event_log.events`; `statuses`: estado y motivo de cada
    incidencia en la vista."""
    rows = list(rows)
    matched: set[int] = set()
    per_poison: dict[str, Any] = {}
    for item in injected:
        p = item.poison
        mine = [(i, r) for i, r in enumerate(rows) if _matches(item, r)]
        matched.update(i for i, _ in mine)
        tried = retries.get((item.partition, item.offset), {})
        by_consumer = {}
        for name, _schema, attends in consumers:
            own = [r for _, r in mine if r["consumer"] == name]
            want = expected(item, attends)
            by_consumer[name] = {
                "expected": want, "dead_letters": len(own), "published": bool(own) and all(r["published"] for r in own),
                "reasons": sorted({r["reason"] for r in own}), "retries": tried.get(name, 0),
                "ok": (len(own) == 1 and own[0]["reason"] == INVALID_SCHEMA and own[0]["published"]) if want
                else not own,
            }
        logged = p.event_id is not None and str(p.event_id) in in_log
        status, reason = statuses.get(p.correlation_id or "", (None, None))
        per_poison[p.kind] = {
            "partition": item.partition, "offset": item.offset, "event_type": p.event_type,
            "correlation_id": p.correlation_id, "consumers": by_consumer, "in_log": logged,
            # Solo se puede escalar una incidencia abierta: si se cerró antes de leer el veneno, queda solo en la DLQ.
            "escalated": (status == "escalated" and reason == INVALID_SCHEMA
                          if p.correlation_id and p.kind != poison.UNKNOWN_TYPE else None),
            "ok": all(c["ok"] for c in by_consumer.values()) and logged == (p.kind == poison.UNKNOWN_TYPE),
        }
    unexpected = [r for i, r in enumerate(rows) if i not in matched]
    return {"poisons": per_poison, "unexpected_dead_letters": len(unexpected),
            "unexpected": [{"consumer": r["consumer"], "reason": r["reason"], "event_id": r["event_id"]}
                           for r in unexpected[:10]]}


def h18(metrics: Mapping[str, Any]) -> bool:
    """Lag 0 en todos los grupos, cada veneno en la DLQ de cada consumidor que atiende su tipo y en ninguna
    más, el de tipo desconocido en el log, ninguna otra dead letter y todas las incidencias cerradas (I1)."""
    return (all(n == 0 for n in metrics["lag"].values()) and metrics["unexpected_dead_letters"] == 0
            and all(p["ok"] for p in metrics["poisons"].values()) and metrics["open"] == 0)


def _open_incidents(h: Harness) -> list[str]:
    rows = h.conn.execute("SELECT incident_id FROM orchestrator.incidents WHERE status = 'open' ORDER BY 1").fetchall()
    h.conn.commit()
    return [r["incident_id"] for r in rows]


def _warm_up(h: Harness, needed: int) -> list[str]:
    """Turnos hasta tener `needed` incidencias abiertas en el orquestador (carga en las particiones), o
    `WARMUP_SECONDS`; hace falta al menos una."""
    start = time.monotonic()
    while len(open_ := _open_incidents(h)) < needed and time.monotonic() - start < WARMUP_SECONDS:
        if not h.step():
            time.sleep(0.01)
    if not open_:
        raise RuntimeError("E18: ninguna incidencia abierta a la que apuntar los venenos")
    return open_


def pick_targets(seed: int, open_incidents: Sequence[str], needed: int) -> list[str]:
    """`needed` incidencias abiertas al azar (con la semilla); si hay menos, se repiten en orden."""
    chosen = random.Random(seed).sample(list(open_incidents), min(needed, len(open_incidents)))
    return [chosen[i % len(chosen)] for i in range(needed)]


def _dead_letter_rows(h: Harness, consumers: Sequence[Consumer]) -> list[dict[str, Any]]:
    rows = []
    for schema in sorted({schema for _, schema, _ in consumers}):
        rows += [dict(r) for r in h.conn.execute(
            f"SELECT consumer, event_id::text AS event_id, reason, raw_message, published_at IS NOT NULL AS published"
            f" FROM {qualified(schema, 'dead_letters')} ORDER BY id").fetchall()]
    h.conn.commit()
    return rows


def run_e18(base: Settings, *, seed: int = 42, incidents: int = 100) -> Report:
    config = RunConfig(seed=seed, incidents=incidents)
    with Harness(base, config) as h:
        h.send_notices()
        needed = len(poison.KINDS) - 1  # todos menos el JSON inválido apuntan a una incidencia
        targets = pick_targets(seed, _warm_up(h, needed), needed)
        producer = make_producer(h.settings.kafka_bootstrap_servers)
        injected = poison.inject(producer, h.settings.events_topic, poison.poison_set(seed, targets), h.partitions())
        elapsed = h.run_until_closed()
        elapsed += h.run_until_caught_up()
        result = h.result(elapsed)
        consumers = h.dead_letter_consumers()
        rows = _dead_letter_rows(h, consumers)
        ids = [str(i.poison.event_id) for i in injected if i.poison.event_id is not None]
        in_log = {r["id"] for r in h.conn.execute(
            "SELECT event_id::text AS id FROM event_log.events WHERE event_id::text = ANY(%s)", (ids,)).fetchall()}
        statuses = {r["incident_id"]: (r["status"], r["escalation_reason"]) for r in h.conn.execute(
            "SELECT incident_id, status, escalation_reason FROM views.view_incident_status WHERE incident_id = ANY(%s)",
            (targets,)).fetchall()}
        h.conn.commit()
        retries = {(i.partition, i.offset): h.retries(i.partition, i.offset) for i in injected}
        lags, dlq = h.lags(), h.dlq_messages()
        checked = invariants.check(h)
    metrics = {
        "closed": result.closed, "open": result.open, "resolved": result.resolved, "escalated": result.escalated,
        "lag": lags, "dlq_messages": dlq, "dead_letter_rows": len(rows),
        **reconcile(injected, consumers, rows, retries=retries, in_log=in_log, statuses=statuses),
        "invariants": checked,
    }
    return Report(
        experiment="E18", seed=seed, model=result.model,
        config={"incidents": incidents, "poisons": list(poison.KINDS), "poison_event_type": poison.POISON_TYPE,
                "large_payload_bytes": poison.LARGE_PAYLOAD_BYTES},
        metrics=metrics, hypothesis={"H18": h18(metrics) and invariants.gate(checked)},
    )
