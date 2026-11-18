"""E17: proyección atrasada y avisos aparcados (spec experiments, «E17 Proyección atrasada y avisos aparcados
(H17)»; spec synthetic-data, «Parada de proyectores»).

Tres variantes, cada una con su base y su topic, con los proyectores parados al llegar los avisos y el plazo
de los aparcados acortado:

- (a) `order_registered`: los pedidos se registran después de aparcar los avisos; el intérprete los reanuda
  al consumir cada `OrderRegistered` de Kafka, aunque los proyectores sigan parados. Aquí se reenvía también
  un aviso aparcado («Aviso aparcado y repetido»).
- (b) `expired`: los pedidos se registran antes de los avisos (el intérprete consume su `OrderRegistered`
  antes de aparcarlos) y los proyectores siguen parados al vencer el plazo: escalan con `no_valid_orders`.
- (c) `projected`: como (b), pero los proyectores se reanudan antes del plazo: al vencer, la vista ya tiene
  el pedido y el intérprete publica `ShipmentDelayed`.

Las cuentas salen del log (`event_log` sigue consumiendo con los proyectores parados): un `ShipmentDelayed`
de un aviso aparcado cuya causa es un `OrderRegistered` es una reanudación por el pedido; si no, por el plazo.
"""

from __future__ import annotations

import statistics
import time
from collections.abc import Callable, Mapping
from typing import Any

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.experiments.report import Report
from eda_retail.interpreter.handler import DUPLICATE_NOTICE, NO_VALID_ORDERS

PENDING_TIMEOUT_S = 30.0  # `PENDING_NOTICE_TIMEOUT_S` acortado (spec E17)
ORDER_REGISTERED = "order_registered"
EXPIRED = "expired"
PROJECTED = "projected"
VARIANTS = (ORDER_REGISTERED, EXPIRED, PROJECTED)
INTERPRETER = "interpreter"


def _count(h: Harness, sql: str, params: tuple = ()) -> int:
    n = h.conn.execute(sql, params).fetchone()["n"]
    h.conn.commit()
    return int(n)


def _until(h: Harness, done: Callable[[], bool], timeout: float) -> bool:
    """Turnos hasta que `done()` o `timeout` segundos. True si se cumplió."""
    end = time.monotonic() + timeout
    while not done():
        if time.monotonic() > end:
            return False
        if not h.step():
            time.sleep(0.01)
    return True


def _pending(h: Harness) -> int:
    return _count(h, "SELECT count(*) AS n FROM interpreter.pending_notices")


def _interpreted(h: Harness) -> int:
    return _count(h, "SELECT count(*) AS n FROM interpreter.interpretations")


def _orders_registered(h: Harness) -> int:
    return _count(h, "SELECT count(*) AS n FROM event_log.events WHERE event_type = 'OrderRegistered'")


def outcomes(h: Harness) -> dict[str, Any]:
    """Avisos aparcados y cómo se resolvió cada uno, según el log, y lo que queda aparcado."""
    rows = h.conn.execute(
        """
        WITH parked AS (
            SELECT correlation_id, min(occurred_at) AS parked_at, count(*) AS times
            FROM event_log.events WHERE event_type = 'NoticeParked' GROUP BY correlation_id
        ), orders AS (
            SELECT event_id FROM event_log.events WHERE event_type = 'OrderRegistered'
        )
        SELECT p.correlation_id, p.times,
               extract(epoch FROM r.occurred_at - p.parked_at) AS seconds, r.event_type,
               r.payload ->> 'reason' AS reason, r.causation_id IN (SELECT event_id FROM orders) AS by_order
        FROM parked p
        LEFT JOIN LATERAL (
            SELECT * FROM event_log.events e
            WHERE e.correlation_id = p.correlation_id AND e.producer = 'interpreter'
              AND e.event_type IN ('ShipmentDelayed', 'IncidentEscalated')
            ORDER BY e.seq LIMIT 1
        ) r ON true
        """
    ).fetchall()
    h.conn.commit()
    resumed = [r for r in rows if r["event_type"] == "ShipmentDelayed"]
    seconds = [float(r["seconds"]) for r in rows if r["seconds"] is not None]
    return {
        "parked": len(rows),
        "parked_twice": sum(1 for r in rows if r["times"] > 1),
        "resumed_by_order": sum(1 for r in resumed if r["by_order"]),
        "resumed_by_deadline": sum(1 for r in resumed if not r["by_order"]),
        "escalated": sum(1 for r in rows if r["event_type"] == "IncidentEscalated" and r["reason"] == NO_VALID_ORDERS),
        "parked_seconds": {"mean": statistics.fmean(seconds) if seconds else None,
                           "max": max(seconds) if seconds else None},
        "still_parked": _pending(h)
        + _count(h, "SELECT count(*) AS n FROM views.view_parked_notices"),
    }


def run_variant(base: Settings, variant: str, *, seed: int, incidents: int, timeout: float) -> dict[str, Any]:
    config = RunConfig(seed=seed, incidents=incidents, seed_orders=False, pending_notice_timeout_seconds=timeout)
    wait = timeout * 3 + config.idle_seconds
    observed: dict[str, Any] = {}
    with Harness(base, config) as h:
        orders = len(h.data.orders)
        h.pause_projectors()
        if variant != ORDER_REGISTERED:
            h.seed_orders(wait=False)  # el intérprete consume los OrderRegistered antes de aparcar los avisos
            _until(h, lambda: _orders_registered(h) >= orders and h.lags()[INTERPRETER] == 0, wait)
        h.send_notices()
        observed["all_interpreted"] = _until(h, lambda: _interpreted(h) >= incidents, wait)
        observed["parked_before_resolution"] = _pending(h)
        observed["interpreter_lag_while_parked"] = h.lags()[INTERPRETER]
        if variant == ORDER_REGISTERED:
            observed["duplicate_reason"] = h.resend_notice(0).body.get("reason")
            h.seed_orders(wait=False)
        elif variant == PROJECTED:
            observed["orders_projected_while_paused"] = h.projected_orders()
            h.resume_projectors()
            _until(h, lambda: h.projected_orders() >= orders, wait)
        observed["resolved"] = _until(h, lambda: _pending(h) == 0, wait)
        if "orders_projected_while_paused" not in observed:
            observed["orders_projected_while_paused"] = h.projected_orders()
        h.resume_projectors()
        elapsed = h.run_until_closed()
        observed["stall"] = h.stall  # qué había ocupado si la variante llegó a su plazo (`Harness.stall_snapshot`)
        result = h.result(elapsed)
        observed |= outcomes(h)
        observed["invariants"] = invariants.check(h)
    return {"model": result.model, "closed": result.closed, "open": result.open, **observed}


def variant_holds(variant: str, m: Mapping[str, Any], incidents: int) -> bool:
    common = (m["parked"] == incidents and m["parked_twice"] == 0 and m["still_parked"] == 0
              and m["interpreter_lag_while_parked"] == 0 and invariants.gate(m["invariants"]))
    if variant == ORDER_REGISTERED:
        return (common and m["resumed_by_order"] == incidents and m["escalated"] == 0
                and m["duplicate_reason"] == DUPLICATE_NOTICE)
    if variant == EXPIRED:
        return common and m["escalated"] == incidents and m["orders_projected_while_paused"] == 0
    return common and m["resumed_by_deadline"] == incidents and m["escalated"] == 0


def h17(metrics: Mapping[str, Any], incidents: int) -> bool:
    return all(variant_holds(v, metrics["variants"][v], incidents) for v in VARIANTS)


def run_e17(base: Settings, *, seed: int = 42, incidents: int = 50, timeout: float = PENDING_TIMEOUT_S) -> Report:
    variants = {v: run_variant(base, v, seed=seed, incidents=incidents, timeout=timeout) for v in VARIANTS}
    metrics = {"variants": variants,
               "holds": {v: variant_holds(v, m, incidents) for v, m in variants.items()}}
    return Report(
        experiment="E17", seed=seed, model=variants[ORDER_REGISTERED]["model"],
        config={"incidents": incidents, "pending_notice_timeout_s": timeout, "variants": list(VARIANTS)},
        metrics=metrics, hypothesis={"H17": h17(metrics, incidents)},
    )
