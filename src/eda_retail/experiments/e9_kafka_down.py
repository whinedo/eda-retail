"""E9: caída de Kafka (spec experiments, «E9 Caída de Kafka (H9)»; spec synthetic-data, «Caída del broker»).

50 incidencias en proceso. Cuando la mitad tienen un `PlanCreated` en el log, se corta el broker durante 30 s
(`synthetic.broker_faults.BrokerOutage`) y en ese tiempo llegan 20 avisos más; después el broker vuelve y se ejecuta
hasta cerrar todas las incidencias. Tres variantes:

- `baseline`: sin corte; los 20 avisos llegan en el mismo momento.
- `stopped`: `docker stop` y `docker start` del contenedor de Kafka (conexión rechazada).
- `paused`: `docker pause` y `docker unpause` (conexiones abiertas sin respuesta).

El turno del harness tolera los lotes que el broker no confirma (`RunConfig.tolerate_relay_errors`, con un plazo de
entrega de 1 s): las filas quedan pendientes y se publican al volver. Un lote sin confirmar puede llegar al broker
igualmente (el productor lo reenvía) y el relay lo vuelve a publicar: los mensajes repetidos son los mensajes del topic
menos las filas de `event_log`, que tiene un `event_id` único. Las métricas salen del outbox de cada componente, de
`event_log.events`, del topic y de los contadores del harness. H9: en todas las variantes, ninguna fila de outbox
pendiente al terminar, cada evento del outbox en `event_log`, todas las incidencias cerradas (también las de los avisos
del corte), ninguna fila publicada durante el corte, I1 a I5 sin violaciones y cada variante con corte provoca su
efecto. El corte lo termina un temporizador de `BrokerOutage` (`duration_s`): un turno bloqueado en una llamada a Kafka
no lo alarga. Los consumidores en proceso usan un `session.timeout.ms` de 6 s, el mínimo del broker: sin broker, un
commit síncrono espera un coordinador hasta ese plazo, y un consumidor recreado espera a que caduque el anterior.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable, Mapping
from typing import Any

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.harness import PUBLISHERS, Harness, RunConfig
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.report import Report, summary
from eda_retail.infra.schemas import qualified
from eda_retail.synthetic.broker_faults import BrokerOutage, broker_ready
from eda_retail.synthetic.generator import generate

VARIANTS: dict[str, str | None] = {"baseline": None, "stopped": "stopped", "paused": "paused"}
OUTAGE_S = 30.0
EXTRA_NOTICES = 20
RELAY_FLUSH_TIMEOUT_S = 1.0
PLANNED_TIMEOUT_S = 180.0  # espera máxima a que la mitad de las incidencias tengan plan
SESSION_TIMEOUT_MS = 6000  # mínimo del broker: lo que un commit espera sin coordinador (por defecto, 45 s)
# Rebalance timeout del grupo: tras perder sus particiones con el broker congelado, el `JoinGroup` de un consumidor
# puede quedar retenido hasta este plazo (por defecto, 300 s: E9 se daba por quieto con avisos sin cerrar). Mayor que
# la sesión y que el plazo de un commit (`COMMIT_TIMEOUT_S`, 10 s) y menor que `IDLE_SECONDS`.
MAX_POLL_INTERVAL_MS = 15000
# Depuración del grupo en librdkafka: con `paused`, un consumidor pierde sus particiones y no vuelve al grupo (L16-L19);
# sus últimas líneas de `cgrp` salen en `stall` para ver en qué estado se queda.
KAFKA_DEBUG = "cgrp"
IDLE_SECONDS = 30.0  # sin avance: por encima del timeout de sesión, la reconexión y la recomposición del grupo
CUT_SLACK_S = 60.0  # margen para salir del bucle del corte si un turno queda bloqueado
SAMPLE_EVERY_S = 0.2  # muestreo del outbox pendiente


def pending_outbox(h: Harness) -> dict[str, int]:
    """Filas de outbox sin publicar, por componente."""
    rows = h.conn.execute(" UNION ALL ".join(
        f"SELECT '{schema}' AS schema, count(*) AS n FROM {qualified(schema, 'outbox')} WHERE published_at IS NULL"
        for schema in PUBLISHERS)).fetchall()
    h.conn.commit()
    return {r["schema"]: r["n"] for r in rows}


def log_counts(h: Harness) -> dict[str, int]:
    """Filas de outbox, filas de `event_log` y eventos del outbox que no están en `event_log`."""
    outbox = " UNION ALL ".join(f"SELECT event_id FROM {qualified(schema, 'outbox')}" for schema in PUBLISHERS)
    row = h.conn.execute(
        f"WITH o AS ({outbox}) SELECT (SELECT count(*) FROM o) AS outbox_rows,"
        " (SELECT count(*) FROM event_log.events) AS log_rows,"
        " (SELECT count(*) FROM o WHERE NOT EXISTS (SELECT 1 FROM event_log.events e WHERE e.event_id = o.event_id))"
        " AS missing_in_log").fetchone()
    h.conn.commit()
    return dict(row)


def published(h: Harness) -> int:
    """Filas de outbox ya publicadas, de todos los componentes."""
    row = h.conn.execute("SELECT " + " + ".join(
        f"(SELECT count(*) FROM {qualified(schema, 'outbox')} WHERE published_at IS NOT NULL)"
        for schema in PUBLISHERS) + " AS n").fetchone()
    h.conn.commit()
    return row["n"]


def planned(h: Harness) -> int:
    row = h.conn.execute(
        "SELECT count(DISTINCT correlation_id) AS n FROM event_log.events WHERE event_type = 'PlanCreated'").fetchone()
    h.conn.commit()
    return row["n"]


class PendingSampler:
    """Máximo de filas pendientes por componente y primer instante con el outbox vacío tras `since`."""

    def __init__(self, h: Harness, clock: Callable[[], float] = time.monotonic) -> None:
        self._h = h
        self._clock = clock
        self._last = float("-inf")
        self.max_pending: Counter[str] = Counter()
        self.since: float | None = None
        self.drained_at: float | None = None

    def __call__(self) -> None:
        now = self._clock()
        if now - self._last < SAMPLE_EVERY_S:
            return
        self._last = now
        pending = pending_outbox(self._h)
        for schema, n in pending.items():
            self.max_pending[schema] = max(self.max_pending[schema], n)
        if self.since is not None and self.drained_at is None and sum(pending.values()) == 0:
            self.drained_at = now

    def force(self) -> None:
        """Una muestra ahora, sin esperar a `SAMPLE_EVERY_S`."""
        self._last = float("-inf")
        self()

    def drain_seconds(self) -> float | None:
        if self.since is None or self.drained_at is None:
            return None
        return round(self.drained_at - self.since, 3)


def provoked(variant: str, metrics: Mapping[str, Any]) -> bool:
    """La variante provocó su efecto: algún error de entrega del relay y alguna fila pendiente durante el corte."""
    if VARIANTS[variant] is None:
        return True
    return sum(metrics["relay_errors"].values()) > 0 and sum(metrics["max_pending_during_outage"].values()) > 0


def h9_variant(variant: str, metrics: Mapping[str, Any]) -> bool:
    return (metrics["open"] == 0 and sum(metrics["pending_at_end"].values()) == 0
            and metrics["missing_in_log"] == 0 and metrics["published_during_outage"] == 0
            and metrics["accepted_during_outage"] == metrics["extra_notices"]
            and provoked(variant, metrics)
            and all(inv["violations"] == 0 for inv in metrics["invariants"].values()))


def diagnostics(h: Harness) -> dict[str, Any]:
    """Lag de cada grupo y estado de cada consumidor al terminar, antes de que `event_log` se ponga al día: si una
    incidencia queda abierta, dice qué consumidor se quedó atrás y si su hilo sigue vivo; `stall`, qué había ocupado
    si la variante llegó a su plazo (`Harness.stall_snapshot`)."""
    try:
        lag: dict[str, Any] = h.lags()
    except Exception as exc:  # noqa: BLE001 — diagnóstico: no debe tumbar el experimento
        lag = {"error": repr(exc)}
    return {"group_lag": lag, "consumers": h.consumer_state(), "stall": getattr(h, "stall", None)}


def run_variant(base: Settings, variant: str, *, seed: int, incidents: int, extra: int = EXTRA_NOTICES,
                outage_s: float = OUTAGE_S, outage: Callable[..., Any] = BrokerOutage,
                run_timeout_s: float | None = None) -> dict[str, Any]:
    total = incidents + extra
    data = generate(seed, total)
    mode = VARIANTS[variant]
    config = RunConfig(seed=seed, incidents=total, tolerate_relay_errors=True,
                       relay_flush_timeout_seconds=RELAY_FLUSH_TIMEOUT_S, consumer_session_timeout_ms=SESSION_TIMEOUT_MS,
                       consumer_max_poll_interval_ms=MAX_POLL_INTERVAL_MS, consumer_kafka_debug=KAFKA_DEBUG,
                       idle_seconds=IDLE_SECONDS,
                       **({} if run_timeout_s is None else {"timeout_seconds": run_timeout_s}))
    with Harness(base, config, data=data, llm=Oracle(data).client()) as h:
        sampler = PendingSampler(h)
        h.send_notices(range(incidents))
        h.run_while(lambda: planned(h) < incidents // 2, PLANNED_TIMEOUT_S)
        planned_at_cut = planned(h)
        late = range(incidents, total)
        if mode is None:
            accepted = sum(h._send_notice(data.notices[i], check=False).status_code == 202 for i in late)
            during: dict[str, int] = {}
            published_during = 0
            cut: dict[str, Any] = {"mode": None}
        else:
            ready = broker_ready(h.settings.kafka_bootstrap_servers, h.settings.events_topic)
            with outage(mode, ready=ready, duration_s=outage_s) as o:
                at_cut = last = published(h)
                accepted = sum(h._send_notice(data.notices[i], check=False).status_code == 202 for i in late)

                def during_cut() -> None:
                    nonlocal last
                    sampler()
                    if not o.restoring:  # el temporizador devuelve el broker: hasta entonces, nada se publica
                        last = published(h)

                h.run_while(lambda: not o.restored, outage_s + CUT_SLACK_S, on_turn=during_cut)
                during = dict(sampler.max_pending)
                published_during = last - at_cut
            cut = o.report()
            sampler.since = time.monotonic()
        elapsed = h.run_until_closed(on_turn=sampler)
        sampler.force()  # `_settle` no muestrea: una muestra final para el tiempo de vaciado
        result = h.result(elapsed)
        diagnosis = diagnostics(h)
        h.catch_up_log()
        counts = log_counts(h)
        messages = h.messages()
        metrics: dict[str, Any] = {
            "closed": result.closed, "open": result.open, "resolved": result.resolved,
            "escalated": result.escalated, "escalation_reasons": result.escalation_reasons,
            "outage": cut, "planned_at_cut": planned_at_cut, "extra_notices": extra,
            "accepted_during_outage": accepted,
            "max_pending_during_outage": during, "published_during_outage": published_during,
            "pending_at_end": pending_outbox(h),
            "relay_errors": dict(h.relay_errors), "commit_failures": h.commit_failures,
            "max_relay_turn_seconds": round(h.relay_max_seconds, 3),
            "drain_seconds": sampler.drain_seconds(),
            "messages": messages, **counts, "repeated_messages": messages - counts["log_rows"],
            "inbox_duplicates": {name: n for (name, outcome), n in h.outcomes.items() if outcome == "duplicate"},
            "latency_ms": summary(result.latencies_ms),
            **diagnosis,
        }
        metrics["invariants"] = invariants.check(h)
    metrics["h9"] = h9_variant(variant, metrics)
    return metrics


def p95_ratio(metrics: Mapping[str, Any], baseline: Mapping[str, Any]) -> float | None:
    base = baseline["latency_ms"].get("p95")
    value = metrics["latency_ms"].get("p95")
    return round(value / base, 3) if base and value is not None else None


def run_e9(base: Settings, *, seed: int = 42, incidents: int = 50, extra: int = EXTRA_NOTICES,
           outage_s: float = OUTAGE_S, run_timeout_s: float | None = None) -> Report:
    """`run_timeout_s`: plazo total de `run_until_closed` por variante; None, el de `RunConfig` (300 s)."""
    results = {v: run_variant(base, v, seed=seed, incidents=incidents, extra=extra, outage_s=outage_s,
                              run_timeout_s=run_timeout_s)
               for v in VARIANTS}
    for metrics in results.values():
        metrics["p95_vs_baseline"] = p95_ratio(metrics, results["baseline"])
    holds = all(m["h9"] and invariants.gate(m["invariants"]) for m in results.values())
    return Report(
        experiment="E9", seed=seed, model=Oracle.model,
        config={"incidents": incidents, "extra_notices": extra, "mode": "in_process", "outage_s": outage_s,
                "relay_flush_timeout_s": RELAY_FLUSH_TIMEOUT_S, "consumer_session_timeout_ms": SESSION_TIMEOUT_MS,
                "consumer_max_poll_interval_ms": MAX_POLL_INTERVAL_MS, "consumer_kafka_debug": KAFKA_DEBUG,
                "variants": {k: v or "none" for k, v in VARIANTS.items()}},
        metrics={"variants": results},
        hypothesis={"H9": holds},
    )
