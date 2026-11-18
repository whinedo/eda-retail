"""E10: caída de PostgreSQL (spec experiments, «E10 Caída de PostgreSQL (H10)»; spec synthetic-data, «Caída de la base
de datos»).

Con todos los componentes en procesos propios (`RunConfig.component_processes`, los de `scripts/run-agents.sh`), 50
incidencias. Cuando la mitad tienen un `PlanCreated` en el log, se corta el contenedor de PostgreSQL durante 30 s
(`synthetic.pg_faults.PostgresOutage`, con su temporizador) y en ese tiempo llegan 20 avisos más por REST: fallan con 5xx
o sin respuesta y quien los envía los reintenta, con el mismo `command_id`, hasta que se aceptan. Después se ejecuta
hasta cerrar todas las incidencias. Tres variantes:

- `baseline`: sin corte; los 20 avisos llegan en el mismo momento.
- `stopped`: `docker stop` y `docker start` (las conexiones se cortan y las transacciones abiertas se deshacen).
- `paused`: `docker pause` y `docker unpause` (las conexiones quedan abiertas sin respuesta).

El harness no lee la base durante el corte: solo envía los avisos, sondea la base con una conexión nueva
(`postgres_ready`, que cuenta los sondeos fallidos) y mira si algún proceso murió. Al volver la base, cambia su conexión
si el corte la rompió (`Harness.reconnect`) y cada muestra posterior tolera un error de la base reconectándose.

Las métricas de cada proceso salen de su log (nivel DEBUG): las líneas de reconexión de la spec resilience,
«Reconexión a PostgreSQL» (`CONNECTION_ERROR_MARKERS`), los mensajes que el proceso vuelve a atender tras «se reintenta
el mensaje» (reintentos en el proceso), los que vuelve a atender sin esa línea (Kafka los volvió a entregar porque su
offset no se confirmó), los descartes del inbox (`<- ...: duplicate`) y los reintentos de comandos del dispatcher. Los
procesos se paran al final y su código de salida dice si alguno quedó colgado (lo mata `wait_stopped`, código distinto
de 0). `event_log` se pone al día con los procesos en marcha; las invariantes, ya parados (I5 reconstruye las vistas).
"""

from __future__ import annotations

import contextlib
import re
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from typing import Any

from eda_retail.config import Settings
from eda_retail.experiments import invariants
from eda_retail.experiments.e9_kafka_down import log_counts, pending_outbox, planned
from eda_retail.experiments.harness import Harness, RunConfig
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.report import Report, summary
from eda_retail.infra.command_client import CommandUnavailable
from eda_retail.infra.commands import ACCEPTED
from eda_retail.synthetic.pg_faults import PostgresOutage, postgres_ready

VARIANTS: dict[str, str | None] = {"baseline": None, "stopped": "stopped", "paused": "paused"}
OUTAGE_S = 30.0
EXTRA_NOTICES = 20
PLANNED_TIMEOUT_S = 180.0  # espera máxima a que la mitad de las incidencias tengan plan
IDLE_SECONDS = 60.0  # sin avance: por encima de las esperas de reconexión de los procesos (1 a 2 s por intento)
CUT_SLACK_S = 60.0  # margen para salir del bucle del corte si el temporizador no devolviera la base
PROBE_EVERY_S = 1.0  # sondeo de la base y de los procesos durante el corte
NOTICE_RETRY_S = 1.0  # espera de quien envía un aviso entre dos intentos
NOTICE_RETRY_WINDOW_S = 120.0  # cuánto sigue reintentando un aviso después del corte
SAMPLE_EVERY_S = 1.0  # muestreo del outbox pendiente y del lag tras el corte
DUPLICATE_NOTICE = "duplicate_notice"  # un intento anterior, sin respuesta, llegó a registrar el aviso

# Líneas de reconexión de la spec resilience, «Reconexión a PostgreSQL», en WARNING o ERROR:
# - «la base no responde»: `EventConsumer` (orquestador, intérprete) y `run_resilient` (proyector, event-log);
# - «sin conexión a la base»: los mismos al reintentar la conexión, y el relay de cada proceso;
# - «: error; se reintenta»: el relay de cada proceso (`ComponentRelay.run_until`) y `run_loop` (dispatcher, plazos,
#   avisos aparcados);
# - «worker de trabajos de»: el worker de trabajos largos (planificador, replanificador, ejecutores).
# Todo proceso tiene relay (el proyector y event-log, el de sus dead letters), que consulta la base sin parar.
CONNECTION_ERROR_MARKERS = ("la base no responde", "sin conexión a la base", ": error; se reintenta",
                            "worker de trabajos de")
_LINE = re.compile(r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3} (\w+) \[[^\]]+\] (.*)$")  # infra.logs.FORMAT
_START = re.compile(r"^-> \w+ \S+ p(\d+) o(\d+)$")
_RETRY = re.compile(r"se reintenta el mensaje p(\d+) o(\d+)$")
_END = re.compile(r"^<- \w+ \S+: (\w+)$")
COMMAND_RETRY = "sin respuesta (intento"  # orchestrator.dispatcher


# -- logs de los procesos ---------------------------------------------------------------------------------------
def log_messages(text: str) -> Iterator[tuple[str, str]]:
    """(nivel, mensaje) de cada línea propia del proceso; las de una traza no tienen el formato y se saltan."""
    for line in text.splitlines():
        if m := _LINE.match(line):
            yield m[1], m[2]


def connection_errors(text: str) -> int:
    """Líneas en las que el proceso dice que la base no respondió (`CONNECTION_ERROR_MARKERS`)."""
    return sum(1 for level, message in log_messages(text)
               if level in ("WARNING", "ERROR") and any(marker in message for marker in CONNECTION_ERROR_MARKERS))


def message_retries(text: str) -> dict[str, int]:
    """Mensajes de Kafka que el proceso volvió a atender: tras «se reintenta el mensaje pX oY» (`retried_in_process`)
    o sin esa línea, porque Kafka los volvió a entregar al no estar confirmado su offset (`kafka_redeliveries`); y los
    que descartó el inbox (`inbox_discards`, resultado `duplicate`). Lee las líneas DEBUG `-> tipo incidencia pX oY`."""
    starts: Counter[tuple[str, str]] = Counter()
    retries: Counter[tuple[str, str]] = Counter()
    discards = 0
    for _, message in log_messages(text):
        if m := _START.match(message):
            starts[(m[1], m[2])] += 1
        elif m := _RETRY.search(message):
            retries[(m[1], m[2])] += 1
        elif (m := _END.match(message)) and m[1] == "duplicate":
            discards += 1
    redelivered = sum(max(0, n - 1 - retries[key]) for key, n in starts.items())
    return {"retried_in_process": sum(retries.values()), "kafka_redeliveries": redelivered, "inbox_discards": discards}


def command_retries(text: str) -> int:
    """Comandos que el dispatcher vuelve a intentar porque el destino no respondió."""
    return sum(1 for _, message in log_messages(text) if COMMAND_RETRY in message)


def process_report(processes: Iterable[Any]) -> dict[str, dict[str, int]]:
    """Por proceso (`name`, `log_text()`): errores de conexión, mensajes vueltos a atender, descartes del inbox y
    reintentos de comandos. Leer antes de salir del harness: los logs están en su directorio temporal."""
    out = {}
    for p in processes:
        text = p.log_text()
        out[p.name] = {"connection_errors": connection_errors(text), **message_retries(text),
                       "command_retries": command_retries(text)}
    return out


# -- avisos reintentados ----------------------------------------------------------------------------------------
@dataclass
class NoticeSend:
    attempts: int = 0
    rejected_during_outage: int = 0
    rejected_after_outage: int = 0
    accepted: bool = False
    refused: bool = False  # rechazo de negocio (4xx distinto de un aviso ya registrado): no se reintenta


def send_until_accepted(send: Callable[[], Any], *, cutting: Callable[[], bool], timeout_s: float,
                        retry_s: float = NOTICE_RETRY_S, clock: Callable[[], float] = time.monotonic,
                        sleep: Callable[[float], None] = time.sleep) -> NoticeSend:
    """Envía un aviso hasta que se acepta, como un transportista que reintenta: un 5xx o la falta de respuesta
    (`CommandUnavailable`) se cuenta como rechazo (durante el corte si `cutting()` al empezar el intento) y se
    reintenta a los `retry_s`, hasta `timeout_s`. Un 422 `duplicate_notice` es un aviso ya registrado por un intento
    cuya respuesta se perdió: cuenta como aceptado."""
    out = NoticeSend()
    end = clock() + timeout_s
    while True:
        during = cutting()
        out.attempts += 1
        try:
            reply = send()
        except CommandUnavailable:
            reply = None
        if reply is not None:
            out.accepted = reply.status_code == ACCEPTED or reply.reason == DUPLICATE_NOTICE
            out.refused = not out.accepted
            return out
        if during:
            out.rejected_during_outage += 1
        else:
            out.rejected_after_outage += 1
        if clock() >= end:
            return out
        sleep(retry_s)


def send_notices(h: Harness, indices: Sequence[int], *, cutting: Callable[[], bool], timeout_s: float,
                 retry_s: float = NOTICE_RETRY_S) -> list[NoticeSend]:
    """Los avisos `indices` a la vez, cada uno desde su hilo y con un `command_id` propio que conserva en cada
    reintento (el inbox de intake repite la respuesta de uno que llegó a registrarse)."""
    def one(i: int) -> NoticeSend:
        command_id = uuid.uuid4()
        return send_until_accepted(lambda: h._send_notice(h.data.notices[i], check=False, command_id=command_id),
                                   cutting=cutting, timeout_s=timeout_s, retry_s=retry_s)

    if not indices:
        return []
    with ThreadPoolExecutor(max_workers=len(indices), thread_name_prefix="notice") as pool:
        return list(pool.map(one, indices))


def notice_summary(sends: Iterable[NoticeSend]) -> dict[str, int]:
    sends = list(sends)
    return {"sent": len(sends), "accepted": sum(s.accepted for s in sends),
            "rejected_during_outage": sum(s.rejected_during_outage for s in sends),
            "rejected_after_outage": sum(s.rejected_after_outage for s in sends),
            "retries": sum(max(0, s.attempts - 1) for s in sends), "refused": sum(s.refused for s in sends)}


# -- tras el corte ----------------------------------------------------------------------------------------------
def sample_backlog(h: Harness) -> tuple[dict[str, int], dict[str, int]]:
    """Filas de outbox pendientes por componente y lag de cada grupo y consumidor posicionado."""
    return pending_outbox(h), h.lags()


class DrainSampler:
    """Primer instante, desde `since` (la base de vuelta), con el outbox vacío y lag 0. Una lectura que falla se cuenta
    en `errors` y el harness se reconecta: el muestreo nunca tumba la ejecución."""

    def __init__(self, h: Any, since: float | None, clock: Callable[[], float] = time.monotonic) -> None:
        self._h = h
        self._clock = clock
        self._last = float("-inf")
        self.since = since
        self.drained_at: float | None = None
        self.errors = 0

    def __call__(self) -> None:
        now = self._clock()
        if self.since is None or self.drained_at is not None or now - self._last < SAMPLE_EVERY_S:
            return
        self._last = now
        try:
            pending, lags = sample_backlog(self._h)
        except Exception:  # noqa: BLE001 — la base o Kafka sin responder: se reconecta y se sigue
            self.errors += 1
            with contextlib.suppress(Exception):  # si aún no responde, se vuelve a intentar en la siguiente muestra
                self._h.reconnect()
            return
        if sum(pending.values()) == 0 and sum(lags.values()) == 0:
            self.drained_at = now

    def force(self) -> None:
        self._last = float("-inf")
        self()

    def drain_seconds(self) -> float | None:
        if self.since is None or self.drained_at is None:
            return None
        return round(self.drained_at - self.since, 3)


def executions_summary(executions: Mapping[tuple[str, str], int]) -> dict[str, int]:
    """Claves ejecutadas en los servicios simulados, el máximo de ejecuciones de una clave y las repetidas (debe ser
    1 y 0)."""
    return {"keys": len(executions), "max_per_key": max(executions.values(), default=0),
            "repeated": sum(n - 1 for n in executions.values() if n > 1)}


# -- H10 --------------------------------------------------------------------------------------------------------
def provoked(variant: str, metrics: Mapping[str, Any]) -> bool:
    """La variante provocó su efecto: algún aviso rechazado durante el corte y, con la base parada, al menos un error
    de conexión en cada proceso; con la base congelada, algún sondeo con una conexión nueva sin respuesta (las
    conexiones abiertas esperan sin error: TCP keepalive no detecta una base congelada)."""
    if VARIANTS[variant] is None:
        return True
    if metrics["notices"]["rejected_during_outage"] < 1:
        return False
    if VARIANTS[variant] == "stopped":
        errors = metrics["connection_errors"]
        return bool(errors) and all(n > 0 for n in errors.values())
    return metrics["db_probe_failures_during_outage"] > 0


def h10_variant(variant: str, metrics: Mapping[str, Any]) -> bool:
    exit_codes = metrics["exit_codes"]
    return (not metrics["process_deaths"] and bool(exit_codes) and all(c == 0 for c in exit_codes.values())
            and sum(metrics["pending_at_end"].values()) == 0 and metrics["missing_in_log"] == 0
            and metrics["executions"]["repeated"] == 0
            and metrics["open"] == 0 and metrics["notices"]["accepted"] == metrics["extra_notices"]
            and all(inv["violations"] == 0 for inv in metrics["invariants"].values())
            and provoked(variant, metrics))


def latency_ratios(metrics: Mapping[str, Any], baseline: Mapping[str, Any]) -> dict[str, float | None]:
    """Media, mediana y p95 de la variante divididas por las de `baseline`."""
    out = {}
    for stat in ("mean", "median", "p95"):
        base, value = baseline["latency_ms"].get(stat), metrics["latency_ms"].get(stat)
        out[stat] = round(value / base, 3) if base and value is not None else None
    return out


# -- ejecución --------------------------------------------------------------------------------------------------
def run_variant(base: Settings, variant: str, *, seed: int, incidents: int, extra: int = EXTRA_NOTICES,
                outage_s: float = OUTAGE_S, outage: Callable[..., Any] = PostgresOutage,
                run_timeout_s: float | None = None) -> dict[str, Any]:
    total = incidents + extra
    mode = VARIANTS[variant]
    # Solo opciones con efecto en este modo: los plazos de `run_until_closed` (`RunConfig.component_processes`).
    config = RunConfig(seed=seed, incidents=total, component_processes=True, idle_seconds=IDLE_SECONDS,
                       **({} if run_timeout_s is None else {"timeout_seconds": run_timeout_s}))
    late = list(range(incidents, total))
    notice_timeout_s = outage_s + NOTICE_RETRY_WINDOW_S
    with Harness(replace(base, poc_version=1), config) as h:
        h.send_notices(range(incidents))
        h.run_while(lambda: planned(h) < incidents // 2, PLANNED_TIMEOUT_S)
        planned_at_cut = planned(h)
        probe_failures = 0
        deaths_during: dict[str, int | None] = {}
        reconnected = False
        if mode is None:
            sends = send_notices(h, late, cutting=lambda: False, timeout_s=notice_timeout_s)
            cut: dict[str, Any] = {"mode": None}
            since = time.monotonic()
        else:
            ready = postgres_ready(h.settings.database_url)
            with ThreadPoolExecutor(max_workers=1, thread_name_prefix="carrier") as carrier:
                with outage(mode, ready=ready, duration_s=outage_s) as o:
                    pending_sends = carrier.submit(send_notices, h, late, cutting=lambda: not o.restored,
                                                   timeout_s=notice_timeout_s)
                    end = time.monotonic() + outage_s + CUT_SLACK_S
                    while not o.restored and time.monotonic() < end:  # sin leer la base del harness
                        probe_failures += not ready()
                        deaths_during |= h.process_deaths()
                        time.sleep(PROBE_EVERY_S)
                cut = o.report()
                since = o.ready_at if o.ready_at is not None else time.monotonic()
                reconnected = h.reconnect()
                sends = pending_sends.result(timeout=notice_timeout_s + CUT_SLACK_S)
        sampler = DrainSampler(h, since)
        elapsed = h.run_until_closed(on_turn=sampler)
        sampler.force()
        log_caught_up = h.catch_up_log()  # con los procesos en marcha: después, `event-log` ya no avanza
        counts = log_counts(h)
        pending_at_end = pending_outbox(h)
        deaths = deaths_during | h.process_deaths()
        processes = process_report(h.components)  # antes de salir: los logs se borran con el harness
        exit_codes = h.stop_components()
        result = h.result(elapsed)
        executions = h.service_executions()
        metrics: dict[str, Any] = {
            "closed": result.closed, "open": result.open, "resolved": result.resolved,
            "escalated": result.escalated, "escalation_reasons": result.escalation_reasons,
            "outage": cut, "planned_at_cut": planned_at_cut, "extra_notices": extra,
            "notices": notice_summary(sends), "db_probe_failures_during_outage": probe_failures,
            "harness_reconnected": reconnected,
            "processes": processes,
            "connection_errors": {name: p["connection_errors"] for name, p in processes.items()},
            **{key: sum(p[key] for p in processes.values())
               for key in ("retried_in_process", "kafka_redeliveries", "inbox_discards", "command_retries")},
            "process_deaths": deaths, "exit_codes": exit_codes,
            "drain_seconds": sampler.drain_seconds(), "sample_errors": sampler.errors,
            "pending_at_end": pending_at_end, **counts, "log_caught_up": log_caught_up,
            "executions": executions_summary(executions),
            "latency_ms": summary(result.latencies_ms),
        }
        metrics["invariants"] = invariants.check(h)  # con los procesos parados: I5 reconstruye las vistas
    metrics["h10"] = h10_variant(variant, metrics)
    return metrics


def run_e10(base: Settings, *, seed: int = 42, incidents: int = 50, extra: int = EXTRA_NOTICES,
            outage_s: float = OUTAGE_S, run_timeout_s: float | None = None) -> Report:
    """`run_timeout_s`: plazo total de `run_until_closed` por variante; None, el de `RunConfig` (300 s)."""
    results = {v: run_variant(base, v, seed=seed, incidents=incidents, extra=extra, outage_s=outage_s,
                              run_timeout_s=run_timeout_s)
               for v in VARIANTS}
    for metrics in results.values():
        metrics["latency_vs_baseline"] = latency_ratios(metrics, results["baseline"])
    holds = all(m["h10"] and invariants.gate(m["invariants"]) for m in results.values())
    return Report(
        experiment="E10", seed=seed, model=Oracle.model,
        config={"incidents": incidents, "extra_notices": extra, "mode": "component_processes", "outage_s": outage_s,
                "poc_version": 1, "variants": {k: v or "none" for k, v in VARIANTS.items()}},
        metrics={"variants": results},
        hypothesis={"H10": holds},
    )
