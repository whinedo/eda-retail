"""Worker de los comandos con trabajo largo (design.md §4.4; spec event-backbone, «Invocación de comandos»).

La petición de un comando `deferred` solo acepta: deja el trabajo en `<schema>.command_jobs` en la misma
transacción que su inbox y responde 202 (`infra/commands.py`). Este módulo hace el trabajo:

1. Toma un trabajo en una transacción corta (`FOR UPDATE SKIP LOCKED`): uno `pending` cuyo intento toca, o
   uno `running` cuyo lease venció (su worker cayó). Lo marca `running` con un lease nuevo y suma un intento.
2. `prepare` (el LLM, el servicio externo) fuera de toda transacción, bajo el contexto de traza guardado.
3. En una transacción: vuelve a bloquear el trabajo y comprueba que sigue siendo suyo (mismo intento y
   `running`; si otro worker lo tomó tras vencer el lease, no escribe nada); el `handle` del comando, sus
   eventos en el outbox y el trabajo `done` con el resultado. Un `Rejected` (una carrera que la aceptación no
   podía ver) deja el trabajo `rejected` con el motivo, sin eventos: el plazo de la saga lo trata.
4. Si algo falla, la transacción se deshace y el trabajo vuelve a `pending` con espera exponencial. Con
   `max_attempts` (`JOB_MAX_ATTEMPTS`), un trabajo que agota sus intentos, o que se retoma más veces porque su
   worker cae siempre a mitad, queda `failed` con `job_attempts_exhausted`, sin eventos: el plazo de la saga lo
   trata, como un rechazo tardío.

Así el resultado (estado y eventos) se escribe una sola vez aunque el worker caiga a mitad: el trabajo se
retoma al vencer su lease, y el `handle` ya protege los efectos (`effect_keys`, `ON CONFLICT`).
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable, Mapping
from typing import Any

import psycopg
from opentelemetry import context as otel_context
from opentelemetry.trace import SpanKind
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from eda_retail.infra import outbox, tracing
from eda_retail.infra.backoff import Backoff
from eda_retail.infra.commands import Accepted, CommandEnvelope, CommandSpec, check_causation
from eda_retail.infra.schemas import qualified

log = logging.getLogger("eda_retail.jobs")

DONE = "done"
REJECTED = "rejected"
FAILED = "failed"
ATTEMPTS_EXHAUSTED = "job_attempts_exhausted"


def _claim(conn: psycopg.Connection, schema: str, lease_seconds: float) -> dict[str, Any] | None:
    table = qualified(schema, "command_jobs")
    with conn.transaction():
        return conn.execute(
            f"UPDATE {table} SET status = 'running', attempts = attempts + 1,"
            " lease_until = now() + make_interval(secs => %s)"
            f" WHERE job_id = (SELECT job_id FROM {table}"
            "   WHERE (status = 'pending' AND next_attempt_at <= now())"
            "      OR (status = 'running' AND lease_until <= now())"
            "   ORDER BY next_attempt_at, job_id LIMIT 1 FOR UPDATE SKIP LOCKED)"
            " RETURNING *",
            (lease_seconds,),
        ).fetchone()


def _still_ours(conn: psycopg.Connection, schema: str, job: dict[str, Any]) -> bool:
    return conn.execute(
        f"SELECT job_id FROM {qualified(schema, 'command_jobs')} WHERE job_id = %s AND status = 'running'"
        " AND attempts = %s FOR UPDATE",
        (job["job_id"], job["attempts"]),
    ).fetchone() is not None


def _finish(conn: psycopg.Connection, schema: str, job: dict[str, Any], status: str, outcome: dict[str, Any]) -> None:
    conn.execute(
        f"UPDATE {qualified(schema, 'command_jobs')} SET status = %s, outcome = %s, finished_at = now(),"
        " lease_until = NULL WHERE job_id = %s",
        (status, Jsonb(outcome), job["job_id"]),
    )


def _retry(conn: psycopg.Connection, schema: str, job: dict[str, Any], delay: float) -> None:
    with conn.transaction():
        conn.execute(
            f"UPDATE {qualified(schema, 'command_jobs')} SET status = 'pending',"
            " next_attempt_at = now() + make_interval(secs => %s), lease_until = NULL"
            " WHERE job_id = %s AND status = 'running' AND attempts = %s",
            (delay, job["job_id"], job["attempts"]),
        )


def _reject(conn: psycopg.Connection, schema: str, job: dict[str, Any], reason: str) -> None:
    log.warning("trabajo %s de %s %s rechazado: %s", job["job_id"], job["command_type"], job["command_id"], reason)
    _finish(conn, schema, job, REJECTED, {"reason": reason})


def _fail(conn: psycopg.Connection, schema: str, job: dict[str, Any]) -> None:
    """El trabajo agotó sus intentos: queda `failed`, sin eventos, si sigue siendo suyo."""
    with conn.transaction():
        if _still_ours(conn, schema, job):
            log.error("trabajo %s de %s %s: %s tras %d intentos", job["job_id"], job["command_type"],
                      job["command_id"], ATTEMPTS_EXHAUSTED, job["attempts"])
            _finish(conn, schema, job, FAILED, {"reason": ATTEMPTS_EXHAUSTED})


def _command(job: dict[str, Any]) -> CommandEnvelope:
    return CommandEnvelope(command_id=job["command_id"], command_type=job["command_type"],
                           correlation_id=job["correlation_id"], causation_id=job["causation_id"],
                           payload=job["payload"])


def _work(conn: psycopg.Connection, schema: str, job: dict[str, Any], spec: CommandSpec | None) -> None:
    if spec is None:
        with conn.transaction():
            if _still_ours(conn, schema, job):
                _reject(conn, schema, job, "unknown_command")
        return
    command = _command(job)
    try:
        payload = spec.payload_model.model_validate(command.payload)
    except ValidationError:
        with conn.transaction():
            if _still_ours(conn, schema, job):
                _reject(conn, schema, job, "invalid_payload")
        return
    prepared = spec.prepare(command, payload) if spec.prepare is not None else None
    with conn.transaction():
        if not _still_ours(conn, schema, job):
            log.warning("trabajo %s de %s %s tomado por otro worker: no escribe su resultado", job["job_id"],
                        job["command_type"], job["command_id"])
            return
        outcome = spec.handle(conn, command, payload, prepared)
        if not isinstance(outcome, Accepted):
            _reject(conn, schema, job, outcome.reason)
            return
        check_causation(command, outcome.events)
        outbox.append(conn, schema, outcome.events)
        _finish(conn, schema, job, DONE, outcome.body | {"status": "accepted"})


def run_job_once(conn: psycopg.Connection, schema: str, specs: Mapping[str, CommandSpec], *,
                 lease_seconds: float, backoff: Backoff, max_attempts: int | None = None) -> bool:
    """Hace un trabajo de `<schema>.command_jobs`. Devuelve False si no había ninguno que tocara.

    Con `max_attempts`, un trabajo que lo supera queda `failed` (ver el punto 4 del módulo).
    `conn` debe estar sin transacción abierta, como en `execute_command`."""
    job = _claim(conn, schema, lease_seconds)
    if job is None:
        return False
    if max_attempts is not None and job["attempts"] > max_attempts:  # su worker cayó en cada intento
        _fail(conn, schema, job)
        return True
    token = otel_context.attach(tracing.extract(job["trace_context"]))
    try:
        with tracing.tracer().start_as_current_span(
            f"{job['command_type']} job", kind=SpanKind.INTERNAL,
            attributes={"eda.command_id": str(job["command_id"]), "eda.command_type": job["command_type"],
                        "eda.incident_id": job["correlation_id"], "eda.component": schema,
                        "eda.job_attempt": job["attempts"]},
        ):
            try:
                _work(conn, schema, job, specs.get(job["command_type"]))
            except Exception:  # el trabajo se reintenta; el error queda en el log
                log.exception("trabajo %s de %s %s falla (intento %d)", job["job_id"], job["command_type"],
                              job["command_id"], job["attempts"])
                if max_attempts is not None and job["attempts"] >= max_attempts:
                    _fail(conn, schema, job)
                else:
                    _retry(conn, schema, job, backoff.delay(job["attempts"]))
    finally:
        otel_context.detach(token)
    return True


class JobWorker(threading.Thread):
    """Worker de los trabajos de un componente en un hilo, con su propia conexión. `stop()` lo detiene tras el
    trabajo en curso; si el proceso muere antes, el trabajo se retoma al vencer su lease."""

    def __init__(self, connect: Callable[[], psycopg.Connection], schema: str, specs: Mapping[str, CommandSpec], *,
                 lease_seconds: float, poll_interval: float, backoff: Backoff, max_attempts: int | None = None,
                 run_once: Callable[..., bool] = run_job_once, name: str | None = None) -> None:
        super().__init__(name=name or f"jobs-{schema}", daemon=True)
        self._connect = connect
        self._schema = schema
        self._specs = specs
        self._lease_seconds = lease_seconds
        self._poll_interval = poll_interval
        self._backoff = backoff
        self._max_attempts = max_attempts
        self._run_once = run_once
        self._stop_requested = threading.Event()
        self.handled = 0

    def run(self) -> None:
        conn: psycopg.Connection | None = None
        try:
            while not self._stop_requested.is_set():
                try:
                    if conn is None or conn.closed:
                        conn = self._connect()
                    if self._run_once(conn, self._schema, self._specs, lease_seconds=self._lease_seconds,
                                      backoff=self._backoff, max_attempts=self._max_attempts):
                        self.handled += 1
                    else:
                        self._stop_requested.wait(self._poll_interval)
                except Exception:  # la base no responde: se reconecta y sigue
                    log.exception("worker de trabajos de %s: error", self._schema)
                    if conn is not None:
                        conn.close()
                    conn = None
                    self._stop_requested.wait(self._poll_interval)
        finally:
            if conn is not None:
                conn.close()

    def stop(self, timeout: float | None = None) -> None:
        self._stop_requested.set()
        self.join(timeout)


def start_worker(settings: Any, schema: str, specs: Mapping[str, CommandSpec]) -> JobWorker:
    """Arranca el worker de trabajos del componente `schema` con su propia conexión."""
    from eda_retail.infra.db import connect

    worker = JobWorker(lambda: connect(settings.database_url), schema, specs,
                       lease_seconds=settings.job_lease_seconds, poll_interval=settings.job_poll_interval_s,
                       backoff=Backoff(settings.job_retry_base_seconds, settings.job_retry_max_seconds),
                       max_attempts=settings.job_max_attempts)
    worker.start()
    return worker
