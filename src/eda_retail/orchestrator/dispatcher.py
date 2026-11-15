"""Dispatcher de comandos y plazos de la saga (design.md §4.2 y §4.4, spec event-backbone
"Invocación de comandos").

- `dispatch_once`: toma un comando `pending` de `orchestrator.command_outbox` cuyo intento toca, lo
  invoca por REST con su `command_id` (el mismo en cada reintento) y su contexto de traza, y fuera
  de la llamada HTTP registra la respuesta: 202 lo da por entregado; un 4xx es un rechazo, que la
  saga trata en la misma transacción; si el sistema no responde, espera con jitter y lo reintenta
  sin bloquear los comandos de otras incidencias.
- `expire_once`: trata los plazos vencidos de `orchestrator.deadlines`, también los que vencieron
  mientras el proceso estaba parado (spec resilience, "Reinicio del orquestador").
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import psycopg
from opentelemetry import context as otel_context
from psycopg.types.json import Jsonb

from eda_retail.command.rules import BusinessRules
from eda_retail.config import Settings
from eda_retail.executors.retry import Backoff
from eda_retail.infra import command_client, tracing
from eda_retail.orchestrator.saga import (
    COMMAND_REJECTED,
    PLAN_TIMEOUT,
    REJECTED,
    TIMEOUT,
    lock,
    target_url,
)

log = logging.getLogger("eda_retail.orchestrator")
Invoke = Callable[..., command_client.CommandReply]


def _due(conn: psycopg.Connection) -> dict[str, Any] | None:
    with conn.transaction():
        return conn.execute(
            "SELECT * FROM orchestrator.command_outbox WHERE status = 'pending' AND next_attempt_at <= now()"
            " ORDER BY next_attempt_at, created_at LIMIT 1"
        ).fetchone()


def _deadline(conn: psycopg.Connection, command_id) -> dict[str, Any] | None:
    return conn.execute("SELECT * FROM orchestrator.deadlines WHERE command_id = %s", (command_id,)).fetchone()


def dispatch_once(conn: psycopg.Connection, settings: Settings, rules: BusinessRules, *,
                  invoke: Invoke = command_client.invoke, backoff: Backoff | None = None) -> bool:
    """Invoca un comando pendiente. Devuelve False si no había ninguno."""
    row = _due(conn)
    if row is None:
        return False
    token = otel_context.attach(tracing.extract(row["trace_context"]))
    try:
        reply = invoke(target_url(settings, row["target"]), row["command_type"], command_id=row["command_id"],
                       correlation_id=row["incident_id"], causation_id=row["causation_id"],
                       payload=row["payload"], timeout=settings.command_http_timeout_s)
    except command_client.CommandUnavailable as exc:
        delay = (backoff or Backoff()).delay(row["attempts"] + 1)
        log.warning("comando %s %s sin respuesta (intento %d): %s", row["command_type"], row["command_id"],
                    row["attempts"] + 1, exc)
        with conn.transaction():
            conn.execute(
                "UPDATE orchestrator.command_outbox SET attempts = attempts + 1,"
                " next_attempt_at = now() + make_interval(secs => %s) WHERE command_id = %s AND status = 'pending'",
                (delay, row["command_id"]),
            )
        return True
    finally:
        otel_context.detach(token)

    with conn.transaction():
        t = lock(conn, settings, rules, row["incident_id"], row["command_id"])
        status = "delivered" if reply.accepted else "rejected"
        updated = conn.execute(
            "UPDATE orchestrator.command_outbox SET status = %s, attempts = attempts + 1, response = %s"
            " WHERE command_id = %s AND status = 'pending'",
            (status, Jsonb(reply.body), row["command_id"]),
        ).rowcount
        if updated == 0 or reply.accepted or t is None:
            return True
        deadline = _deadline(conn, row["command_id"])
        if deadline is None or deadline["resolved_at"] is not None:
            return True  # ya resuelto: el rechazo llega tarde
        conn.execute("UPDATE orchestrator.deadlines SET resolved_at = now() WHERE command_id = %s",
                     (row["command_id"],))
        log.warning("comando %s %s rechazado: %s", row["command_type"], row["command_id"], reply.reason)
        if deadline["kind"] == "execute_step":
            step = t.step(deadline["step_id"])
            if step is not None and step["status"] == "requested":
                t.fail(step, error=REJECTED, retryable=False)
        else:
            t.escalate(COMMAND_REJECTED, publish=True)
        t.flush()
    return True


def expire_once(conn: psycopg.Connection, settings: Settings, rules: BusinessRules) -> int:
    """Trata los plazos vencidos. Devuelve cuántos ha tratado."""
    with conn.transaction():
        due = conn.execute(
            "SELECT command_id, incident_id FROM orchestrator.deadlines WHERE resolved_at IS NULL"
            " AND expires_at <= now() ORDER BY expires_at"
        ).fetchall()
    handled = 0
    for item in due:
        with conn.transaction():
            t = lock(conn, settings, rules, item["incident_id"], item["command_id"])
            deadline = conn.execute(
                "SELECT * FROM orchestrator.deadlines WHERE command_id = %s AND resolved_at IS NULL FOR UPDATE",
                (item["command_id"],),
            ).fetchone()
            if t is None or deadline is None:  # lo resolvió un resultado mientras tanto
                continue
            conn.execute("UPDATE orchestrator.deadlines SET resolved_at = now() WHERE command_id = %s",
                         (item["command_id"],))
            conn.execute("UPDATE orchestrator.command_outbox SET status = 'expired' WHERE command_id = %s"
                         " AND status = 'pending'", (item["command_id"],))
            if deadline["kind"] == "execute_step":
                step = t.step(deadline["step_id"])
                if step is not None and step["status"] == "requested":
                    t.fail(step, error=TIMEOUT, retryable=False)
            else:
                t.escalate(PLAN_TIMEOUT, publish=True)
            t.flush()
            handled += 1
    return handled
