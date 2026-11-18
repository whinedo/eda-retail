"""Conexión a PostgreSQL con psycopg 3, con cada sentencia trazada (OpenTelemetry), y reconexión tras perderla
(spec resilience, «Reconexión a PostgreSQL»)."""

from __future__ import annotations

import contextlib
import logging
import threading
from collections.abc import Callable

import psycopg
from opentelemetry.instrumentation.psycopg import PsycopgInstrumentor
from opentelemetry.trace import NoOpTracerProvider
from psycopg.rows import dict_row

from eda_retail.infra import tracing

log = logging.getLogger(__name__)

CONNECT_TIMEOUT_S = 5  # una base que no responde da error al conectar en vez de esperar sin límite
# TCP keepalive: una conexión cuyo otro extremo desapareció (red cortada) se da por rota en unos 25 s. No detecta
# una base congelada (`docker pause`): el kernel del contenedor sigue respondiendo; esa conexión espera y continúa.
KEEPALIVES = {"keepalives": 1, "keepalives_idle": 10, "keepalives_interval": 5, "keepalives_count": 3}
CONNECTION_ERRORS = (psycopg.OperationalError, psycopg.InterfaceError)
RETRY_S = 1.0  # espera antes de volver a intentarlo con la base caída

Reconnect = Callable[[], psycopg.Connection]


def connect(database_url: str) -> psycopg.Connection:
    """Abre una conexión sin autocommit: cada `with conn.transaction()` es una transacción."""
    conn = psycopg.connect(database_url, row_factory=dict_row, autocommit=False, connect_timeout=CONNECT_TIMEOUT_S,
                           **KEEPALIVES)
    return PsycopgInstrumentor().instrument_connection(
        conn, tracer_provider=tracing.provider() or NoOpTracerProvider()
    )


def healthy(conn: psycopg.Connection) -> bool:
    """False si la conexión está cerrada o se perdió (el servidor la cortó o dejó de responder)."""
    return not (conn.closed or conn.broken)


def reopen(conn: psycopg.Connection, reconnect: Reconnect) -> psycopg.Connection:
    """`conn` si sigue sana; si no, la cierra y devuelve otra de `reconnect`. Si la base aún no responde, el error
    de conexión sube y `conn` queda cerrada: quien llama lo vuelve a intentar en la siguiente vuelta."""
    if healthy(conn):
        return conn
    with contextlib.suppress(Exception):
        conn.close()
    return reconnect()


def run_loop(reconnect: Reconnect, stop: threading.Event, step: Callable[[psycopg.Connection], object], *,
             interval: float, name: str, retry_s: float = RETRY_S) -> None:
    """Bucle de un hilo de larga duración (dispatcher, plazos): `step(conn)` hasta `stop`, esperando `interval`
    cuando no hay trabajo. Un error se registra y el bucle sigue; si además rompió la conexión, se abre otra, y
    mientras la base no responda se reintenta cada `retry_s`."""
    conn: psycopg.Connection | None = None
    try:
        while not stop.is_set():
            try:
                conn = reconnect() if conn is None else reopen(conn, reconnect)
                busy = bool(step(conn))
            except Exception:
                log.exception("%s: error; se reintenta", name)  # si la conexión se rompió, `reopen` abre otra
                stop.wait(retry_s)
                continue
            if not busy:
                stop.wait(interval)
    finally:
        if conn is not None:
            with contextlib.suppress(Exception):
                conn.close()
