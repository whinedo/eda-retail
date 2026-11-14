"""Reconstrucción de vistas (spec cqrs-views, «Reconstrucción de vistas»; experimento E4).

`rebuild` pone la vista al día de forma incremental, calcula su huella, la vacía, pone la
posición del proyector a 0, reproduce `events` desde `seq` 1 (sin depender de Kafka) y
vuelve a calcular la huella: las dos deben coincidir. Registra el tiempo de reconstrucción.

Supuesto A3 (odd/tasks/views.md): la reconstrucción se lanza sin escritores activos, así que
un hueco en `seq` es un rollback antiguo y se salta sin esperar (`gap_timeout` 0).

La huella es el sha256 de las filas de las tablas de la vista ordenadas por su clave, sin
las columnas que no salen del log (`updated_at`).
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import timedelta

import psycopg

from eda_retail.projectors.base import Projector
from eda_retail.projectors.registry import PROJECTORS

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class RebuildResult:
    view: str
    fingerprint_before: str
    fingerprint_after: str
    events_applied: int
    seconds: float

    @property
    def identical(self) -> bool:
        return self.fingerprint_before == self.fingerprint_after


def fingerprint(conn: psycopg.Connection, view: str) -> str:
    spec = PROJECTORS[view]
    digest = hashlib.sha256()
    with conn.transaction():
        for table in spec.tables:
            digest.update(table.encode())
            for row in conn.execute(f"SELECT * FROM {table} ORDER BY 1"):  # la clave es la primera columna
                kept = {k: v for k, v in row.items() if k not in spec.volatile}
                digest.update(json.dumps(kept, sort_keys=True, default=str).encode())
    return digest.hexdigest()


def _catch_up(conn: psycopg.Connection, projector: Projector) -> int:
    applied = 0
    while advanced := projector.run_once(conn):
        applied += advanced
    return applied


def rebuild(conn: psycopg.Connection, view: str) -> RebuildResult:
    spec = PROJECTORS[view]
    projector = spec.build(gap_timeout=timedelta(0))
    _catch_up(conn, projector)
    before = fingerprint(conn, view)
    start = time.perf_counter()
    with conn.transaction():
        for table in spec.tables:
            conn.execute(f"DELETE FROM {table}")
        conn.execute(
            """
            INSERT INTO projector_offsets (projector, last_seq) VALUES (%s, 0)
            ON CONFLICT (projector) DO UPDATE SET last_seq = 0
            """,
            (projector.name,),
        )
    applied = _catch_up(conn, projector)
    seconds = time.perf_counter() - start
    after = fingerprint(conn, view)
    log.info("vista %s reconstruida: %d eventos en %.3f s, huella %s", view, applied, seconds,
             "idéntica" if before == after else "distinta")
    return RebuildResult(view, before, after, applied, seconds)
