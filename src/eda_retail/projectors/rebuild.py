"""Reconstrucción de vistas (spec cqrs-views, «Reconstrucción de vistas»; experimento E4).

`rebuild` pone la vista al día consumiendo el topic hasta sus marcas de agua altas, calcula su
huella, vacía las tablas de la vista, su posición (`views.offsets`) y sus eventos aplicados
(`views.applied_events`), vuelve a consumir `eda.events` desde el principio (retención infinita)
hasta las mismas marcas y calcula de nuevo la huella: las dos deben coincidir. Registra el tiempo
de reconstrucción.

Se lanza con el proceso `eda-projector` parado, para que nadie más escriba en la vista.

La huella es el sha256 de las filas de las tablas de la vista ordenadas por su clave, sin las
columnas que no salen del log (`updated_at`).
"""

from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass

import psycopg

from eda_retail.config import Settings
from eda_retail.projectors.base import consumer_for
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


def rebuild(settings: Settings, conn: psycopg.Connection, view: str) -> RebuildResult:
    spec = PROJECTORS[view]
    projector = spec.build()
    consumer = consumer_for(projector, settings, conn)
    try:
        consumer.catch_up()
        before = fingerprint(conn, view)
        start = time.perf_counter()
        with conn.transaction():
            conn.execute("DELETE FROM views.applied_events WHERE projector = %s", (projector.name,))
        consumer.reset(spec.tables)
        applied = consumer.catch_up()
        seconds = time.perf_counter() - start
    finally:
        consumer.close()
    after = fingerprint(conn, view)
    log.info("vista %s reconstruida: %d eventos en %.3f s, huella %s", view, applied, seconds,
             "idéntica" if before == after else "distinta")
    return RebuildResult(view, before, after, applied, seconds)
