"""Proyector genérico (design.md §8, spec cqrs-views).

Lee `events` por `seq` (no Kafka) y guarda su posición en `projector_offsets` en la misma
transacción que la vista. Varias instancias del mismo proyector se serializan con
`FOR UPDATE` sobre su fila de posición.

Huecos en `seq`: una transacción que aún no ha hecho COMMIT puede tener un `seq` menor
que otra ya confirmada. Si el proyector avanzara, ese evento se perdería para siempre, así
que se detiene en el primer hueco. Un rollback deja un hueco permanente: si el hueco sigue
ahí `gap_timeout` después de verlo por primera vez (reloj monotónico del proceso, sin
depender de `occurred_at` ni de relojes de otras máquinas), el proyector lo da por perdido.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Any

import psycopg
from opentelemetry import trace

from eda_retail.events.catalog import CATALOG, PayloadError, PersonalDataError, validate_payload
from eda_retail.events.envelope import EventEnvelope
from eda_retail.events.upcasters import UPCASTERS
from eda_retail.events.versioning import Upcasters
from eda_retail.infra import tracing

log = logging.getLogger(__name__)

Apply = Callable[[psycopg.Connection, int, EventEnvelope], None]

_ENVELOPE_FIELDS = ("event_id", "event_type", "event_version", "occurred_at", "correlation_id",
                    "causation_id", "producer", "payload")


def load_event(row: Mapping[str, Any]) -> EventEnvelope:
    return EventEnvelope.model_validate({k: row[k] for k in _ENVELOPE_FIELDS})


class Projector:
    def __init__(
        self,
        name: str,
        handlers: Mapping[str, Apply],
        *,
        upcasters: Upcasters | None = None,
        gap_timeout: timedelta = timedelta(seconds=5),
        batch_size: int = 500,
    ) -> None:
        self.name = name
        self._handlers = dict(handlers)
        self._upcasters = upcasters or UPCASTERS
        self.gap_timeout = gap_timeout
        self.batch_size = batch_size
        self._gap_seen: dict[int, float] = {}  # seq tras el que hay un hueco -> cuándo se vio

    def apply(self, conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
        """Aplica un evento a la vista. Los handlers son idempotentes."""
        handler = self._handlers.get(event.event_type)
        if handler is None:
            return
        target = CATALOG[event.event_type].version if event.event_type in CATALOG else event.event_version
        event = self._upcasters.upcast(event, target)
        if event.event_type in CATALOG:
            try:
                validate_payload(event.event_type, event.payload)
            except (PayloadError, PersonalDataError) as exc:
                # Evento envenenado: no puede aplicarse y no debe bloquear la vista para siempre.
                log.warning("proyector %s: se salta el evento %s (seq %s): %s", self.name, event.event_id, seq, exc)
                trace.get_current_span().add_event(
                    "eda.invalid_event_skipped", {"eda.seq": seq, "eda.event_id": str(event.event_id)}
                )
                return
        handler(conn, seq, event)

    def run_once(self, conn: psycopg.Connection) -> int:
        """Aplica el siguiente lote contiguo; devuelve cuántos eventos avanzó."""
        with tracing.tracer().start_as_current_span(f"projector {self.name}") as span, conn.transaction():
            conn.execute(
                "INSERT INTO projector_offsets (projector) VALUES (%s) ON CONFLICT DO NOTHING", (self.name,)
            )
            last = conn.execute(
                "SELECT last_seq FROM projector_offsets WHERE projector = %s FOR UPDATE", (self.name,)
            ).fetchone()["last_seq"]
            rows = conn.execute(
                """
                SELECT * FROM events WHERE seq > %s ORDER BY seq LIMIT %s
                """,
                (last, self.batch_size),
            ).fetchall()
            advanced = 0
            for row in rows:
                if row["seq"] != last + 1 and not self._gap_expired(last):
                    span.set_attribute("eda.waiting_gap_after", last)
                    break
                self._gap_seen.pop(last, None)
                self.apply(conn, row["seq"], load_event(row))
                last = row["seq"]
                advanced += 1
            conn.execute("UPDATE projector_offsets SET last_seq = %s WHERE projector = %s", (last, self.name))
            span.set_attributes({"eda.projector": self.name, "eda.advanced": advanced, "eda.last_seq": last})
        return advanced

    def _gap_expired(self, last: int) -> bool:
        first_seen = self._gap_seen.setdefault(last, time.monotonic())
        return time.monotonic() - first_seen >= self.gap_timeout.total_seconds()
