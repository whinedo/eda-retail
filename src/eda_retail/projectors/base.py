"""Proyector genérico (design.md §8, spec cqrs-views).

Cada proyector consume `eda.events` de Kafka con el consumidor posicionado (`infra.positions`):
guarda en `views.offsets` el último offset aplicado de cada partición en la misma transacción que
su vista, así que un mensaje que Kafka reentrega no se aplica otra vez. Además anota cada evento
aplicado en `views.applied_events`: un evento que se vuelve a publicar (el mismo `event_id` con un
offset nuevo, p. ej. al reproducir un outbox) tampoco cambia la vista (proyector idempotente).

Un mensaje que no es un evento válido se salta, con su offset: no debe bloquear la vista.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any

import psycopg
from confluent_kafka import Message
from opentelemetry import trace

from eda_retail.config import Settings
from eda_retail.events.catalog import CATALOG, PayloadError, PersonalDataError, validate_payload
from eda_retail.events.envelope import EventEnvelope
from eda_retail.events.upcasters import UPCASTERS
from eda_retail.events.versioning import Upcasters
from eda_retail.infra import tracing
from eda_retail.infra.consumer import InvalidMessage, decode_checked
from eda_retail.infra.db import connect
from eda_retail.infra.kafka import make_consumer
from eda_retail.infra.positions import Position, PositionedConsumer, position_of

log = logging.getLogger(__name__)

SCHEMA = "views"

Apply = Callable[[psycopg.Connection, Position, EventEnvelope], None]

_ENVELOPE_FIELDS = ("event_id", "event_type", "event_version", "occurred_at", "correlation_id",
                    "causation_id", "producer", "payload")


def load_event(row: Mapping[str, Any]) -> EventEnvelope:
    return EventEnvelope.model_validate({k: row[k] for k in _ENVELOPE_FIELDS})


class Projector:
    def __init__(self, name: str, handlers: Mapping[str, Apply], *, upcasters: Upcasters | None = None) -> None:
        self.name = name
        self._handlers = dict(handlers)
        # Los conversores propios amplían el registro compartido (no lo sustituyen).
        self._upcasters = UPCASTERS.extended(upcasters) if upcasters else UPCASTERS

    def apply(self, conn: psycopg.Connection, position: Position, event: EventEnvelope) -> bool:
        """Aplica un evento a la vista dentro de la transacción abierta. False si no la cambia."""
        handler = self._handlers.get(event.event_type)
        if handler is None:
            return False
        target = CATALOG[event.event_type].version if event.event_type in CATALOG else event.event_version
        event = self._upcasters.upcast(event, target)
        if event.event_type in CATALOG:
            try:
                validate_payload(event.event_type, event.payload)
            except (PayloadError, PersonalDataError) as exc:
                # Evento envenenado: no puede aplicarse y no debe bloquear la vista para siempre.
                log.warning("proyector %s: se salta el evento %s (%s): %s", self.name, event.event_id, position, exc)
                trace.get_current_span().add_event("eda.invalid_event_skipped", {"eda.event_id": str(event.event_id)})
                return False
        first_time = conn.execute(
            "INSERT INTO views.applied_events (projector, event_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (self.name, event.event_id),
        ).rowcount == 1
        if not first_time:
            return False
        handler(conn, position, event)
        return True

    def apply_message(self, conn: psycopg.Connection, msg: Message) -> None:
        """`ApplyMessage` del consumidor posicionado."""
        try:
            event = decode_checked(msg)
        except InvalidMessage as bad:
            log.warning("proyector %s: se salta un mensaje inválido (%s): %s", self.name, bad.event_type or "?", bad)
            return
        with tracing.tracer().start_as_current_span(
            f"projector {self.name}", attributes={"eda.projector": self.name, **tracing.event_attributes(event)}
        ):
            self.apply(conn, position_of(msg), event)


def consumer_for(projector: Projector, settings: Settings, conn: psycopg.Connection | None = None) -> PositionedConsumer:
    """Consumidor posicionado del proyector sobre `settings.events_topic` (su posición en `views.offsets`)."""
    return PositionedConsumer(
        conn if conn is not None else connect(settings.database_url),
        make_consumer(settings.kafka_bootstrap_servers, group_id=f"views.{projector.name}"),
        settings.events_topic,
        schema=SCHEMA,
        consumer=projector.name,
        apply=projector.apply_message,
    )
