"""Consumidor genérico de `eda.events` (design.md §6).

Cada mensaje se procesa en una transacción de PostgreSQL que incluye el registro en el
inbox y lo que escriba el handler (estado, `events`, `outbox`). El offset de Kafka se
confirma solo después del `COMMIT` (`enable.auto.commit=false`).

Fuera de alcance aquí (capacidad `resilience`): reintentos con backoff, dead letter
queue y escalado. Si el handler falla, la excepción se propaga y el offset no se confirma.
"""

from __future__ import annotations

import enum
import json
import uuid
from collections.abc import Callable, Mapping
from typing import Protocol, runtime_checkable

import psycopg
from confluent_kafka import Consumer, Message, TopicPartition
from opentelemetry.trace import SpanKind

from eda_retail.config import Settings
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra import inbox, tracing
from eda_retail.infra.db import connect
from eda_retail.infra.kafka import make_consumer

class Outcome(enum.Enum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    IGNORED = "ignored"


@runtime_checkable
class UnitOfWork(Protocol):
    """Handler que gestiona sus propias transacciones (agentes con LLM, regla 6 de CLAUDE.md):
    la llamada al LLM se hace fuera de la transacción que registra el inbox y el comando."""

    def process(self, conn: psycopg.Connection, event: EventEnvelope) -> Outcome: ...


Handler = Callable[[psycopg.Connection, EventEnvelope], None] | UnitOfWork


def decode_message(msg: Message) -> EventEnvelope:
    body = json.loads(msg.value())
    body.pop("seq", None)
    return EventEnvelope.model_validate(body)


class EventConsumer:
    def __init__(
        self,
        name: str,
        conn: psycopg.Connection,
        kafka_consumer: Consumer,
        topic: str,
        handlers: Mapping[str, Handler],
        *,
        inbox_enabled: bool = True,
    ) -> None:
        self.name = name
        self._conn = conn
        self._kafka = kafka_consumer
        self._handlers = dict(handlers)
        self._inbox_enabled = inbox_enabled
        self._kafka.subscribe([topic])

    @classmethod
    def from_settings(
        cls, settings: Settings, *, name: str, handlers: Mapping[str, Handler]
    ) -> EventConsumer:
        """`name` es a la vez el `group.id` de Kafka y el nombre del consumidor en el inbox."""
        return cls(
            name,
            connect(settings.database_url),
            make_consumer(settings.kafka_bootstrap_servers, group_id=name),
            settings.events_topic,
            handlers,
            inbox_enabled=settings.inbox_enabled,
        )

    def poll(self, timeout: float) -> Message | None:
        msg = self._kafka.poll(timeout)
        if msg is None or msg.error():
            return None
        return msg

    @staticmethod
    def event_id_of(msg: Message) -> uuid.UUID:
        return uuid.UUID(json.loads(msg.value())["event_id"])

    def handle_message(self, msg: Message) -> Outcome:
        """Procesa el mensaje en una transacción. No confirma el offset."""
        event = decode_message(msg)
        if event.event_type not in self._handlers:
            return Outcome.IGNORED
        headers = {k: v.decode() for k, v in (msg.headers() or []) if v is not None}
        attributes = tracing.event_attributes(event) | {
            "messaging.system": "kafka",
            "messaging.destination.name": msg.topic(),
            "messaging.consumer.group.name": self.name,
            "messaging.kafka.partition": msg.partition(),
            "messaging.kafka.offset": msg.offset(),
        }
        with tracing.tracer().start_as_current_span(
            f"{event.event_type} process",
            context=tracing.extract(headers),
            kind=SpanKind.CONSUMER,
            attributes=attributes,
        ) as span:
            outcome = self.process(event)
            span.set_attribute("eda.outcome", outcome.value)
        return outcome

    def process(self, event: EventEnvelope) -> Outcome:
        """Procesa un evento ya decodificado (sin Kafka). No confirma ningún offset."""
        handler = self._handlers.get(event.event_type)
        if handler is None:
            return Outcome.IGNORED
        if isinstance(handler, UnitOfWork):
            return handler.process(self._conn, event)
        with self._conn.transaction():
            if not inbox.register(self._conn, self.name, event.event_id, enabled=self._inbox_enabled):
                return Outcome.DUPLICATE
            handler(self._conn, event)
        return Outcome.PROCESSED

    def commit(self, msg: Message) -> None:
        """Confirma el offset en Kafka. Llamar solo tras el COMMIT de PostgreSQL."""
        self._kafka.commit(message=msg, asynchronous=False)

    def committed_offset(self, msg: Message) -> int:
        [tp] = self._kafka.committed([TopicPartition(msg.topic(), msg.partition())], timeout=10)
        return tp.offset

    def run_once(self, timeout: float = 1.0) -> Outcome | None:
        msg = self.poll(timeout)
        if msg is None:
            return None
        outcome = self.handle_message(msg)
        self.commit(msg)
        return outcome

    def run_forever(self) -> None:
        while True:
            self.run_once()

    def close(self) -> None:
        self._kafka.close()
        self._conn.close()
