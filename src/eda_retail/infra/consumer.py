"""Consumidor genérico de `eda.events` (design.md §6).

Cada mensaje se procesa en una transacción de PostgreSQL que incluye el registro en el
inbox y lo que escriba el handler (su estado y su `outbox`, en el esquema del componente). El offset de Kafka se
confirma solo después del `COMMIT` (`enable.auto.commit=false`).

Un mensaje que no cumple su esquema (o no es JSON) va a la dead letter queue en una
transacción con su fila de inbox, y se confirma su offset: no se descarta en silencio ni
bloquea la partición. `on_dead_letter` permite escalar la incidencia a la que pertenece.
Si el handler falla por otra causa, la excepción se propaga y el offset no se confirma.
"""

from __future__ import annotations

import enum
import json
import logging
import uuid
from collections.abc import Callable, Mapping
from typing import Protocol, runtime_checkable

import psycopg
from confluent_kafka import Consumer, Message, TopicPartition
from opentelemetry.trace import SpanKind
from pydantic import ValidationError

from eda_retail.config import Settings
from eda_retail.events.catalog import CATALOG, PayloadError, PersonalDataError, validate_payload
from eda_retail.events.envelope import EventEnvelope
from eda_retail.events.upcasters import to_current
from eda_retail.events.versioning import UpcastError
from eda_retail.infra import dead_letters, inbox, tracing
from eda_retail.infra.db import connect
from eda_retail.infra.kafka import make_consumer

log = logging.getLogger("eda_retail.consumer")


class Outcome(enum.Enum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    IGNORED = "ignored"
    DEAD_LETTER = "dead_letter"


@runtime_checkable
class UnitOfWork(Protocol):
    """Handler que gestiona sus propias transacciones (agentes con LLM, regla 6 de CLAUDE.md):
    la llamada al LLM se hace fuera de la transacción que registra el inbox y el comando."""

    def process(self, conn: psycopg.Connection, event: EventEnvelope) -> Outcome: ...


Handler = Callable[[psycopg.Connection, EventEnvelope], None] | UnitOfWork
# (conexión, correlation_id, event_id) -> escala la incidencia, dentro de la transacción.
DeadLetterHook = Callable[[psycopg.Connection, str, uuid.UUID], None]


class InvalidMessage(ValueError):
    """El mensaje no es JSON o no cumple la estructura común o el esquema de su tipo."""

    def __init__(self, detail: str, raw: object, event_id: uuid.UUID | None, correlation_id: str | None,
                 event_type: str | None) -> None:
        super().__init__(detail)
        self.raw = raw
        self.event_id = event_id
        self.correlation_id = correlation_id
        self.event_type = event_type


def decode_message(msg: Message) -> EventEnvelope:
    body = json.loads(msg.value())
    body.pop("seq", None)
    return to_current(EventEnvelope.model_validate(body))


def _uuid(value: object) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(value))
    except ValueError:
        return None


def decode_checked(msg: Message) -> EventEnvelope:
    """Decodifica y valida el payload con el catálogo; si no puede, `InvalidMessage`."""
    raw = msg.value()
    try:
        body = json.loads(raw)
    except (TypeError, ValueError) as exc:
        text = raw.decode(errors="replace") if isinstance(raw, bytes) else str(raw)
        raise InvalidMessage(f"no es JSON: {exc}", {"raw": text}, None, None, None) from exc
    if not isinstance(body, dict):
        raise InvalidMessage("no es un objeto JSON", {"raw": body}, None, None, None)
    event_id, correlation_id = _uuid(body.get("event_id")), body.get("correlation_id")
    event_type = body.get("event_type") if isinstance(body.get("event_type"), str) else None
    try:
        envelope = decode_message(msg)
        if (spec := CATALOG.get(envelope.event_type)) is not None:
            if envelope.event_version > spec.version:
                # Una versión posterior a la vigente no tiene upcaster: no se sabe leer (spec experiments, E18).
                raise UpcastError(f"{envelope.event_type}: versión {envelope.event_version} desconocida "
                                  f"(la vigente es la {spec.version})")
            validate_payload(envelope.event_type, envelope.payload)
    except (ValidationError, PayloadError, PersonalDataError, UpcastError, KeyError) as exc:
        cid = correlation_id if isinstance(correlation_id, str) else None
        raise InvalidMessage(str(exc), body, event_id, cid, event_type) from exc
    return envelope


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
        on_dead_letter: DeadLetterHook | None = None,
        schema: str,
    ) -> None:
        self.name = name
        self.schema = schema  # esquema del componente: su inbox y sus dead letters
        self._on_dead_letter = on_dead_letter
        self._conn = conn
        self._kafka = kafka_consumer
        self._handlers = dict(handlers)
        self._inbox_enabled = inbox_enabled
        self._kafka.subscribe([topic], on_assign=self._assigned, on_revoke=self._revoked, on_lost=self._lost)

    # Rebalanceos del grupo, en el log del proceso (E16 los lee para fechar el rebalanceo). Sin llamar a `assign`:
    # tras el callback, la biblioteca aplica la asignación.
    def _assigned(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        log.info("%s: particiones asignadas %s", self.name, sorted(p.partition for p in partitions))

    def _revoked(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        log.info("%s: particiones revocadas %s", self.name, sorted(p.partition for p in partitions))

    def _lost(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        log.info("%s: particiones perdidas %s", self.name, sorted(p.partition for p in partitions))

    @classmethod
    def from_settings(
        cls, settings: Settings, *, name: str, handlers: Mapping[str, Handler],
        on_dead_letter: DeadLetterHook | None = None, schema: str,
    ) -> EventConsumer:
        """`name` es a la vez el `group.id` de Kafka y el nombre del consumidor en el inbox."""
        return cls(
            name,
            connect(settings.database_url),
            make_consumer(settings.kafka_bootstrap_servers, group_id=name),
            settings.events_topic,
            handlers,
            inbox_enabled=settings.inbox_enabled,
            on_dead_letter=on_dead_letter,
            schema=schema,
        )

    def poll(self, timeout: float) -> Message | None:
        msg = self._kafka.poll(timeout)
        if msg is None or msg.error():
            return None
        return msg

    @staticmethod
    def event_id_of(msg: Message) -> uuid.UUID:
        return uuid.UUID(json.loads(msg.value())["event_id"])

    def attends(self, event_type: str | None) -> bool:
        """True si atiende el tipo; un mensaje sin `event_type` legible lo atienden todos (spec resilience)."""
        return event_type is None or event_type in self._handlers

    def handle_message(self, msg: Message) -> Outcome:
        """Procesa el mensaje en una transacción. No confirma el offset."""
        try:
            event = decode_checked(msg)
        except InvalidMessage as bad:
            if not self.attends(bad.event_type):
                return Outcome.IGNORED  # no es para este consumidor
            return self._dead_letter(bad)
        if event.event_type not in self._handlers:
            return Outcome.IGNORED
        log.debug("-> %s %s p%d o%d", event.event_type, event.correlation_id, msg.partition(), msg.offset())
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
        log.info("<- %s %s: %s", event.event_type, event.correlation_id, outcome.value)
        return outcome

    def process(self, event: EventEnvelope) -> Outcome:
        """Procesa un evento ya decodificado (sin Kafka). No confirma ningún offset."""
        handler = self._handlers.get(event.event_type)
        if handler is None:
            return Outcome.IGNORED
        if isinstance(handler, UnitOfWork):
            return handler.process(self._conn, event)
        with self._conn.transaction():
            if not inbox.register(
                self._conn, self.name, event.event_id, enabled=self._inbox_enabled, schema=self.schema
            ):
                return Outcome.DUPLICATE
            handler(self._conn, event)
        return Outcome.PROCESSED

    def _dead_letter(self, bad: InvalidMessage) -> Outcome:
        return write_dead_letter(self._conn, self.name, bad, inbox_enabled=self._inbox_enabled,
                                 hook=self._on_dead_letter, schema=self.schema)

    def commit(self, msg: Message) -> None:
        """Confirma el offset en Kafka. Llamar solo tras el COMMIT de PostgreSQL."""
        self._kafka.commit(message=msg, asynchronous=False)

    def committed_offset(self, msg: Message) -> int:
        [tp] = self._kafka.committed([TopicPartition(msg.topic(), msg.partition())], timeout=10)
        return tp.offset

    def positions(self) -> dict[int, int]:
        """Partición asignada → siguiente offset que leerá el consumidor (negativo si aún no tiene posición)."""
        return {tp.partition: tp.offset for tp in self._kafka.position(self._kafka.assignment())}

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


def write_dead_letter(
    conn: psycopg.Connection, consumer: str, bad: InvalidMessage, *, inbox_enabled: bool = True,
    hook: DeadLetterHook | None = None, schema: str,
) -> Outcome:
    """Guarda el mensaje inválido en `dead_letters` (con su fila de inbox) y escala la incidencia."""
    with tracing.tracer().start_as_current_span(
        "dead letter", attributes={"eda.reason": dead_letters.INVALID_SCHEMA, "eda.consumer": consumer}
    ), conn.transaction():
        if bad.event_id is not None and not inbox.register(
            conn, consumer, bad.event_id, enabled=inbox_enabled, schema=schema
        ):
            return Outcome.DUPLICATE
        dead_letters.record(
            conn, consumer=consumer, reason=dead_letters.INVALID_SCHEMA, event_id=bad.event_id,
            correlation_id=bad.correlation_id, raw=bad.raw, schema=schema,
        )
        log.warning("dead letter %s (%s): %s", bad.event_type or "mensaje", bad.correlation_id or "-", bad)
        if hook is not None and bad.correlation_id and bad.event_id is not None:
            hook(conn, bad.correlation_id, bad.event_id)
    return Outcome.DEAD_LETTER
