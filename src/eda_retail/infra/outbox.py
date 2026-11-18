"""Outbox y relay de cada componente (design.md §5, spec event-backbone "Publicación atómica con
el estado (Outbox)").

- `append` escribe los eventos en `<esquema>.outbox` dentro de la transacción del componente, junto
  a su cambio de estado. Nunca publica en Kafka.
- `ComponentRelay` publica el outbox del componente en `eda.events` (clave `correlation_id` =
  `incident_id`) en orden de `id`, espera la confirmación del broker y guarda la partición y el
  offset en la misma transacción. Publica también sus `dead_letters` en `eda.dlq`. Al menos una vez.
- `RelayThread` ejecuta el relay en un hilo del proceso del componente, con su propia conexión.
- `republish_in_order` es el procedimiento de recuperación tras perder el topic (spec event-backbone, «Pérdida del
  topic»): con el sistema parado, republica a la vez el outbox de todos los componentes, mezclado por la posición que
  cada fila tenía en el topic perdido, para que cada partición recupere su orden también entre componentes.

Un solo relay por componente conserva el orden de su outbox; Kafka conserva el orden por partición.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Iterable, Mapping
from typing import Any

import psycopg
from confluent_kafka import KafkaError, Producer
from opentelemetry.trace import SpanKind
from psycopg.types.json import Jsonb

from eda_retail.config import Settings
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra import tracing
from eda_retail.infra.db import connect
from eda_retail.infra.event_store import validate_event
from eda_retail.infra.kafka import make_producer
from eda_retail.infra.schemas import qualified

log = logging.getLogger("eda_retail.outbox")

_ENVELOPE_FIELDS = tuple(EventEnvelope.model_fields)


def append(
    conn: psycopg.Connection, schema: str, events: Iterable[EventEnvelope | Mapping[str, Any]]
) -> list[int]:
    """Valida y escribe los eventos en `<schema>.outbox`. Devuelve sus `id` en orden."""
    table = qualified(schema, "outbox")
    validated = [validate_event(e) for e in events]
    ids: list[int] = []
    for e in validated:
        with tracing.tracer().start_as_current_span(
            f"{e.event_type} create", kind=SpanKind.PRODUCER, attributes=tracing.event_attributes(e)
        ):
            trace_context = tracing.inject_current()
            row = conn.execute(
                f"""
                INSERT INTO {table} (event_id, event_type, event_version, occurred_at, correlation_id,
                                     causation_id, producer, payload, trace_context)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    e.event_id,
                    e.event_type,
                    e.event_version,
                    e.occurred_at,
                    e.correlation_id,
                    e.causation_id,
                    e.producer,
                    Jsonb(e.payload),
                    Jsonb(trace_context) if trace_context else None,
                ),
            ).fetchone()
        ids.append(row["id"])
    return ids


def encode_event(envelope: EventEnvelope) -> bytes:
    return json.dumps(envelope.model_dump(mode="json"), ensure_ascii=False).encode()


class RelayDeliveryError(RuntimeError):
    """El broker no confirmó algún mensaje del lote; el lote se reintenta."""


def produce_event(producer: Producer, topic: str, envelope: EventEnvelope, trace_context, on_delivery) -> None:
    """Produce un mensaje dentro del span `<event_type> publish`, hijo del de escritura."""
    attributes = tracing.event_attributes(envelope) | {
        "messaging.system": "kafka",
        "messaging.destination.name": topic,
    }
    with tracing.tracer().start_as_current_span(
        f"{envelope.event_type} publish",
        context=tracing.extract(trace_context),
        kind=SpanKind.PRODUCER,
        attributes=attributes,
    ):
        headers = [("event_type", envelope.event_type.encode())]
        headers += [(k, v.encode()) for k, v in (tracing.inject_current() or {}).items()]
        producer.produce(
            topic,
            key=envelope.correlation_id.encode(),
            value=encode_event(envelope),
            headers=headers,
            on_delivery=on_delivery,
        )


def republish_order(rows: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Orden de republicación: primero las filas con posición en el topic perdido, por partición y offset (el orden
    que tenía cada partición, también entre componentes); después las que nunca se publicaron, por `created_at` y
    `id`."""
    def key(row: Mapping[str, Any]) -> tuple:
        positioned = row["kafka_partition"] is not None and row["kafka_offset"] is not None
        if positioned:
            return (0, row["kafka_partition"], row["kafka_offset"], row["created_at"], row["id"])
        return (1, 0, 0, row["created_at"], row["id"])

    return sorted(rows, key=key)


def republish_in_order(conn: psycopg.Connection, producer: Producer, schemas: Iterable[str], topic: str, *,
                       batch_size: int = 500, flush_timeout: float = 30.0) -> dict[str, int]:
    """Pérdida del topic (spec event-backbone): con el sistema parado, republica en `topic` todas las filas del outbox
    de `schemas` en el orden de `republish_order`, con un solo productor (Kafka conserva así el orden de cada
    partición), y guarda en cada fila su posición nueva y `published_at`. Devuelve las filas republicadas por esquema.

    Por lotes, pero las posiciones nuevas se guardan solo cuando se han confirmado todos, en una transacción: si un
    lote no se confirma, lanza `RelayDeliveryError` sin tocar ninguna fila, y el procedimiento se repite desde el
    principio (con el topic vacío otra vez) con el orden original intacto."""
    schemas = list(schemas)
    fields = ", ".join(_ENVELOPE_FIELDS)
    with conn.transaction():
        rows: list[Mapping[str, Any]] = []
        for schema in schemas:
            rows += [dict(r, schema=schema) for r in conn.execute(
                f"SELECT id, created_at, kafka_partition, kafka_offset, trace_context, {fields}"
                f" FROM {qualified(schema, 'outbox')}").fetchall()]
    counts = dict.fromkeys(schemas, 0)
    ordered = republish_order(rows)
    published: list[tuple[Mapping[str, Any], tuple[int, int]]] = []
    for start in range(0, len(ordered), batch_size):
        batch = ordered[start:start + batch_size]
        errors: list[KafkaError] = []
        positions: dict[int, tuple[int, int]] = {}

        def on_delivery_for(index: int, errors: list[KafkaError] = errors,
                            positions: dict[int, tuple[int, int]] = positions):
            def on_delivery(err: KafkaError | None, msg) -> None:
                if err is not None:
                    errors.append(err)
                else:
                    positions[index] = (msg.partition(), msg.offset())
            return on_delivery

        for index, row in enumerate(batch):
            envelope = EventEnvelope.model_validate({k: row[k] for k in _ENVELOPE_FIELDS})
            produce_event(producer, topic, envelope, row["trace_context"], on_delivery_for(index))
        pending = producer.flush(flush_timeout)
        if pending or errors:
            raise RelayDeliveryError(f"republicación no confirmada: pendientes={pending}, errores={errors}")
        published += [(row, positions[index]) for index, row in enumerate(batch)]
    with conn.transaction():
        for row, (partition, offset) in published:
            conn.execute(
                f"UPDATE {qualified(row['schema'], 'outbox')} SET published_at = now(),"
                " attempts = attempts + 1, kafka_partition = %s, kafka_offset = %s WHERE id = %s",
                (partition, offset, row["id"]))
            counts[row["schema"]] += 1
    return counts


class ComponentRelay:
    """Publica en Kafka el outbox y las dead letters de un componente."""

    def __init__(
        self,
        conn: psycopg.Connection,
        producer: Producer,
        schema: str,
        topic: str,
        *,
        dlq_topic: str | None = None,
        outbox: bool = True,
    ) -> None:
        self._conn = conn
        self._has_outbox = outbox  # False: solo publica dead letters (lado de lectura, sin outbox)
        self._producer = producer
        self._schema = schema
        self._outbox = qualified(schema, "outbox")
        self._dead_letters = qualified(schema, "dead_letters")
        self._topic = topic
        self._dlq_topic = dlq_topic

    @classmethod
    def from_settings(cls, settings: Settings, schema: str, *, outbox: bool = True) -> ComponentRelay:
        return cls(
            connect(settings.database_url),
            make_producer(settings.kafka_bootstrap_servers),
            schema,
            settings.events_topic,
            dlq_topic=settings.dlq_topic,
            outbox=outbox,
        )

    def run_once(self, batch_size: int = 100, flush_timeout: float = 30.0) -> int:
        """Publica un lote de pendientes. Devuelve cuántos publicó (0 si no había o no tiene outbox)."""
        if not self._has_outbox:
            return 0
        errors: list[KafkaError] = []
        positions: dict[int, tuple[int, int]] = {}

        def on_delivery_for(outbox_id: int):
            def on_delivery(err: KafkaError | None, msg) -> None:
                if err is not None:
                    errors.append(err)
                else:
                    positions[outbox_id] = (msg.partition(), msg.offset())

            return on_delivery

        with self._conn.transaction():
            rows = self._conn.execute(
                f"""
                SELECT id, trace_context, {", ".join(_ENVELOPE_FIELDS)}
                FROM {self._outbox} WHERE published_at IS NULL
                ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED
                """,
                (batch_size,),
            ).fetchall()
            if not rows:
                return 0
            for row in rows:
                envelope = EventEnvelope.model_validate({k: row[k] for k in _ENVELOPE_FIELDS})
                self._produce(envelope, row["trace_context"], on_delivery_for(row["id"]))
            pending = self._producer.flush(flush_timeout)
            if pending or errors:
                # Rollback: las filas siguen pendientes y el lote se republica.
                raise RelayDeliveryError(f"entrega no confirmada: pendientes={pending}, errores={errors}")
            ids = [row["id"] for row in rows]
            self._conn.execute(
                f"""
                UPDATE {self._outbox} o
                SET published_at = now(), attempts = o.attempts + 1,
                    kafka_partition = p.kafka_partition, kafka_offset = p.kafka_offset
                FROM unnest(%s::bigint[], %s::integer[], %s::bigint[]) AS p(id, kafka_partition, kafka_offset)
                WHERE o.id = p.id
                """,
                (
                    ids,
                    [positions.get(i, (None, None))[0] for i in ids],
                    [positions.get(i, (None, None))[1] for i in ids],
                ),
            )
        return len(rows)

    def _produce(self, envelope: EventEnvelope, trace_context, on_delivery) -> None:
        produce_event(self._producer, self._topic, envelope, trace_context, on_delivery)

    def publish_dead_letters(self, batch_size: int = 100, flush_timeout: float = 30.0) -> int:
        """Publica en `eda.dlq` las dead letters pendientes del componente. Al menos una vez."""
        if self._dlq_topic is None:
            return 0
        errors: list[KafkaError] = []

        def on_delivery(err: KafkaError | None, _msg) -> None:
            if err is not None:
                errors.append(err)

        with self._conn.transaction():
            rows = self._conn.execute(
                f"""
                SELECT id, consumer, event_id, correlation_id, reason, raw_message, created_at
                FROM {self._dead_letters} WHERE published_at IS NULL
                ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED
                """,
                (batch_size,),
            ).fetchall()
            if not rows:
                return 0
            for row in rows:
                body = {k: (str(v) if k in ("event_id", "created_at") and v is not None else v) for k, v in row.items()}
                body["component"] = self._schema
                self._producer.produce(
                    self._dlq_topic,
                    key=(row["correlation_id"] or "").encode(),
                    value=json.dumps(body, ensure_ascii=False).encode(),
                    headers=[("event_type", b"DeadLetter"), ("reason", row["reason"].encode())],
                    on_delivery=on_delivery,
                )
            pending = self._producer.flush(flush_timeout)
            if pending or errors:
                raise RelayDeliveryError(f"dead letters sin confirmar: pendientes={pending}, errores={errors}")
            self._conn.execute(
                f"UPDATE {self._dead_letters} SET published_at = now() WHERE id = ANY(%s)",
                ([r["id"] for r in rows],),
            )
        return len(rows)

    def replay_from(self, outbox_id: int) -> int:
        """Marca como pendientes las filas desde `outbox_id` (republicación tras perder el topic)."""
        with self._conn.transaction():
            cur = self._conn.execute(
                f"UPDATE {self._outbox} SET published_at = NULL WHERE id >= %s", (outbox_id,)
            )
            return cur.rowcount

    def run_until(self, stop: threading.Event, idle_sleep: float = 0.1, error_sleep: float = 2.0) -> None:
        while not stop.is_set():
            try:
                published, dead = self.run_once(), self.publish_dead_letters()
            except RelayDeliveryError as exc:
                log.warning("relay de %s: %s; se reintenta", self._schema, exc)
                published = dead = 0
            except Exception:  # base de datos caída, cola del productor llena…: el hilo no debe morir
                log.exception("relay de %s: error; se reintenta en %.1f s", self._schema, error_sleep)
                stop.wait(error_sleep)
                continue
            if published + dead == 0:
                stop.wait(idle_sleep)

    def close(self) -> None:
        self._producer.flush(10)
        self._conn.close()


class RelayThread(threading.Thread):
    """Relay de un componente en un hilo de su proceso. `stop()` lo detiene tras el lote en curso."""

    def __init__(self, relay: ComponentRelay, *, idle_sleep: float = 0.1) -> None:
        super().__init__(name="outbox-relay", daemon=True)
        self._relay = relay
        self._idle_sleep = idle_sleep
        self._stop_requested = threading.Event()

    def run(self) -> None:
        try:
            self._relay.run_until(self._stop_requested, self._idle_sleep)
        finally:
            self._relay.close()

    def stop(self, timeout: float | None = None) -> None:
        self._stop_requested.set()
        self.join(timeout)


def start_relay(settings: Settings, schema: str, *, outbox: bool = True) -> RelayThread:
    """Arranca el relay del componente `schema` con su propia conexión y su propio productor.

    Con `outbox=False` solo publica las dead letters del esquema (lado de lectura: `event_log`, `views`).
    """
    thread = RelayThread(ComponentRelay.from_settings(settings, schema, outbox=outbox))
    thread.start()
    return thread
