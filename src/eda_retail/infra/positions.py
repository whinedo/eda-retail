"""Consumidor de Kafka con la posición en PostgreSQL, para los componentes de consulta (design.md §8,
spec cqrs-views "Vistas en PostgreSQL con posición en Kafka" y "Reconstrucción de vistas").

`event_log` y cada proyector guardan en `<schema>.offsets` el último offset aplicado de cada
partición, en la misma transacción que su vista. Por eso no se confirma nada en Kafka: al
arrancar se asignan todas las particiones del topic en el offset guardado + 1, y un mensaje cuyo
offset no es posterior al guardado (Kafka lo reentrega) no se aplica otra vez.

Supuesto (odd/tasks/eda-component-ownership.md, L21): el número de particiones del topic no
cambia mientras el proceso corre; una partición nueva exige reiniciarlo.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

import psycopg
from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaError, KafkaException, Message, TopicPartition

from eda_retail.infra.db import CONNECTION_ERRORS, RETRY_S, Reconnect, healthy, reopen
from eda_retail.infra.schemas import qualified

log = logging.getLogger(__name__)

# Aplica un mensaje a la vista dentro de la transacción abierta. Un mensaje que no se puede
# aplicar (envenenado) se ignora sin lanzar: su offset avanza igual.
ApplyMessage = Callable[[psycopg.Connection, Message], None]

_TRACE_HEADERS = ("traceparent", "tracestate")
_METADATA_TIMEOUT = 10.0
# Errores de Kafka transitorios al pedir una marca de agua: la partición aún no tiene líder (p. ej. justo después de
# crear el topic) o el cliente aún no conoce su metadata. `retriable()` no los marca, así que van en una lista.
_TRANSIENT_WATERMARK_ERRORS = frozenset({KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.LEADER_NOT_AVAILABLE,
                                         KafkaError.UNKNOWN_TOPIC_OR_PART})
_WATERMARK_RETRY_S = 0.2


def high_watermark(kafka: Consumer, tp: TopicPartition, *, timeout: float = _METADATA_TIMEOUT,
                   sleep: Callable[[float], None] = time.sleep) -> int:
    """La marca de agua alta de `tp`. Reintenta los errores transitorios de líder y metadata hasta `timeout`
    segundos (cada intento espera solo lo que queda del plazo); otro error, o el mismo más allá del plazo, se
    propaga."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            return kafka.get_watermark_offsets(tp, timeout=max(deadline - time.monotonic(), 0.1))[1]
        except KafkaException as exc:
            if exc.args[0].code() not in _TRANSIENT_WATERMARK_ERRORS or time.monotonic() >= deadline:
                raise
            sleep(_WATERMARK_RETRY_S)


@dataclass(frozen=True)
class Position:
    topic: str
    partition: int
    offset: int


def position_of(msg: Message) -> Position:
    return Position(msg.topic(), msg.partition(), msg.offset())


def trace_context_of(msg: Message) -> dict[str, str] | None:
    """Contexto W3C (`traceparent`, `tracestate`) de las cabeceras, o None si no viene."""
    headers = {k: v.decode() for k, v in (msg.headers() or []) if k in _TRACE_HEADERS and v is not None}
    return headers or None


def start_offsets(stored: Mapping[int, int], partitions: Iterable[int]) -> dict[int, int]:
    """Offset por el que empezar cada partición: el siguiente al guardado, o el principio."""
    return {p: stored[p] + 1 if stored.get(p, -1) >= 0 else OFFSET_BEGINNING for p in partitions}


def is_new(offset: int, stored: int | None) -> bool:
    """True si el mensaje va después del último offset aplicado de su partición."""
    return stored is None or offset > stored


def caught_up(applied: Mapping[int, int], highs: Mapping[int, int]) -> bool:
    """True si cada partición llegó a su marca de agua alta (`high` = offset siguiente al último)."""
    return all(applied.get(p, -1) >= high - 1 for p, high in highs.items() if high > 0)


def pending_messages(consumed: Mapping[int, int], highs: Mapping[int, int]) -> int:
    """Mensajes que faltan por leer hasta la marca de agua alta de cada partición (el lag)."""
    return sum(max(0, high - 1 - consumed.get(p, -1)) for p, high in highs.items())


def positioned_lags(rows: Iterable[Mapping[str, Any]], highs: Mapping[int, int],
                    consumers: Iterable[str]) -> dict[str, int]:
    """Lag de cada consumidor posicionado (`event_log`, cada proyector) desde sus filas de `*.offsets` (consumidor,
    partición y último offset aplicado) frente a las marcas de agua altas; sin fila, la partición está sin leer."""
    consumed: dict[str, dict[int, int]] = {c: {} for c in consumers}
    for r in rows:
        if r["consumer"] in consumed:
            consumed[r["consumer"]][r["kafka_partition"]] = r["kafka_offset"]
    return {c: pending_messages(offsets, highs) for c, offsets in consumed.items()}


class PositionedConsumer:
    """Consume `topic` y aplica cada mensaje junto con su offset en una transacción de PostgreSQL."""

    def __init__(
        self,
        conn: psycopg.Connection,
        kafka_consumer: Consumer,
        topic: str,
        *,
        schema: str,
        consumer: str,
        apply: ApplyMessage,
        reconnect: Reconnect | None = None,
    ) -> None:
        self.consumer = consumer
        self._conn = conn
        self._reconnect = reconnect  # None: conserva su conexión (harness en proceso); si no, `recover` abre otra
        self._kafka = kafka_consumer
        self._topic = topic
        self._offsets = qualified(schema, "offsets")
        self._apply = apply
        self._consumed: dict[int, int] = {}  # último offset leído por partición (aplicado o no)
        self._assigned = False

    # -- posición guardada -------------------------------------------------------------
    def stored_offsets(self) -> dict[int, int]:
        with self._conn.transaction():
            rows = self._conn.execute(
                f"SELECT kafka_partition, kafka_offset FROM {self._offsets} WHERE consumer = %s AND kafka_topic = %s",
                (self.consumer, self._topic),
            ).fetchall()
        return {r["kafka_partition"]: r["kafka_offset"] for r in rows if r["kafka_offset"] >= 0}

    def partitions(self) -> list[int]:
        metadata = self._kafka.list_topics(self._topic, timeout=_METADATA_TIMEOUT)
        return sorted(metadata.topics[self._topic].partitions)

    def assign(self) -> None:
        """(Re)asigna todas las particiones en la posición guardada."""
        stored = self.stored_offsets()
        start = start_offsets(stored, self.partitions())
        self._kafka.assign([TopicPartition(self._topic, p, offset) for p, offset in start.items()])
        self._consumed = dict(stored)
        self._assigned = True

    def rewind(self) -> None:
        """Vuelve a leer todas las particiones desde el offset 0 sin tocar la posición guardada: lo ya
        aplicado se descarta en `handle` (reproducción del log, E21)."""
        self._kafka.assign([TopicPartition(self._topic, p, 0) for p in self.partitions()])
        self._consumed = {}
        self._assigned = True

    def lag(self) -> int:
        """Mensajes que faltan por leer hasta las marcas de agua altas de ahora."""
        return pending_messages(self._consumed, self.high_watermarks())

    # -- aplicación ----------------------------------------------------------------------
    def handle(self, msg: Message) -> bool:
        """Aplica el mensaje y guarda su offset en la misma transacción. False si ya estaba aplicado."""
        key = (self.consumer, msg.topic(), msg.partition())
        with self._conn.transaction():
            self._conn.execute(
                f"INSERT INTO {self._offsets} (consumer, kafka_topic, kafka_partition, kafka_offset) "
                "VALUES (%s, %s, %s, -1) ON CONFLICT DO NOTHING",
                key,
            )
            stored = self._conn.execute(
                f"SELECT kafka_offset FROM {self._offsets} "
                "WHERE consumer = %s AND kafka_topic = %s AND kafka_partition = %s FOR UPDATE",
                key,
            ).fetchone()["kafka_offset"]
            if not is_new(msg.offset(), stored):
                return False
            self._apply(self._conn, msg)
            self._conn.execute(
                f"UPDATE {self._offsets} SET kafka_offset = %s "
                "WHERE consumer = %s AND kafka_topic = %s AND kafka_partition = %s",
                (msg.offset(), *key),
            )
        return True

    def run_once(self, timeout: float = 1.0, max_messages: int = 500) -> int:
        """Lee un lote y lo aplica; devuelve cuántos mensajes aplicó."""
        if not self._assigned:
            self.assign()
        applied = 0
        for msg in self._kafka.consume(max_messages, timeout):
            if msg.error():
                log.warning("%s: error de Kafka: %s", self.consumer, msg.error())
                continue
            try:
                applied += self.handle(msg)
            except Exception:
                # La transacción se deshizo: se vuelve a leer este mensaje en la siguiente vuelta.
                self._kafka.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                raise
            self._consumed[msg.partition()] = msg.offset()
        return applied

    def recover(self) -> None:
        """Tras un error: si la conexión se perdió y hay `reconnect`, abre otra y vuelve a asignar las particiones en
        la posición guardada (spec resilience, «Reconexión a PostgreSQL»). Si la base aún no responde, el error de
        conexión sube y quien llama lo vuelve a intentar."""
        if self._reconnect is None or healthy(self._conn):
            return
        self._conn = reopen(self._conn, self._reconnect)
        self._assigned = False  # `run_once` reasigna desde `views.offsets`/`event_log.offsets` con la conexión nueva

    def high_watermarks(self) -> dict[int, int]:
        return {p: high_watermark(self._kafka, TopicPartition(self._topic, p)) for p in self.partitions()}

    def catch_up(self, timeout: float = 60.0) -> int:
        """Consume hasta las marcas de agua altas de ahora; devuelve cuántos mensajes aplicó."""
        if not self._assigned:
            self.assign()
        highs = self.high_watermarks()
        deadline = time.monotonic() + timeout
        applied = 0
        while not caught_up(self._consumed, highs):
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{self.consumer}: no llegó a {highs} en {timeout} s")
            applied += self.run_once(timeout=0.5)
        return applied

    def reset(self, tables: Iterable[str]) -> None:
        """Vacía las tablas de la vista y su posición, y vuelve a asignar desde el principio."""
        with self._conn.transaction():
            for table in tables:
                self._conn.execute(f"DELETE FROM {table}")
            self._conn.execute(
                f"DELETE FROM {self._offsets} WHERE consumer = %s AND kafka_topic = %s", (self.consumer, self._topic)
            )
        self.assign()

    def close(self) -> None:
        self._kafka.close()


def run_resilient(consumer: PositionedConsumer, logger: logging.Logger, *, timeout: float = 0.0,
                  retry_s: float = RETRY_S) -> int:
    """`consumer.run_once`; si la base cae, espera `retry_s`, recupera la conexión (`recover`) y devuelve 0: el
    mensaje que falló se vuelve a leer en la siguiente vuelta. Cualquier otro error sube (spec resilience,
    «Reconexión a PostgreSQL»)."""
    try:
        return consumer.run_once(timeout=timeout)
    except CONNECTION_ERRORS:
        logger.warning("%s: la base no responde; se reintenta", consumer.consumer, exc_info=True)
    time.sleep(retry_s)
    try:
        consumer.recover()
    except CONNECTION_ERRORS:
        logger.warning("%s: sin conexión a la base; se reintenta", consumer.consumer)
    return 0
