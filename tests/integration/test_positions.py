"""Posición en Kafka guardada en PostgreSQL (spec cqrs-views, "Vistas en PostgreSQL con posición en Kafka")."""

from __future__ import annotations

import json
import uuid

import pytest

from eda_retail.infra.kafka import make_consumer, make_producer
from eda_retail.infra.positions import PositionedConsumer


def _produce(settings, count: int, key: str = "I-1") -> None:
    producer = make_producer(settings.kafka_bootstrap_servers)
    for i in range(count):
        producer.produce(settings.events_topic, key=key.encode(), value=json.dumps({"n": i}).encode())
    assert producer.flush(30) == 0


def _consumer(settings, conn, name: str, applied: list[int], fail_on: int | None = None) -> PositionedConsumer:
    def apply(_conn, msg) -> None:
        n = json.loads(msg.value())["n"]
        if n == fail_on:
            raise RuntimeError("fallo al aplicar")
        applied.append(n)

    return PositionedConsumer(
        conn, make_consumer(settings.kafka_bootstrap_servers, group_id=name), settings.events_topic,
        schema="views", consumer=name, apply=apply,
    )


def _stored(conn, name: str, topic: str) -> dict[int, int]:
    rows = conn.execute(
        "SELECT kafka_partition, kafka_offset FROM views.offsets WHERE consumer = %s AND kafka_topic = %s",
        (name, topic),
    ).fetchall()
    conn.commit()
    return {r["kafka_partition"]: r["kafka_offset"] for r in rows}


def test_restart_continues_from_the_next_offset_of_each_partition(settings, connect_db):
    """Reinicio de un proyector: continúa desde el offset siguiente y no aplica dos veces lo anterior"""
    name = f"test-{uuid.uuid4().hex[:8]}"
    _produce(settings, 3)
    first: list[int] = []
    consumer = _consumer(settings, connect_db(), name, first)
    assert consumer.catch_up() == 3
    consumer.close()
    [(partition, offset)] = _stored(connect_db(), name, settings.events_topic).items()
    assert offset == 2

    _produce(settings, 1)
    second: list[int] = []
    restarted = _consumer(settings, connect_db(), name, second)
    assert restarted.catch_up() == 1
    restarted.close()
    assert (first, second) == ([0, 1, 2], [0])
    assert _stored(connect_db(), name, settings.events_topic) == {partition: 3}


def test_redelivered_message_is_not_applied_twice(settings, connect_db):
    """Reinicio de un proyector: un mensaje que Kafka reentrega no se aplica otra vez"""
    name = f"test-{uuid.uuid4().hex[:8]}"
    _produce(settings, 1)
    applied: list[int] = []
    consumer = _consumer(settings, connect_db(), name, applied)
    consumer.assign()
    [msg] = [m for m in consumer._kafka.consume(1, 10)]
    assert consumer.handle(msg) is True
    assert consumer.handle(msg) is False
    consumer.close()
    assert applied == [0]


def test_failed_apply_keeps_the_offset_and_retries_the_message(settings, connect_db):
    """La vista y el offset van en la misma transacción: si falla la vista, el offset no avanza"""
    name = f"test-{uuid.uuid4().hex[:8]}"
    _produce(settings, 2)
    applied: list[int] = []
    consumer = _consumer(settings, connect_db(), name, applied, fail_on=1)
    with pytest.raises(RuntimeError):
        consumer.catch_up()
    consumer.close()
    assert applied == [0]
    assert list(_stored(connect_db(), name, settings.events_topic).values()) == [0]

    retried: list[int] = []
    healed = _consumer(settings, connect_db(), name, retried)
    assert healed.catch_up() == 1
    healed.close()
    assert retried == [1]


def test_reset_empties_the_position_and_reconsumes_from_the_beginning(settings, connect_db):
    """Reconstrucción: sin posición, se vuelve a consumir el topic desde el principio"""
    name = f"test-{uuid.uuid4().hex[:8]}"
    _produce(settings, 2)
    applied: list[int] = []
    consumer = _consumer(settings, connect_db(), name, applied)
    consumer.catch_up()
    consumer.reset(tables=[])
    assert consumer.catch_up() == 2
    consumer.close()
    assert applied == [0, 1, 0, 1]
