"""Commit del offset con plazo (spec event-backbone, «Entrega al menos una vez»): el consumidor no se queda esperando
sin límite a un commit que el broker no confirma. Sin Kafka: se sustituye el consumidor de librdkafka."""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaException, TopicPartition

from eda_retail.infra.consumer import EventConsumer


class _Kafka:
    """Guarda el offset confirmado del grupo; con `confirm=False`, el broker nunca aplica los commits."""

    def __init__(self, *, confirm: bool) -> None:
        self.confirm = confirm
        self.commits: list[tuple[int, bool]] = []
        self.stored = -1001  # OFFSET_INVALID: el grupo aún no ha confirmado nada

    def subscribe(self, *_args, **_kwargs) -> None:
        pass

    def commit(self, message, asynchronous: bool) -> None:
        self.commits.append((message.offset(), asynchronous))
        if self.confirm:
            self.stored = message.offset() + 1

    def committed(self, partitions, timeout: float):
        [tp] = partitions
        return [TopicPartition(tp.topic, tp.partition, self.stored)]


def _message(offset: int) -> SimpleNamespace:
    return SimpleNamespace(topic=lambda: "t", partition=lambda: 0, offset=lambda: offset, error=lambda: None)


def _consumer(kafka: _Kafka, **kwargs) -> EventConsumer:
    return EventConsumer("interpreter", object(), kafka, "t", {}, schema="interpreter", **kwargs)


def test_an_unconfirmed_offset_commit_fails_within_its_timeout():
    """Commit del offset sin confirmar: el consumidor cae con KafkaException al vencer su plazo, sin esperar más"""
    kafka = _Kafka(confirm=False)
    consumer = _consumer(kafka, commit_timeout_s=0.3)

    start = time.monotonic()
    with pytest.raises(KafkaException):
        consumer.commit(_message(7))
    elapsed = time.monotonic() - start

    assert 0.3 <= elapsed < 2.0
    assert kafka.commits == [(7, True)]  # no se queda bloqueado dentro de librdkafka
    assert kafka.stored == -1001


def test_a_confirmed_offset_commit_returns_at_once():
    """Commit del offset confirmado: vuelve en cuanto el grupo tiene el offset siguiente al mensaje"""
    kafka = _Kafka(confirm=True)
    consumer = _consumer(kafka, commit_timeout_s=5.0)

    start = time.monotonic()
    consumer.commit(_message(7))

    assert time.monotonic() - start < 1.0
    assert kafka.commits == [(7, True)] and kafka.stored == 8
