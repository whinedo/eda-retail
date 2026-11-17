"""Posición en Kafka de los componentes de consulta (spec cqrs-views, "Vistas en PostgreSQL con
posición en Kafka").

Sin PostgreSQL ni Kafka: se comprueban las decisiones puras del consumidor posicionado. La
transacción con la vista y el offset está en tests/integration/test_positions.py.
"""

from __future__ import annotations

from types import SimpleNamespace

from confluent_kafka import OFFSET_BEGINNING

from eda_retail.infra import positions


class FakeMessage:
    def __init__(self, *, topic="eda.events", partition=1, offset=121, headers=None):
        self._topic, self._partition, self._offset, self._headers = topic, partition, offset, headers

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def headers(self):
        return self._headers


def test_restart_continues_after_the_stored_offset_of_each_partition():
    """Reinicio de un proyector: continúa desde el offset 121 de la partición 1"""
    start = positions.start_offsets({1: 120}, partitions=[0, 1, 2])
    assert start == {0: OFFSET_BEGINNING, 1: 121, 2: OFFSET_BEGINNING}


def test_redelivered_message_is_not_newer_than_the_stored_offset():
    """Reinicio de un proyector: no aplica dos veces lo que Kafka reentrega"""
    assert positions.is_new(121, stored=120)
    assert not positions.is_new(120, stored=120)
    assert not positions.is_new(37, stored=120)
    assert positions.is_new(0, stored=None)


def test_position_of_a_message():
    assert positions.position_of(FakeMessage()) == positions.Position("eda.events", 1, 121)


def test_trace_context_comes_from_the_w3c_headers():
    traceparent = "00-" + "a" * 32 + "-" + "b" * 16 + "-01"
    msg = FakeMessage(headers=[("event_type", b"PlanCreated"), ("traceparent", traceparent.encode()),
                               ("tracestate", b"k=v")])
    assert positions.trace_context_of(msg) == {"traceparent": traceparent, "tracestate": "k=v"}


def test_message_without_trace_headers_has_no_trace_context():
    assert positions.trace_context_of(FakeMessage(headers=[("event_type", b"PlanCreated")])) is None
    assert positions.trace_context_of(FakeMessage(headers=None)) is None


def test_caught_up_when_every_partition_reached_its_high_watermark():
    """Reconstrucción: se consume hasta las marcas de agua altas de cada partición"""
    highs = {0: 0, 1: 5, 2: 3}  # la partición 0 está vacía
    assert positions.caught_up({1: 4, 2: 2}, highs)
    assert not positions.caught_up({1: 4, 2: 1}, highs)
    assert not positions.caught_up({}, highs)
    assert positions.caught_up({}, {0: 0})


def test_pending_messages_until_the_high_watermarks():
    """Grupo reiniciado al principio: el lag es lo que falta por leer hasta las marcas de agua altas"""
    highs = {0: 0, 1: 5, 2: 3}
    assert positions.pending_messages({}, highs) == 8
    assert positions.pending_messages({1: 4, 2: 0}, highs) == 2
    assert positions.pending_messages({1: 4, 2: 2}, highs) == 0


class FakeKafka:
    def __init__(self, partitions):
        self._partitions = partitions
        self.assigned = None

    def list_topics(self, topic, timeout):
        partitions = SimpleNamespace(partitions=dict.fromkeys(self._partitions))
        return SimpleNamespace(topics={topic: partitions})

    def assign(self, tps):
        self.assigned = [(tp.topic, tp.partition, tp.offset) for tp in tps]


def test_rewind_reads_every_partition_from_offset_zero():
    """Grupo reiniciado al principio: el lado de lectura vuelve a recibir el topic desde el offset 0"""
    kafka = FakeKafka([0, 1, 2])
    consumer = positions.PositionedConsumer(None, kafka, "eda.events", schema="views", consumer="v",
                                            apply=lambda conn, msg: None)
    consumer.rewind()
    assert kafka.assigned == [("eda.events", 0, 0), ("eda.events", 1, 0), ("eda.events", 2, 0)]


class _Watermarks:
    """Consumer de prueba: falla `errors` veces con un error de Kafka y después da la marca de agua alta 7."""

    def __init__(self, *errors: int) -> None:
        self._errors = list(errors)
        self.calls = 0

    def get_watermark_offsets(self, tp, timeout):
        from confluent_kafka import KafkaError, KafkaException

        self.calls += 1
        if self._errors:
            raise KafkaException(KafkaError(self._errors.pop(0)))
        return (0, 7)


def test_high_watermark_retries_while_the_partition_has_no_leader():
    """Justo después de crear un topic, Kafka aún elige el líder de la partición: la marca de agua se reintenta"""
    from confluent_kafka import KafkaError, TopicPartition

    kafka = _Watermarks(KafkaError.NOT_LEADER_FOR_PARTITION, KafkaError.LEADER_NOT_AVAILABLE)
    slept = []
    assert positions.high_watermark(kafka, TopicPartition("t", 0), timeout=10, sleep=slept.append) == 7
    assert kafka.calls == 3 and len(slept) == 2


def test_high_watermark_does_not_retry_other_errors_or_past_the_timeout():
    """Otro error de Kafka, o el mismo error transitorio más allá del plazo, se propaga"""
    import pytest
    from confluent_kafka import KafkaError, KafkaException, TopicPartition

    with pytest.raises(KafkaException):
        positions.high_watermark(_Watermarks(KafkaError._ALL_BROKERS_DOWN), TopicPartition("t", 0), timeout=10,
                                 sleep=lambda s: None)
    with pytest.raises(KafkaException):
        positions.high_watermark(_Watermarks(*[KafkaError.NOT_LEADER_FOR_PARTITION] * 1000), TopicPartition("t", 0),
                                 timeout=0, sleep=lambda s: None)
