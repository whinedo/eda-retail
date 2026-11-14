"""Posición en Kafka de los componentes de consulta (spec cqrs-views, "Vistas en PostgreSQL con
posición en Kafka").

Sin PostgreSQL ni Kafka: se comprueban las decisiones puras del consumidor posicionado. La
transacción con la vista y el offset está en tests/integration/test_positions.py.
"""

from __future__ import annotations

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
