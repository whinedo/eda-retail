"""Mensajes veneno inyectados (spec synthetic-data, «Mensajes veneno inyectados»).

Sin Kafka: qué publica cada veneno y que la inyección anota su partición y su offset.
"""

from __future__ import annotations

import json

import pytest

from eda_retail.infra.consumer import InvalidMessage, decode_checked
from eda_retail.synthetic import poison


class FakeMessage:
    def __init__(self, value: bytes):
        self._value = value

    def value(self):
        return self._value


class _Delivered:
    def __init__(self, partition: int, offset: int):
        self._p, self._o = partition, offset

    def partition(self):
        return self._p

    def offset(self):
        return self._o


class FakeProducer:
    """Una partición con carga: cada partición empieza con 100 mensajes válidos."""

    def __init__(self):
        self.next_offset = {0: 100, 1: 100, 2: 100}
        self.produced: list[dict] = []
        self._pending = []

    def produce(self, topic, key, value, partition, on_delivery):
        offset = self.next_offset[partition]
        self.next_offset[partition] += 1
        self.produced.append({"topic": topic, "key": key, "value": value, "partition": partition})
        self._pending.append((on_delivery, _Delivered(partition, offset)))

    def flush(self, timeout=None):
        for callback, msg in self._pending:
            callback(None, msg)
        self._pending = []
        return 0


INCIDENTS = [f"I-{n}" for n in range(6)]


def test_every_kind_of_poison_is_invalid_except_the_unknown_type():
    """JSON inválido, campo obligatorio, versión sin upcaster, schema, datos personales y payload grande son
    inválidos; el event_type desconocido tiene la estructura común válida"""
    for p in poison.poison_set(7, INCIDENTS):
        if p.kind == poison.UNKNOWN_TYPE:
            assert decode_checked(FakeMessage(p.value)).event_type == poison.UNKNOWN_EVENT_TYPE  # type: ignore[arg-type]
            continue
        with pytest.raises(InvalidMessage) as bad:
            decode_checked(FakeMessage(p.value))  # type: ignore[arg-type]
        assert bad.value.event_type == p.event_type, p.kind
        assert bad.value.event_id == p.event_id and bad.value.correlation_id == p.correlation_id, p.kind


def test_poisons_with_a_readable_type_carry_an_incident():
    poisons = poison.poison_set(7, INCIDENTS)
    assert [p.kind for p in poisons] == list(poison.KINDS)
    [invalid_json] = [p for p in poisons if p.kind == poison.INVALID_JSON]
    assert (invalid_json.event_type, invalid_json.correlation_id, invalid_json.event_id) == (None, None, None)
    others = [p for p in poisons if p.kind != poison.INVALID_JSON]
    assert [p.correlation_id for p in others] == INCIDENTS
    assert {p.key for p in others} == set(INCIDENTS)


def test_large_payload_stays_below_the_broker_limit():
    [large] = [p for p in poison.poison_set(7, INCIDENTS) if p.kind == poison.LARGE_PAYLOAD]
    assert 900_000 <= len(large.value) < 1_000_000


def test_same_seed_same_poisons():
    """Misma semilla, mismos venenos: los mensajes y sus event_id coinciden"""
    assert poison.poison_set(7, INCIDENTS) == poison.poison_set(7, INCIDENTS)
    assert poison.poison_set(7, INCIDENTS)[1].event_id != poison.poison_set(8, INCIDENTS)[1].event_id


def test_poison_in_the_middle_of_a_partition():
    """Veneno en mitad de la partición: el informe guarda su partición y su offset"""
    producer = FakeProducer()
    poisons = poison.poison_set(7, INCIDENTS)
    injected = poison.inject(producer, "eda.events", poisons, partitions=[0, 1, 2])
    assert [(i.partition, i.offset) for i in injected] == [
        (0, 100), (1, 100), (2, 100), (0, 101), (1, 101), (2, 101), (0, 102)
    ]
    assert [i.poison for i in injected] == poisons
    assert {m["topic"] for m in producer.produced} == {"eda.events"}


def test_raw_dead_letter_matches_what_the_consumer_stores():
    for p in poison.poison_set(7, INCIDENTS):
        if p.kind == poison.INVALID_JSON:
            with pytest.raises(InvalidMessage) as bad:
                decode_checked(FakeMessage(p.value))  # type: ignore[arg-type]
            assert poison.raw_dead_letter(p) == bad.value.raw
        else:
            assert poison.raw_dead_letter(p) == json.loads(p.value)
