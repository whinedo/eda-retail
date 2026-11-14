"""Posición de Kafka que guarda el relay (spec event-backbone, "Kafka como bus de entrega").

Sin PostgreSQL ni Kafka: conexión y productor falsos. El escenario completo, contra los
servicios reales, está en tests/integration/test_relay.py.
"""

from __future__ import annotations

import contextlib
import uuid
from datetime import UTC, datetime

import pytest

from eda_retail.infra.relay import OutboxRelay, RelayDeliveryError


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.updates: list[tuple[str, tuple]] = []

    @contextlib.contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=()):
        if sql.lstrip().startswith("UPDATE"):
            self.updates.append((sql, params))
            return _Result([])
        return _Result(self.rows)


class _Msg:
    def __init__(self, partition: int, offset: int):
        self._p, self._o = partition, offset

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o


class FakeProducer:
    """Confirma cada mensaje con la partición indicada y offsets crecientes por partición."""

    def __init__(self, partitions: dict[str, int], fail: bool = False):
        self._partitions = partitions
        self._next: dict[int, int] = {}
        self._fail = fail
        self._pending: list = []

    def produce(self, topic, key, value, headers, on_delivery):
        partition = self._partitions[key.decode()]
        offset = self._next.get(partition, 100)
        self._next[partition] = offset + 1
        self._pending.append((on_delivery, _Msg(partition, offset)))

    def flush(self, timeout=None):
        for callback, msg in self._pending:
            callback("broker caído" if self._fail else None, msg)
        self._pending = []
        return 0


def _row(seq: int, incident: str) -> dict:
    return {
        "seq": seq,
        "trace_context": None,
        "event_id": uuid.uuid4(),
        "event_type": "ShipmentDelayed",
        "event_version": 1,
        "occurred_at": datetime.now(UTC),
        "correlation_id": incident,
        "causation_id": None,
        "producer": "test",
        "payload": {"incident_id": incident},
    }


def test_relay_records_the_confirmed_partition_and_offset():
    """Posición del mensaje publicado: cada fila guarda la partición y el offset que confirmó el broker"""
    conn = FakeConn([_row(7, "I-a"), _row(8, "I-b"), _row(9, "I-a")])
    relay = OutboxRelay(conn, FakeProducer({"I-a": 2, "I-b": 0}), "eda.events")  # type: ignore[arg-type]

    assert relay.run_once() == 3

    [(sql, params)] = conn.updates
    assert "kafka_partition" in sql and "kafka_offset" in sql
    assert dict(zip(params[0], zip(params[1], params[2], strict=True), strict=True)) == {
        7: (2, 100),
        8: (0, 100),
        9: (2, 101),
    }


def test_unconfirmed_batch_records_no_position():
    """Si el broker no confirma, el lote se reintenta y no se guarda ninguna posición"""
    conn = FakeConn([_row(7, "I-a")])
    relay = OutboxRelay(conn, FakeProducer({"I-a": 2}, fail=True), "eda.events")  # type: ignore[arg-type]

    with pytest.raises(RelayDeliveryError):
        relay.run_once()

    assert conn.updates == []
