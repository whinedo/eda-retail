"""Outbox y relay de cada componente (spec event-backbone, "Publicación atómica con el estado
(Outbox)" y "Kafka como bus de entrega"; design.md §5).

Sin PostgreSQL ni Kafka: conexión y productor falsos. Los escenarios contra los servicios
reales están en tests/integration/test_component_outbox.py.
"""

from __future__ import annotations

import contextlib
import json
import threading
import uuid
from datetime import UTC, datetime

import pytest

from eda_retail.events.envelope import new_event
from eda_retail.infra import outbox
from eda_retail.infra.event_store import InvalidEventError
from eda_retail.infra.outbox import RelayDeliveryError


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    @property
    def rowcount(self):
        return len(self._rows)


class FakeConn:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.statements: list[tuple[str, tuple]] = []
        self._next_id = 1

    @contextlib.contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=()):
        self.statements.append((" ".join(sql.split()), params))
        if sql.lstrip().upper().startswith("INSERT"):
            row = {"id": self._next_id}
            self._next_id += 1
            return _Result([row])
        if sql.lstrip().upper().startswith("SELECT"):
            rows, self.rows = self.rows, []
            return _Result(rows)
        return _Result([])

    def close(self):
        self.closed = True

    def writes(self):
        return [(s, p) for s, p in self.statements if not s.upper().startswith("SELECT")]


class _Msg:
    def __init__(self, partition, offset):
        self._p, self._o = partition, offset

    def partition(self):
        return self._p

    def offset(self):
        return self._o


class FakeProducer:
    def __init__(self, fail=False):
        self.fail = fail
        self.produced: list[dict] = []
        self._pending = []

    def produce(self, topic, key, value, headers, on_delivery):
        offset = len(self.produced)
        self.produced.append({"topic": topic, "key": key, "value": value, "headers": dict(headers)})
        self._pending.append((on_delivery, _Msg(1, offset)))

    def flush(self, timeout=None):
        for callback, msg in self._pending:
            callback("broker caído" if self.fail else None, msg)
        self._pending = []
        return 0


def _event(incident="I-1", event_type="PlanCreated"):
    return new_event(
        event_type=event_type, correlation_id=incident, causation_id=None, producer="planner",
        payload={"incident_id": incident},
    )


def _row(outbox_id, incident="I-1"):
    e = _event(incident)
    return {"id": outbox_id, "trace_context": None, **e.model_dump()}


def test_append_writes_the_event_into_the_component_outbox():
    """Publicación atómica: el evento se escribe en el outbox del propio componente"""
    conn = FakeConn()
    event = _event()

    ids = outbox.append(conn, "planner", [event])

    [(sql, params)] = conn.writes()
    assert sql.startswith("INSERT INTO planner.outbox")
    assert event.event_id in params and event.event_type in params
    assert ids == [1]


def test_append_never_touches_the_shared_tables():
    conn = FakeConn()
    outbox.append(conn, "planner", [_event(), _event()])
    for sql, _ in conn.writes():
        assert sql.startswith("INSERT INTO planner.outbox")


def test_append_rejects_an_incomplete_event():
    """Evento sin correlation_id: se rechaza y no se escribe en el outbox"""
    conn = FakeConn()
    body = _event().model_dump()
    del body["correlation_id"]
    with pytest.raises(InvalidEventError, match="correlation_id"):
        outbox.append(conn, "planner", [body])
    assert conn.writes() == []


def test_append_rejects_an_invalid_schema_name():
    with pytest.raises(ValueError):
        outbox.append(FakeConn(), "planner; DROP", [_event()])


def test_relay_publishes_its_outbox_keyed_by_incident():
    """El relay del componente publica su outbox en eda.events con clave incident_id"""
    conn = FakeConn([_row(1, "I-a"), _row(2, "I-b")])
    producer = FakeProducer()
    relay = outbox.ComponentRelay(conn, producer, "planner", "eda.events")  # type: ignore[arg-type]

    assert relay.run_once() == 2

    assert [m["key"] for m in producer.produced] == [b"I-a", b"I-b"]
    assert {m["topic"] for m in producer.produced} == {"eda.events"}
    first = producer.produced[0]
    assert first["headers"]["event_type"] == b"PlanCreated"
    body = json.loads(first["value"])
    assert body["correlation_id"] == "I-a" and "seq" not in body and "id" not in body
    selects = [s for s, _ in conn.statements if s.upper().startswith("SELECT")]
    assert all("FROM planner.outbox" in s for s in selects)


def test_relay_records_the_confirmed_position_in_its_outbox():
    """Posición del mensaje publicado: la fila guarda la partición y el offset confirmados"""
    conn = FakeConn([_row(5), _row(6)])
    relay = outbox.ComponentRelay(conn, FakeProducer(), "planner", "eda.events")  # type: ignore[arg-type]

    relay.run_once()

    [(sql, params)] = conn.writes()
    assert sql.startswith("UPDATE planner.outbox")
    assert dict(zip(params[0], zip(params[1], params[2], strict=True), strict=True)) == {5: (1, 0), 6: (1, 1)}


def test_unconfirmed_batch_stays_pending():
    """Si el broker no confirma, no se marca nada y el lote se republica"""
    conn = FakeConn([_row(5)])
    relay = outbox.ComponentRelay(conn, FakeProducer(fail=True), "planner", "eda.events")  # type: ignore[arg-type]
    with pytest.raises(RelayDeliveryError):
        relay.run_once()
    assert conn.writes() == []


def test_relay_publishes_its_dead_letters_to_the_dlq():
    row = {
        "id": 3, "consumer": "planner", "event_id": uuid.uuid4(), "correlation_id": "I-1",
        "reason": "invalid_schema", "raw_message": {"x": 1}, "created_at": datetime.now(UTC),
    }
    conn = FakeConn([row])
    producer = FakeProducer()
    relay = outbox.ComponentRelay(conn, producer, "planner", "eda.events", dlq_topic="eda.dlq")  # type: ignore[arg-type]

    assert relay.publish_dead_letters() == 1

    assert producer.produced[0]["topic"] == "eda.dlq"
    [(sql, _)] = conn.writes()
    assert sql.startswith("UPDATE planner.dead_letters")


def test_replay_marks_its_own_outbox_as_pending():
    """Pérdida del topic: cada componente republica su outbox"""
    conn = FakeConn()
    outbox.ComponentRelay(conn, FakeProducer(), "planner", "eda.events").replay_from(1)  # type: ignore[arg-type]
    [(sql, params)] = conn.writes()
    assert sql.startswith("UPDATE planner.outbox SET published_at = NULL")
    assert params == (1,)


def test_relay_keeps_running_after_an_unexpected_error():
    """Un fallo de la base de datos no mata el relay: lo registra y reintenta"""

    class Flaky(FakeConn):
        def __init__(self):
            super().__init__()
            self.calls = 0

        def execute(self, sql, params=()):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("conexión perdida")
            return super().execute(sql, params)

    conn = Flaky()
    relay = outbox.ComponentRelay(conn, FakeProducer(), "planner", "eda.events")  # type: ignore[arg-type]
    stop = threading.Event()
    thread = threading.Thread(target=relay.run_until, args=(stop,), kwargs={"idle_sleep": 0.01, "error_sleep": 0.01})
    thread.start()
    for _ in range(200):
        if conn.calls > 2:
            break
        stop.wait(0.01)
    stop.set()
    thread.join(2)
    assert conn.calls > 2 and not thread.is_alive()


def test_relay_thread_stops_on_request():
    """El relay corre en un hilo del proceso del componente y se para limpiamente"""
    relay = outbox.ComponentRelay(FakeConn(), FakeProducer(), "planner", "eda.events")  # type: ignore[arg-type]
    thread = outbox.RelayThread(relay, idle_sleep=0.01)
    thread.start()
    thread.stop(timeout=2)
    assert not thread.is_alive()
    assert isinstance(thread, threading.Thread)
