"""Reconexión a PostgreSQL (spec resilience, «Reconexión a PostgreSQL»): cada bucle de larga duración sustituye una
conexión rota por otra y sigue, sin perder el mensaje que estaba atendiendo ni confirmar su offset antes de tiempo."""

from __future__ import annotations

import logging
import threading
from types import SimpleNamespace

import psycopg
import pytest

from eda_retail.infra import db
from eda_retail.infra.consumer import EventConsumer, Outcome
from eda_retail.infra.outbox import ComponentRelay
from eda_retail.infra.positions import PositionedConsumer, run_resilient


class FakeConn:
    def __init__(self, name: str = "c") -> None:
        self.name = name
        self.closed = False
        self.broken = False

    def close(self) -> None:
        self.closed = True


class Connections:
    """Fábrica de conexiones: las primeras `failures` llamadas fallan como una base que aún no responde."""

    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.opened: list[FakeConn] = []

    def __call__(self) -> FakeConn:
        if self.failures:
            self.failures -= 1
            raise psycopg.OperationalError("connection refused")
        conn = FakeConn(f"c{len(self.opened) + 1}")
        self.opened.append(conn)
        return conn


def lost(conn: FakeConn) -> psycopg.OperationalError:
    conn.broken = True
    return psycopg.OperationalError("server closed the connection unexpectedly")


# --- conexión ----------------------------------------------------------------------------------------------------
def test_connect_sets_a_connect_timeout_and_tcp_keepalives(monkeypatch):
    """Una base que no responde da error al conectar en vez de esperar sin límite"""
    seen: dict = {}

    def fake_connect(url, **kwargs):
        seen.update(kwargs, url=url)
        return FakeConn()

    monkeypatch.setattr(db.psycopg, "connect", fake_connect)
    monkeypatch.setattr(db.PsycopgInstrumentor, "instrument_connection", lambda self, conn, **_: conn)
    db.connect("postgresql://x/y")
    assert seen["connect_timeout"] == db.CONNECT_TIMEOUT_S and seen["keepalives"] == 1
    assert {"keepalives_idle", "keepalives_interval", "keepalives_count"} <= set(seen)
    assert seen["autocommit"] is False


def test_reopen_keeps_a_healthy_connection_and_replaces_a_broken_one():
    """Una conexión sana se reutiliza; una rota o cerrada se cierra y se abre otra"""
    factory = Connections()
    healthy = FakeConn("sana")
    assert db.reopen(healthy, factory) is healthy and factory.opened == []
    broken = FakeConn("rota")
    broken.broken = True
    fresh = db.reopen(broken, factory)
    assert fresh is factory.opened[0] and broken.closed


def test_reopen_propagates_while_the_database_is_still_down():
    """Si la base aún no responde, el error de conexión sube y quien llama reintenta en la siguiente vuelta"""
    broken = FakeConn()
    broken.broken = True
    with pytest.raises(psycopg.OperationalError):
        db.reopen(broken, Connections(failures=1))
    assert broken.closed


def test_run_loop_reconnects_after_a_broken_connection():
    """Bucle del orquestador o del intérprete: tras una conexión rota abre otra, también si la base tarda en volver"""
    factory = Connections()
    stop = threading.Event()
    calls: list[str] = []

    def step(conn: FakeConn) -> bool:
        calls.append(conn.name)
        if len(calls) == 1:
            factory.failures = 2  # la base tarda dos intentos en volver
            raise lost(conn)
        if len(calls) == 3:
            stop.set()
        return True

    db.run_loop(factory, stop, step, interval=0, name="prueba", retry_s=0)
    assert calls == ["c1", "c2", "c2"]
    assert factory.opened[0].closed and factory.opened[1].closed  # la última se cierra al salir


def test_run_loop_keeps_the_connection_after_a_business_error():
    """Un error que no rompe la conexión no la cambia: el bucle sigue con la misma"""
    factory = Connections()
    stop = threading.Event()
    calls: list[str] = []

    def step(conn: FakeConn) -> bool:
        calls.append(conn.name)
        if len(calls) == 2:
            stop.set()
        raise ValueError("fallo de negocio")

    db.run_loop(factory, stop, step, interval=0, name="prueba", retry_s=0)
    assert calls == ["c1", "c1"] and len(factory.opened) == 1


# --- EventConsumer -----------------------------------------------------------------------------------------------
class FakeKafka:
    def __init__(self, messages: list, stop: threading.Event) -> None:
        self._messages = list(messages)
        self._stop = stop
        self.committed: list = []

    def subscribe(self, *_args, **_kwargs) -> None:
        pass

    def poll(self, _timeout: float):
        if not self._messages:
            self._stop.set()
            return None
        return self._messages.pop(0)

    def commit(self, message, asynchronous: bool) -> None:
        assert asynchronous is False
        self.committed.append(message)


def _message(offset: int) -> SimpleNamespace:
    return SimpleNamespace(offset=lambda: offset, partition=lambda: 0, error=lambda: None)


def _event_consumer(conn: FakeConn, kafka: FakeKafka, **kwargs) -> EventConsumer:
    return EventConsumer("interpreter", conn, kafka, "t", {}, schema="interpreter", **kwargs)


def test_consumer_retries_the_same_message_on_a_new_connection_and_commits_once():
    """Base caída al atender un evento: se reconecta, lo vuelve a atender y solo entonces confirma su offset"""
    stop = threading.Event()
    msg = _message(7)
    kafka = FakeKafka([msg], stop)
    factory = Connections(failures=1)
    first = FakeConn("c0")
    consumer = _event_consumer(first, kafka, reconnect=factory)
    seen: list[str] = []

    def handle(m):
        seen.append(consumer._conn.name)
        assert kafka.committed == []  # nada confirmado mientras no se haya atendido
        if len(seen) == 1:
            raise lost(consumer._conn)
        return Outcome.PROCESSED

    consumer.handle_message = handle
    consumer.run_forever(stop=stop, retry_s=0)
    assert seen == ["c0", "c1"]  # el primer intento de reconexión falló: la base aún no respondía
    assert kafka.committed == [msg] and first.closed


def test_consumer_without_reconnect_keeps_failing_as_before():
    """Sin fábrica de conexiones (harness en proceso), un error de la base sube como hasta ahora"""
    stop = threading.Event()
    kafka = FakeKafka([_message(1)], stop)
    consumer = _event_consumer(FakeConn(), kafka)

    def handle(_m):
        raise lost(consumer._conn)

    consumer.handle_message = handle
    with pytest.raises(psycopg.OperationalError):
        consumer.run_forever(stop=stop, retry_s=0)
    assert kafka.committed == []


def test_consumer_does_not_retry_errors_that_are_not_from_the_database():
    """Solo se reintentan los errores de conexión: un fallo del handler sigue tumbando el consumidor"""
    stop = threading.Event()
    kafka = FakeKafka([_message(1)], stop)
    consumer = _event_consumer(FakeConn(), kafka, reconnect=Connections())

    def handle(_m):
        raise ValueError("bug")

    consumer.handle_message = handle
    with pytest.raises(ValueError):
        consumer.run_forever(stop=stop, retry_s=0)
    assert kafka.committed == []


# --- relay -------------------------------------------------------------------------------------------------------
def test_relay_replaces_a_broken_connection_and_keeps_publishing():
    """Relay: tras perder la base abre otra conexión; antes se quedaba con la cerrada para siempre"""
    factory = Connections()
    first = FakeConn("c0")
    relay = ComponentRelay(first, producer=None, schema="intake", topic="t", reconnect=factory)
    stop = threading.Event()
    seen: list[str] = []

    def run_once(flush_timeout: float = 30.0) -> int:
        seen.append(relay._conn.name)
        if len(seen) == 1:
            raise lost(relay._conn)
        stop.set()
        return 1

    relay.run_once = run_once
    relay.publish_dead_letters = lambda flush_timeout=30.0: 0
    relay.run_until(stop, idle_sleep=0, error_sleep=0)
    assert seen == ["c0", "c1"] and first.closed


def test_relay_without_reconnect_keeps_its_connection():
    """Sin fábrica (harness en proceso), el relay conserva su conexión como hasta ahora"""
    conn = FakeConn()
    relay = ComponentRelay(conn, producer=None, schema="intake", topic="t")
    stop = threading.Event()

    def run_once(flush_timeout: float = 30.0) -> int:
        stop.set()
        raise lost(conn)

    relay.run_once = run_once
    relay.publish_dead_letters = lambda flush_timeout=30.0: 0
    relay.run_until(stop, idle_sleep=0, error_sleep=0)
    assert relay._conn is conn


# --- consumidor posicionado (projector, event-log) ---------------------------------------------------------------
def test_positioned_consumer_recovers_on_a_new_connection_and_reassigns():
    """Proyector o event_log: tras perder la base abre otra conexión y vuelve a la posición guardada"""
    factory = Connections()
    first = FakeConn("c0")
    consumer = PositionedConsumer(first, kafka_consumer=None, topic="t", schema="views", consumer="v",
                                  apply=lambda *_: None, reconnect=factory)
    consumer._assigned = True
    first.broken = True
    consumer.recover()
    assert consumer._conn is factory.opened[0] and first.closed and consumer._assigned is False


def test_positioned_consumer_recover_is_a_no_op_with_a_healthy_connection_or_without_a_factory():
    """Con la conexión sana, o sin fábrica, `recover` no cambia nada"""
    conn = FakeConn()
    consumer = PositionedConsumer(conn, kafka_consumer=None, topic="t", schema="views", consumer="v",
                                  apply=lambda *_: None, reconnect=Connections())
    consumer._assigned = True
    consumer.recover()
    assert consumer._conn is conn and consumer._assigned
    plain = PositionedConsumer(FakeConn(), kafka_consumer=None, topic="t", schema="views", consumer="v",
                               apply=lambda *_: None)
    plain._conn.broken = True
    plain.recover()
    assert not plain._conn.closed


class FlakyPositioned:
    consumer = "event_log"

    def __init__(self, error: Exception | None, recover_error: Exception | None = None) -> None:
        self._error = error
        self._recover_error = recover_error
        self.recovered = 0

    def run_once(self, timeout: float = 1.0) -> int:
        if self._error is not None:
            raise self._error
        return 3

    def recover(self) -> None:
        self.recovered += 1
        if self._recover_error is not None:
            raise self._recover_error


def test_run_resilient_recovers_after_a_database_error():
    """Bucle de projector o event-log: un error de la base no termina el proceso; recupera la conexión y sigue"""
    log = logging.getLogger("test")
    ok = FlakyPositioned(None)
    assert run_resilient(ok, log, retry_s=0) == 3 and ok.recovered == 0
    down = FlakyPositioned(psycopg.OperationalError("down"))
    assert run_resilient(down, log, retry_s=0) == 0 and down.recovered == 1
    still_down = FlakyPositioned(psycopg.OperationalError("down"), psycopg.OperationalError("refused"))
    assert run_resilient(still_down, log, retry_s=0) == 0 and still_down.recovered == 1


def test_run_resilient_lets_other_errors_through():
    """Un error que no es de la base sigue terminando el bucle, como antes"""
    with pytest.raises(ValueError):
        run_resilient(FlakyPositioned(ValueError("bug")), logging.getLogger("test"), retry_s=0)
