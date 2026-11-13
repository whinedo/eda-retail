"""idempotency: effect_keys, inbox frente a claves, concurrencia, conservación e interruptores."""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import replace

import psycopg
import pytest

from eda_retail.config import load_settings
from eda_retail.events.envelope import EventEnvelope, new_event
from eda_retail.infra import effect_keys
from eda_retail.infra.consumer import EventConsumer, Outcome
from eda_retail.infra.effect_keys import ClaimStatus, compute_idempotency_key
from eda_retail.infra.event_store import append_events
from eda_retail.infra.relay import OutboxRelay


class EffectBusy(RuntimeError):
    """Otra instancia tiene la clave en curso: reintentar más tarde sin confirmar el offset."""


def _reserve_handler(enabled: bool = True):
    """Ejecutor de prueba: reserva stock (efecto simulado en PostgreSQL, sin restricción única)."""

    def handle(conn: psycopg.Connection, event: EventEnvelope) -> None:
        p = event.payload
        claim = effect_keys.acquire(
            conn,
            key=p["idempotency_key"],
            incident_id=p["incident_id"],
            step_id=p["step_id"],
            action="reserve_stock",
            lock_seconds=30,
            enabled=enabled,
        )
        if claim.status is ClaimStatus.COMPLETED:
            return  # resultado guardado en claim.result; no se repite el efecto
        if claim.status is ClaimStatus.BUSY:
            raise EffectBusy(p["idempotency_key"])
        reservation_id = f"R-{uuid.uuid4().hex[:8]}"
        conn.execute(
            "INSERT INTO reservations (reservation_id, order_id, warehouse, sku, quantity, status) "
            "VALUES (%s, %s, 'W1', 'SKU1', 1, 'active')",
            (reservation_id, p["order_id"]),
        )
        effect_keys.complete(conn, p["idempotency_key"], {"reservation_id": reservation_id}, enabled=enabled)
        append_events(
            conn,
            [
                new_event(
                    event_type="StockReserved",
                    correlation_id=event.correlation_id,
                    causation_id=event.event_id,
                    producer="executor-reserve_stock",
                    payload={"order_id": p["order_id"], "warehouse": "W1", "reservation_id": reservation_id},
                )
            ],
        )

    return handle


def _step_requested(incident: str, order: str, step_id: str = "S1") -> EventEnvelope:
    return new_event(
        event_type="StepRequested",
        correlation_id=incident,
        causation_id=None,
        producer="test",
        payload={
            "incident_id": incident,
            "plan_version": 1,
            "step_id": step_id,
            "action": "reserve_stock",
            "params": {"order_id": order},
            "order_id": order,
            "idempotency_key": compute_idempotency_key(incident, step_id, origin_plan_version=1),
        },
    )


def _new_ids() -> tuple[str, str]:
    return f"I-{uuid.uuid4().hex[:8]}", f"P-{uuid.uuid4().hex[:8]}"


def _reservations(conn: psycopg.Connection, order: str) -> int:
    return conn.execute(
        "SELECT count(*) AS n FROM reservations WHERE order_id = %s", (order,)
    ).fetchone()["n"]


def _stock_reserved(conn: psycopg.Connection, incident: str) -> int:
    return conn.execute(
        "SELECT count(*) AS n FROM events WHERE correlation_id = %s AND event_type = 'StockReserved'",
        (incident,),
    ).fetchone()["n"]


def _process_directly(consumer_name: str, conn, event: EventEnvelope, handler, inbox_enabled=True):
    """Procesa como lo hace EventConsumer.handle_message, sin pasar por Kafka."""
    from eda_retail.infra import inbox

    with conn.transaction():
        if not inbox.register(conn, consumer_name, event.event_id, enabled=inbox_enabled):
            return Outcome.DUPLICATE
        handler(conn, event)
    return Outcome.PROCESSED


def test_new_event_requesting_already_executed_effect(connect_db):
    """Evento nuevo que pide un efecto ya ejecutado"""
    incident, order = _new_ids()
    conn = connect_db()
    handler = _reserve_handler()
    first = _step_requested(incident, order)
    second = _step_requested(incident, order)  # otro event_id, misma idempotency_key
    assert first.event_id != second.event_id

    assert _process_directly("executor", conn, first, handler) is Outcome.PROCESSED
    assert _process_directly("executor", conn, second, handler) is Outcome.PROCESSED  # el inbox lo ve nuevo

    inbox_rows = conn.execute(
        "SELECT count(*) AS n FROM inbox WHERE consumer = 'executor' AND event_id = ANY(%s)",
        ([first.event_id, second.event_id],),
    ).fetchone()["n"]
    assert inbox_rows == 2
    assert _reservations(conn, order) == 1
    stored = conn.execute(
        "SELECT status, result FROM effect_keys WHERE idempotency_key = %s",
        (first.payload["idempotency_key"],),
    ).fetchone()
    assert stored["status"] == "completed"
    claim = effect_keys.acquire(
        conn, key=first.payload["idempotency_key"], incident_id=incident, step_id="S1",
        action="reserve_stock", lock_seconds=30,
    )
    conn.rollback()
    assert claim.status is ClaimStatus.COMPLETED
    assert claim.result == stored["result"]


def test_duplicate_event(connect_db):
    """Evento duplicado"""
    incident, order = _new_ids()
    conn = connect_db()
    handler = _reserve_handler()
    event = _step_requested(incident, order)
    # Sin inbox, para comprobar que effect_keys por sí solo impide repetir el efecto.
    _process_directly("executor-noinbox", conn, event, handler, inbox_enabled=False)
    _process_directly("executor-noinbox", conn, event, handler, inbox_enabled=False)
    assert _reservations(conn, order) == 1
    assert _stock_reserved(conn, incident) == 1  # sin publicar un efecto nuevo


def test_two_executors_at_once(connect_db):
    """Dos ejecutores a la vez"""
    incident, _ = _new_ids()
    key = compute_idempotency_key(incident, "S1", origin_plan_version=1)
    a, b = connect_db(), connect_db()
    args = dict(key=key, incident_id=incident, step_id="S1", action="reserve_stock", lock_seconds=30)

    result: dict[str, ClaimStatus] = {}
    with a.transaction():
        assert effect_keys.acquire(a, **args).status is ClaimStatus.ACQUIRED

        def other() -> None:
            with b.transaction():
                result["b"] = effect_keys.acquire(b, **args).status

        t = threading.Thread(target=other)
        t.start()
        time.sleep(0.5)
        assert t.is_alive()  # la segunda instancia espera a la primera
        effect_keys.complete(a, key, {"reservation_id": "R-1"})
    t.join(timeout=10)
    assert result["b"] is ClaimStatus.COMPLETED  # no obtiene la clave ni ejecuta el efecto


def test_in_progress_key_blocks_until_lock_expires(connect_db):
    """Registro atómico: una clave in_progress bloquea hasta vencer locked_until"""
    incident, _ = _new_ids()
    key = compute_idempotency_key(incident, "S1", origin_plan_version=1)
    conn = connect_db()
    args = dict(key=key, incident_id=incident, step_id="S1", action="notify_customer")
    with conn.transaction():
        assert effect_keys.acquire(conn, lock_seconds=30, **args).status is ClaimStatus.ACQUIRED
    with conn.transaction():
        assert effect_keys.acquire(conn, lock_seconds=30, **args).status is ClaimStatus.BUSY
    with conn.transaction():
        conn.execute(
            "UPDATE effect_keys SET locked_until = now() - interval '1 second' WHERE idempotency_key = %s",
            (key,),
        )
    with conn.transaction():
        assert effect_keys.acquire(conn, lock_seconds=30, **args).status is ClaimStatus.ACQUIRED


def test_late_duplicate(connect_db):
    """Duplicado tardío"""
    incident, order = _new_ids()
    conn = connect_db()
    handler = _reserve_handler()
    event = _step_requested(incident, order)
    with conn.transaction():
        conn.execute(
            "INSERT INTO incidents (incident_id, status, severity, cause) VALUES (%s, 'open', 'high', 'lost')",
            (incident,),
        )
    _process_directly("executor-late", conn, event, handler, inbox_enabled=False)
    with conn.transaction():
        conn.execute(
            "UPDATE incidents SET status = 'resolved', closed_at = now() WHERE incident_id = %s", (incident,)
        )
    _process_directly("executor-late", conn, event, handler, inbox_enabled=False)
    assert _reservations(conn, order) == 1


def test_topic_loss_consumers_do_not_repeat_effects(connect_db, settings):
    """Pérdida del topic (los consumidores no repiten ningún efecto)"""
    conn = connect_db()
    pairs = [_new_ids() for _ in range(200)]
    events = [_step_requested(i, o) for i, o in pairs]
    with conn.transaction():
        seqs = append_events(conn, events)

    def relay_all() -> None:
        relay = OutboxRelay.from_settings(settings)
        try:
            while relay.run_once() > 0:
                pass
        finally:
            relay.close()

    def consume_until(group: str, wanted: set[uuid.UUID]) -> None:
        consumer = EventConsumer.from_settings(
            settings, name=group, handlers={"StepRequested": _reserve_handler()}
        )
        seen: set[uuid.UUID] = set()
        deadline = time.monotonic() + 60
        try:
            while not wanted <= seen and time.monotonic() < deadline:
                msg = consumer.poll(0.5)
                if msg is None:
                    continue
                consumer.handle_message(msg)
                consumer.commit(msg)
                seen.add(consumer.event_id_of(msg))
        finally:
            consumer.close()
        assert wanted <= seen

    wanted = {e.event_id for e in events}
    relay_all()
    consume_until("executor-reserve", wanted)

    relay = OutboxRelay.from_settings(settings)
    try:
        relay.replay_from(seqs[0])  # el log se republica ante los ejecutores activos
    finally:
        relay.close()
    relay_all()
    consume_until(f"executor-reserve-new-{uuid.uuid4().hex[:6]}", wanted)  # grupo nuevo: lee todo

    total = conn.execute(
        "SELECT count(*) AS n FROM reservations WHERE order_id = ANY(%s)", ([o for _, o in pairs],)
    ).fetchone()["n"]
    assert total == 200


def test_inbox_switch_disabled_processes_duplicates(connect_db):
    """INBOX_ENABLED=false: el inbox deja de descartar duplicados (E1)"""
    incident, order = _new_ids()
    conn = connect_db()
    event = _step_requested(incident, order)
    calls: list[uuid.UUID] = []

    def handler(c, ev):
        calls.append(ev.event_id)

    assert _process_directly("e1", conn, event, handler, inbox_enabled=False) is Outcome.PROCESSED
    assert _process_directly("e1", conn, event, handler, inbox_enabled=False) is Outcome.PROCESSED
    assert len(calls) == 2
    assert conn.execute("SELECT count(*) AS n FROM inbox WHERE consumer = 'e1'").fetchone()["n"] == 0


def test_effect_keys_switch_disabled_repeats_effect(connect_db):
    """EFFECT_KEYS_ENABLED=false: el efecto se repite (E1)"""
    incident, order = _new_ids()
    conn = connect_db()
    handler = _reserve_handler(enabled=False)
    event = _step_requested(incident, order)
    _process_directly("e1-keys", conn, event, handler, inbox_enabled=False)
    _process_directly("e1-keys", conn, event, handler, inbox_enabled=False)
    assert _reservations(conn, order) == 2
    assert conn.execute(
        "SELECT count(*) AS n FROM effect_keys WHERE incident_id = %s", (incident,)
    ).fetchone()["n"] == 0


@pytest.mark.parametrize(
    ("value", "expected"), [(None, True), ("", True), ("false", False), ("0", False), ("true", True)]
)
def test_switches_default_on(monkeypatch, value, expected):
    """Los interruptores INBOX_ENABLED y EFFECT_KEYS_ENABLED están activos por defecto"""
    for name in ("INBOX_ENABLED", "EFFECT_KEYS_ENABLED"):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    s = load_settings()
    assert (s.inbox_enabled, s.effect_keys_enabled) == (expected, expected)


def test_consumer_honours_inbox_switch(settings):
    """EventConsumer.from_settings aplica INBOX_ENABLED"""
    consumer = EventConsumer.from_settings(replace(settings, inbox_enabled=False), name="x", handlers={})
    try:
        assert consumer._inbox_enabled is False
    finally:
        consumer.close()


