"""Alimentar los proyectores por Kafka en los tests de integración.

Mientras los productores escriben en el log compartido `events` (hasta T10), `publish_log` hace lo
que el relay: publica en el topic del test los eventos de unas incidencias que aún no publicó.
`catch_up` pone al día un proyector consumiendo ese topic con su consumidor posicionado.
"""

from __future__ import annotations

import uuid
from collections import defaultdict

from eda_retail.config import Settings
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.kafka import make_producer
from eda_retail.infra.outbox import encode_event
from eda_retail.infra.positions import Position
from eda_retail.projectors.base import consumer_for, load_event
from eda_retail.projectors.registry import PROJECTORS

_published: dict[str, set[uuid.UUID]] = defaultdict(set)


def publish(settings: Settings, events: list[EventEnvelope]) -> list[Position]:
    """Publica los eventos (clave `correlation_id`) y devuelve la posición de cada uno, en orden."""
    producer = make_producer(settings.kafka_bootstrap_servers)
    positions: list[Position | None] = [None] * len(events)

    def on_delivery_for(i: int):
        def on_delivery(err, msg) -> None:
            assert err is None, err
            positions[i] = Position(msg.topic(), msg.partition(), msg.offset())
        return on_delivery

    for i, event in enumerate(events):
        producer.produce(settings.events_topic, key=event.correlation_id.encode(), value=encode_event(event),
                         headers=[("event_type", event.event_type.encode())], on_delivery=on_delivery_for(i))
    assert producer.flush(30) == 0
    _published[settings.events_topic].update(e.event_id for e in events)
    return [p for p in positions if p is not None]


def publish_log(settings: Settings, conn, *incidents: str) -> list[Position]:
    """Publica los eventos de `events` de esas incidencias que aún no están en el topic del test."""
    rows = conn.execute(
        "SELECT * FROM events WHERE correlation_id = ANY(%s) ORDER BY seq", (list(incidents),)
    ).fetchall()
    conn.commit()
    pending = [load_event(r) for r in rows if r["event_id"] not in _published[settings.events_topic]]
    return publish(settings, pending) if pending else []


def catch_up(settings: Settings, conn, view: str) -> int:
    """Consume el topic del test hasta el final con el proyector de `view`."""
    consumer = consumer_for(PROJECTORS[view].build(), settings, conn)
    try:
        return consumer.catch_up()
    finally:
        consumer.close()


def project(settings: Settings, conn, view: str, *incidents: str) -> int:
    publish_log(settings, conn, *incidents)
    return catch_up(settings, conn, view)
