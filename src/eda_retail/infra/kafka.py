"""Configuración de los clientes de Kafka (design.md §9). Sin transacciones de Kafka."""

from __future__ import annotations

from confluent_kafka import Consumer, Producer


def make_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def make_consumer(bootstrap_servers: str, group_id: str, *, session_timeout_ms: int | None = None) -> Consumer:
    """Consumidor sin auto commit. `session_timeout_ms`: el `session.timeout.ms` del grupo; None, el de librdkafka."""
    config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    if session_timeout_ms is not None:
        config["session.timeout.ms"] = session_timeout_ms
    return Consumer(config)
