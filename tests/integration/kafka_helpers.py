"""Utilidades de test para leer un topic de Kafka completo."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Any

from confluent_kafka import Consumer


@dataclass(frozen=True)
class ReadMessage:
    partition: int
    offset: int
    key: str
    event_type: str
    value: dict[str, Any]


def read_all(bootstrap: str, topic: str, expected: int, timeout: float = 30.0) -> list[ReadMessage]:
    """Lee desde el principio hasta tener `expected` mensajes o agotar el tiempo."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"test-reader-{uuid.uuid4().hex[:8]}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([topic])
    out: list[ReadMessage] = []
    deadline = time.monotonic() + timeout
    try:
        while len(out) < expected and time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None or msg.error():
                continue
            headers = dict(msg.headers() or [])
            out.append(
                ReadMessage(
                    partition=msg.partition(),
                    offset=msg.offset(),
                    key=msg.key().decode(),
                    event_type=headers["event_type"].decode(),
                    value=json.loads(msg.value()),
                )
            )
    finally:
        consumer.close()
    return out
