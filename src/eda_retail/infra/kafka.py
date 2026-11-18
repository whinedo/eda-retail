"""Configuración de los clientes de Kafka (design.md §9). Sin transacciones de Kafka."""

from __future__ import annotations

import logging
import time
from collections import deque

from confluent_kafka import Consumer, Producer

KAFKA_LOG_LINES = 300  # líneas de librdkafka que guarda un `KafkaLogRing`
KAFKA_TRANSITION_LINES = 200  # cambios de estado del grupo que guarda aparte
# Facilidades de librdkafka que marcan un cambio de estado del grupo: se guardan aparte porque el resto (consultas al
# coordinador, heartbeats) llena el anillo en segundos y taparía el momento en que el consumidor pierde sus particiones.
GROUP_TRANSITIONS = frozenset({
    "CGRPSTATE", "CGRPJOINSTATE", "CGRPCOORD", "COORDCLEAR", "JOINGROUP", "SYNCGROUP", "REJOIN", "REBALANCE",
    "ASSIGN", "ASSIGNMENT", "ASSIGNDONE", "CLEARASSIGN", "UNASSIGN", "LOST", "SESSTMOUT", "MAXPOLL", "LEAVEGROUP",
    "MEMBERID", "HEARTBEAT",
})


class KafkaLogRing(logging.Handler):
    """Las últimas líneas del log de librdkafka de un consumidor (p. ej. `debug=cgrp`, E9), con su hora local. librdkafka
    las entrega desde el `poll` del propio consumidor; `snapshot` y `transitions` solo copian, así que se pueden leer
    desde otro hilo sin llamar a Kafka. Los cambios de estado del grupo (`GROUP_TRANSITIONS`) se guardan también aparte."""

    def __init__(self, size: int = KAFKA_LOG_LINES, transitions: int = KAFKA_TRANSITION_LINES) -> None:
        super().__init__(level=logging.DEBUG)
        self.lines: deque[str] = deque(maxlen=size)
        self.group_lines: deque[str] = deque(maxlen=transitions)

    def emit(self, record: logging.LogRecord) -> None:
        stamp = time.strftime("%H:%M:%S", time.localtime(record.created)) + f".{int(record.msecs):03d}"
        message = record.getMessage()
        line = f"{stamp} {message}"
        self.lines.append(line)
        if message.split(" ", 1)[0] in GROUP_TRANSITIONS:
            self.group_lines.append(line)

    def snapshot(self) -> list[str]:
        return list(self.lines)

    def transitions(self) -> list[str]:
        return list(self.group_lines)


def make_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
        }
    )


def make_consumer(bootstrap_servers: str, group_id: str, *, session_timeout_ms: int | None = None,
                  max_poll_interval_ms: int | None = None, debug: str | None = None,
                  log: logging.Handler | None = None) -> Consumer:
    """Consumidor sin auto commit. `session_timeout_ms`: el `session.timeout.ms` del grupo; `max_poll_interval_ms`: el
    `max.poll.interval.ms`, que también es el rebalance timeout con el que el broker retiene un `JoinGroup`. None, en
    los dos, el de librdkafka. `debug`: los contextos de depuración de librdkafka (p. ej. `cgrp`); `log`: dónde van sus
    líneas (un `KafkaLogRing`), en lugar del log del proceso."""
    config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
    if session_timeout_ms is not None:
        config["session.timeout.ms"] = session_timeout_ms
    if max_poll_interval_ms is not None:
        config["max.poll.interval.ms"] = max_poll_interval_ms
    if debug is not None:
        config["debug"] = debug
    if log is not None:
        # Sin registrar: con getLogger, recrear el consumidor añadiría otro handler al mismo logger global.
        logger = logging.Logger(f"librdkafka.{group_id}", level=logging.DEBUG)  # noqa: LOG001
        logger.addHandler(log)
        logger.propagate = False
        config["logger"] = logger
    return Consumer(config)
