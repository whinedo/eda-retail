"""Límite de concurrencia de un ejecutor (spec resilience, «Límite de concurrencia»; supuesto A7).

El ejecutor toma de Kafka como mucho `max_concurrency` mensajes, los procesa en otros
tantos hilos, cada uno con su conexión, y confirma los offsets solo cuando todos han
hecho `COMMIT` en PostgreSQL. Mientras tanto no lee más: los eventos esperan en el bus.
Si uno falla, no se confirma ningún offset del lote y Kafka los vuelve a entregar (los
ya procesados se descartan por el inbox).
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor

import psycopg
from confluent_kafka import Consumer, Message

from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.consumer import Outcome, UnitOfWork, decode_message


class ConcurrentRunner:
    def __init__(
        self, unit: UnitOfWork, connect: Callable[[], psycopg.Connection], *, max_concurrency: int = 5
    ) -> None:
        self._unit = unit
        self.max_concurrency = max_concurrency
        self._conns = [connect() for _ in range(max_concurrency)]
        self._pool = ThreadPoolExecutor(max_workers=max_concurrency, thread_name_prefix="executor")

    def process_batch(self, events: Sequence[EventEnvelope]) -> list[Outcome]:
        """Procesa como mucho `max_concurrency` eventos a la vez; propaga el primer error."""
        if len(events) > self.max_concurrency:
            raise ValueError(f"lote de {len(events)} > límite {self.max_concurrency}")
        futures = [self._pool.submit(self._unit.process, conn, ev) for conn, ev in zip(self._conns, events, strict=False)]
        return [f.result() for f in futures]

    def run_once(self, consumer: Consumer, timeout: float = 1.0) -> int:
        messages: list[Message] = [m for m in consumer.consume(self.max_concurrency, timeout) if not m.error()]
        if not messages:
            return 0
        self.process_batch([decode_message(m) for m in messages])
        for m in messages:
            consumer.commit(message=m, asynchronous=False)
        return len(messages)

    def close(self) -> None:
        self._pool.shutdown(wait=True)
        for c in self._conns:
            c.close()
