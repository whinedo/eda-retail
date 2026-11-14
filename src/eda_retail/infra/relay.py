"""Relay del outbox (design.md §5): único proceso que publica en Kafka.

Lee las filas pendientes de `outbox` por `seq`, las produce con clave `correlation_id`
(= `incident_id`), espera la confirmación del broker y las marca como publicadas en la
misma transacción. Garantía: al menos una vez. Se ejecuta UNA sola instancia para
conservar el orden por `seq`.
"""

from __future__ import annotations

import argparse
import json
import time

import psycopg
from confluent_kafka import KafkaError, Producer

from eda_retail.config import Settings, load_settings
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.db import connect
from eda_retail.infra.kafka import make_producer

_SELECT_PENDING = """
SELECT o.seq, e.event_id, e.event_type, e.event_version, e.occurred_at,
       e.correlation_id, e.causation_id, e.producer, e.payload
FROM outbox o JOIN events e USING (seq)
WHERE o.published_at IS NULL
ORDER BY o.seq
LIMIT %s
FOR UPDATE OF o SKIP LOCKED
"""


class RelayDeliveryError(RuntimeError):
    """El broker no confirmó algún mensaje del lote; el lote se reintenta."""


def encode_message(seq: int, envelope: EventEnvelope) -> bytes:
    """Valor del mensaje: el evento completo más su `seq` en el log."""
    body = envelope.model_dump(mode="json")
    body["seq"] = seq
    return json.dumps(body, ensure_ascii=False).encode()


class OutboxRelay:
    def __init__(self, conn: psycopg.Connection, producer: Producer, topic: str) -> None:
        self._conn = conn
        self._producer = producer
        self._topic = topic

    @classmethod
    def from_settings(cls, settings: Settings) -> OutboxRelay:
        return cls(
            connect(settings.database_url),
            make_producer(settings.kafka_bootstrap_servers),
            settings.events_topic,
        )

    def run_once(self, batch_size: int = 100, flush_timeout: float = 30.0) -> int:
        """Publica un lote de pendientes. Devuelve cuántos publicó (0 si no había)."""
        errors: list[KafkaError] = []

        def on_delivery(err: KafkaError | None, _msg) -> None:
            if err is not None:
                errors.append(err)

        with self._conn.transaction():
            rows = self._conn.execute(_SELECT_PENDING, (batch_size,)).fetchall()
            if not rows:
                return 0
            for row in rows:
                envelope = EventEnvelope.model_validate({k: v for k, v in row.items() if k != "seq"})
                self._producer.produce(
                    self._topic,
                    key=envelope.correlation_id.encode(),
                    value=encode_message(row["seq"], envelope),
                    headers=[("event_type", envelope.event_type.encode())],
                    on_delivery=on_delivery,
                )
            pending = self._producer.flush(flush_timeout)
            if pending or errors:
                # Rollback: las filas siguen pendientes y el lote se republica.
                raise RelayDeliveryError(f"entrega no confirmada: pendientes={pending}, errores={errors}")
            self._conn.execute(
                "UPDATE outbox SET published_at = now(), attempts = attempts + 1 WHERE seq = ANY(%s)",
                ([row["seq"] for row in rows],),
            )
        return len(rows)

    def replay_from(self, seq: int) -> int:
        """Marca como no publicadas las filas desde `seq` (republicación tras perder el topic)."""
        with self._conn.transaction():
            cur = self._conn.execute(
                "UPDATE outbox SET published_at = NULL WHERE seq >= %s", (seq,)
            )
            return cur.rowcount

    def run_forever(self, idle_sleep: float = 0.1) -> None:
        while True:
            if self.run_once() == 0:
                time.sleep(idle_sleep)

    def close(self) -> None:
        self._producer.flush(10)
        self._conn.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Relay del outbox hacia Kafka")
    parser.add_argument("--replay-from", type=int, help="republica desde este seq")
    args = parser.parse_args(argv)
    relay = OutboxRelay.from_settings(load_settings())
    try:
        if args.replay_from is not None:
            print(f"filas marcadas para republicar: {relay.replay_from(args.replay_from)}")
        relay.run_forever()
    finally:
        relay.close()


if __name__ == "__main__":
    main()
