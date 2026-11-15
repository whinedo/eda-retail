"""Relay del outbox (design.md §5): único proceso que publica en Kafka.

Lee las filas pendientes de `outbox` por `seq`, las produce con clave `correlation_id`
(= `incident_id`), espera la confirmación del broker y las marca como publicadas, con la
partición y el offset que confirmó, en la misma transacción. Garantía: al menos una vez. Se ejecuta UNA sola instancia para
conservar el orden por `seq`.
"""

from __future__ import annotations

import argparse
import json
import logging
import time

import psycopg
from confluent_kafka import KafkaError, Producer
from opentelemetry.trace import SpanKind

from eda_retail.config import Settings, load_settings
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra import tracing
from eda_retail.infra.db import connect
from eda_retail.infra.kafka import make_producer
from eda_retail.infra.logs import setup_logging
from eda_retail.infra.outbox import RelayDeliveryError
from eda_retail.infra.tracing import setup_tracing

log = logging.getLogger("eda_retail.relay")

_SELECT_PENDING = """
SELECT o.seq, o.trace_context, e.event_id, e.event_type, e.event_version, e.occurred_at,
       e.correlation_id, e.causation_id, e.producer, e.payload
FROM outbox o JOIN events e USING (seq)
WHERE o.published_at IS NULL
ORDER BY o.seq
LIMIT %s
FOR UPDATE OF o SKIP LOCKED
"""

# Marca el lote como publicado con la posición que confirmó el broker (la última, si se republica).
_MARK_PUBLISHED = """
UPDATE outbox o
SET published_at = now(), attempts = o.attempts + 1, kafka_partition = p.kafka_partition,
    kafka_offset = p.kafka_offset
FROM unnest(%s::bigint[], %s::integer[], %s::bigint[]) AS p(seq, kafka_partition, kafka_offset)
WHERE o.seq = p.seq
"""


_SELECT_DEAD_LETTERS = """
SELECT id, consumer, event_id, correlation_id, reason, raw_message, created_at
FROM dead_letters WHERE published_at IS NULL
ORDER BY id
LIMIT %s
FOR UPDATE SKIP LOCKED
"""




def encode_message(seq: int, envelope: EventEnvelope) -> bytes:
    """Valor del mensaje: el evento completo más su `seq` en el log."""
    body = envelope.model_dump(mode="json")
    body["seq"] = seq
    return json.dumps(body, ensure_ascii=False).encode()


class OutboxRelay:
    def __init__(
        self, conn: psycopg.Connection, producer: Producer, topic: str, *, dlq_topic: str | None = None
    ) -> None:
        self._conn = conn
        self._producer = producer
        self._topic = topic
        self._dlq_topic = dlq_topic

    @classmethod
    def from_settings(cls, settings: Settings) -> OutboxRelay:
        return cls(
            connect(settings.database_url),
            make_producer(settings.kafka_bootstrap_servers),
            settings.events_topic,
            dlq_topic=settings.dlq_topic,
        )

    def run_once(self, batch_size: int = 100, flush_timeout: float = 30.0) -> int:
        """Publica un lote de pendientes. Devuelve cuántos publicó (0 si no había)."""
        errors: list[KafkaError] = []
        positions: dict[int, tuple[int, int]] = {}

        def on_delivery_for(seq: int):
            def on_delivery(err: KafkaError | None, msg) -> None:
                if err is not None:
                    errors.append(err)
                else:
                    positions[seq] = (msg.partition(), msg.offset())

            return on_delivery

        with self._conn.transaction():
            rows = self._conn.execute(_SELECT_PENDING, (batch_size,)).fetchall()
            if not rows:
                return 0
            for row in rows:
                envelope = EventEnvelope.model_validate(
                    {k: v for k, v in row.items() if k not in ("seq", "trace_context")}
                )
                self._produce(row["seq"], envelope, row["trace_context"], on_delivery_for(row["seq"]))
            pending = self._producer.flush(flush_timeout)
            if pending or errors:
                # Rollback: las filas siguen pendientes y el lote se republica.
                raise RelayDeliveryError(f"entrega no confirmada: pendientes={pending}, errores={errors}")
            seqs = [row["seq"] for row in rows]
            self._conn.execute(
                _MARK_PUBLISHED,
                (
                    seqs,
                    [positions.get(seq, (None, None))[0] for seq in seqs],
                    [positions.get(seq, (None, None))[1] for seq in seqs],
                ),
            )
        return len(rows)

    def _produce(self, seq: int, envelope: EventEnvelope, trace_context, on_delivery) -> None:
        """Produce un mensaje dentro del span `<event_type> publish`, hijo del de escritura."""
        attributes = tracing.event_attributes(envelope) | {
            "messaging.system": "kafka",
            "messaging.destination.name": self._topic,
            "eda.seq": seq,
        }
        with tracing.tracer().start_as_current_span(
            f"{envelope.event_type} publish",
            context=tracing.extract(trace_context),
            kind=SpanKind.PRODUCER,
            attributes=attributes,
        ):
            headers = [("event_type", envelope.event_type.encode())]
            headers += [(k, v.encode()) for k, v in (tracing.inject_current() or {}).items()]
            self._producer.produce(
                self._topic,
                key=envelope.correlation_id.encode(),
                value=encode_message(seq, envelope),
                headers=headers,
                on_delivery=on_delivery,
            )

    def publish_dead_letters(self, batch_size: int = 100, flush_timeout: float = 30.0) -> int:
        """Publica en `eda.dlq` las dead letters pendientes (supuesto A6). Al menos una vez."""
        if self._dlq_topic is None:
            return 0
        errors: list[KafkaError] = []

        def on_delivery(err: KafkaError | None, _msg) -> None:
            if err is not None:
                errors.append(err)

        with self._conn.transaction():
            rows = self._conn.execute(_SELECT_DEAD_LETTERS, (batch_size,)).fetchall()
            if not rows:
                return 0
            for row in rows:
                body = {k: (str(v) if k in ("event_id", "created_at") and v is not None else v) for k, v in row.items()}
                self._producer.produce(
                    self._dlq_topic,
                    key=(row["correlation_id"] or "").encode(),
                    value=json.dumps(body, ensure_ascii=False).encode(),
                    headers=[("event_type", b"DeadLetter"), ("reason", row["reason"].encode())],
                    on_delivery=on_delivery,
                )
            pending = self._producer.flush(flush_timeout)
            if pending or errors:
                raise RelayDeliveryError(f"dead letters sin confirmar: pendientes={pending}, errores={errors}")
            self._conn.execute(
                "UPDATE dead_letters SET published_at = now() WHERE id = ANY(%s)", ([r["id"] for r in rows],)
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
            published, dead = self.run_once(), self.publish_dead_letters()
            if published:
                log.info("publicados %d eventos en %s", published, self._topic)
            if dead:
                log.info("publicadas %d dead letters en %s", dead, self._dlq_topic)
            if published + dead == 0:
                time.sleep(idle_sleep)

    def close(self) -> None:
        self._producer.flush(10)
        self._conn.close()


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Relay del outbox hacia Kafka")
    parser.add_argument("--replay-from", type=int, help="republica desde este seq")
    args = parser.parse_args(argv)
    settings = load_settings()
    setup_logging("relay")
    provider = setup_tracing("relay", settings)
    relay = OutboxRelay.from_settings(settings)
    try:
        if args.replay_from is not None:
            print(f"filas marcadas para republicar: {relay.replay_from(args.replay_from)}")
        relay.run_forever()
    finally:
        relay.close()
        if provider is not None:
            provider.shutdown()


if __name__ == "__main__":
    main()
