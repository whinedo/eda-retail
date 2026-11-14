"""Proceso `eda-event-log`: consume `eda.events` y mantiene `event_log.events` (design.md §8).

Guarda su posición en `event_log.offsets` en la misma transacción que cada fila del log.
"""

from __future__ import annotations

import argparse
import logging

from eda_retail.config import load_settings
from eda_retail.event_log.store import CONSUMER, SCHEMA, apply_message
from eda_retail.infra.db import connect
from eda_retail.infra.kafka import make_consumer
from eda_retail.infra.logs import setup_logging
from eda_retail.infra.positions import PositionedConsumer
from eda_retail.infra.tracing import setup_tracing


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return argparse.ArgumentParser(description="Vista del log de eventos (consulta de CQRS)").parse_args(argv)


def build(settings) -> PositionedConsumer:
    return PositionedConsumer(
        connect(settings.database_url),
        make_consumer(settings.kafka_bootstrap_servers, group_id=CONSUMER),
        settings.events_topic,
        schema=SCHEMA,
        consumer=CONSUMER,
        apply=apply_message,
    )


def main(argv: list[str] | None = None) -> None:
    parse_args(argv)
    settings = load_settings()
    setup_logging("event-log")
    provider = setup_tracing("event-log", settings)
    consumer = build(settings)
    log = logging.getLogger("eda_retail.event_log")
    try:
        while True:
            if applied := consumer.run_once():
                log.info("log al día: %d mensajes", applied)
    finally:
        consumer.close()
        if provider is not None:
            provider.shutdown()


if __name__ == "__main__":
    main()
