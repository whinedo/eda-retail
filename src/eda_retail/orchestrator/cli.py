"""Proceso `eda-orchestrator`: la saga reactiva (design.md §4.2).

Un hilo consume los eventos (grupo e inbox `orchestrator`), otro publica su outbox (relay), otro
invoca por REST los comandos de su `command_outbox` y otro trata los plazos vencidos. Todo su estado
está en sus tablas, así que reiniciarlo no pierde nada (spec resilience, "Reinicio del orquestador").
Una instancia basta.
"""

from __future__ import annotations

import argparse
import logging
import threading
from collections.abc import Callable

import psycopg

from eda_retail.config import Settings, load_settings
from eda_retail.infra.backoff import Backoff
from eda_retail.infra.db import connect
from eda_retail.infra.kafka import make_consumer
from eda_retail.infra.logs import setup_logging
from eda_retail.infra.outbox import start_relay
from eda_retail.infra.tracing import setup_tracing
from eda_retail.orchestrator.consumer import build_consumer
from eda_retail.orchestrator.dispatcher import dispatch_once, expire_once
from eda_retail.orchestrator.saga import SCHEMA
from eda_retail.rules import BusinessRules

log = logging.getLogger("eda_retail.orchestrator")
IDLE_S = 0.2
EXPIRY_INTERVAL_S = 1.0


def _loop(settings: Settings, stop: threading.Event, step: Callable[[psycopg.Connection], object],
          interval: float) -> None:
    conn = connect(settings.database_url)
    try:
        while not stop.is_set():
            try:
                busy = step(conn)
            except Exception:
                log.exception("fallo en el bucle del orquestador; se reintenta")
                busy = False
            if not busy:
                stop.wait(interval)
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(description="Orquestador: saga reactiva con su command_outbox").parse_args(argv)
    settings = load_settings()
    rules = BusinessRules()
    setup_logging(SCHEMA)
    provider = setup_tracing(SCHEMA, settings)
    relay = start_relay(settings, SCHEMA)
    stop = threading.Event()
    backoff = Backoff(settings.step_backoff_base_seconds, settings.step_backoff_max_seconds)
    threads = [
        threading.Thread(target=_loop, name="command-dispatcher", daemon=True, args=(
            settings, stop, lambda c: dispatch_once(c, settings, rules, backoff=backoff), IDLE_S)),
        threading.Thread(target=_loop, name="saga-deadlines", daemon=True, args=(
            settings, stop, lambda c: expire_once(c, settings, rules) > 0, EXPIRY_INTERVAL_S)),
    ]
    for t in threads:
        t.start()
    consumer = build_consumer(connect(settings.database_url),
                              make_consumer(settings.kafka_bootstrap_servers, group_id=SCHEMA), settings, rules)
    try:
        consumer.run_forever()
    finally:
        stop.set()
        consumer.close()
        relay.stop(10)
        for t in threads:
            t.join(5)
        if provider is not None:
            provider.shutdown()


if __name__ == "__main__":
    main()
