"""Proceso `eda-interpreter`: consume `CarrierNoticeReceived` y `OrderRegistered` (grupo `interpreter`),
publica su outbox con su relay y resuelve los avisos aparcados cuyo plazo vence.

Usa Ollama (`OLLAMA_HOST`, `LLM_MODEL`, `LLM_SEED`). Si un aviso falla, la excepción detiene el
proceso sin confirmar el offset; al reiniciar, Kafka vuelve a entregarlo.
"""

from __future__ import annotations

import argparse
import logging
import threading

from eda_retail.command.rules import BusinessRules
from eda_retail.config import Settings, load_settings
from eda_retail.infra.db import connect
from eda_retail.infra.kafka import make_consumer
from eda_retail.infra.logs import setup_logging
from eda_retail.infra.outbox import start_relay
from eda_retail.infra.tracing import setup_tracing
from eda_retail.interpreter.consumer import build_consumer, expire_parked
from eda_retail.interpreter.handler import SCHEMA
from eda_retail.llm.ollama_client import OllamaClient

log = logging.getLogger("eda_retail.interpreter")
EXPIRY_INTERVAL_S = 1.0


def _expire_forever(settings: Settings, stop: threading.Event) -> None:
    conn = connect(settings.database_url)
    try:
        while not stop.is_set():
            if resolved := expire_parked(conn):
                log.info("plazo vencido: %d avisos aparcados resueltos", resolved)
            stop.wait(EXPIRY_INTERVAL_S)
    finally:
        conn.close()


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(description="Intérprete de avisos (LLM + su command handler)").parse_args(argv)
    settings = load_settings()
    setup_logging("interpreter")
    provider = setup_tracing("interpreter", settings)
    relay = start_relay(settings, SCHEMA)
    stop = threading.Event()
    expiry = threading.Thread(target=_expire_forever, args=(settings, stop), name="parked-notices", daemon=True)
    expiry.start()
    consumer = build_consumer(
        connect(settings.database_url), make_consumer(settings.kafka_bootstrap_servers, group_id=SCHEMA),
        settings, OllamaClient(), BusinessRules(),
    )
    try:
        consumer.run_forever()
    finally:
        stop.set()
        consumer.close()
        relay.stop(10)
        expiry.join(5)
        if provider is not None:
            provider.shutdown()


if __name__ == "__main__":
    main()
