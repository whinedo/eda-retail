"""Proceso del orquestador: `eda-orchestrator`. Una instancia basta; es un consumidor sin
estado propio (supuesto A1), así que reiniciarlo no pierde nada."""

from __future__ import annotations

import argparse

from eda_retail.command.escalation import escalation_hook
from eda_retail.command.handler import CommandHandler
from eda_retail.config import load_settings
from eda_retail.infra.consumer import EventConsumer
from eda_retail.infra.tracing import setup_tracing
from eda_retail.orchestrator.runner import NAME, Orchestrator


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(description="Orquestador del plan y de la saga").parse_args(argv)
    settings = load_settings()
    provider = setup_tracing(NAME, settings)
    handler = CommandHandler()
    orchestrator = Orchestrator(handler, inbox_enabled=settings.inbox_enabled)
    consumer = EventConsumer.from_settings(
        settings, name=NAME, handlers=orchestrator.handlers, on_dead_letter=escalation_hook(handler, producer=NAME)
    )
    try:
        consumer.run_forever()
    finally:
        consumer.close()
        if provider is not None:
            provider.shutdown()


if __name__ == "__main__":
    main()
