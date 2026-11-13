"""Proceso de un ejecutor: `eda-executor <acción>`.

Cada acción es un consumidor con su propio `group.id` (`executor-<acción>`), así que un
ejecutor lento o caído no frena a los demás (spec resilience, «Aislamiento entre
ejecutores»). Procesa como mucho `EXECUTOR_MAX_CONCURRENCY` pasos a la vez y reintenta
los errores transitorios hasta `STEP_MAX_ATTEMPTS` con espera exponencial y jitter.
"""

from __future__ import annotations

import argparse
from functools import partial

from eda_retail.command.escalation import escalation_hook
from eda_retail.command.handler import CommandHandler
from eda_retail.config import Settings, load_settings
from eda_retail.events.catalog import Action
from eda_retail.executors.actions import build_actions
from eda_retail.executors.base import Executor, ExecutorSettings, dead_letter_on_exhaustion
from eda_retail.executors.pool import ConcurrentRunner
from eda_retail.executors.retry import Backoff
from eda_retail.infra.db import connect
from eda_retail.infra.kafka import make_consumer
from eda_retail.infra.logs import setup_logging
from eda_retail.infra.tracing import setup_tracing
from eda_retail.llm.generation import LlmClient
from eda_retail.llm.ollama_client import OllamaClient


def build_executor(action: str, llm: LlmClient, handler: CommandHandler, settings: Settings) -> Executor:
    name = f"executor-{action}"
    return Executor(
        build_actions(llm, handler)[action], handler,
        settings=ExecutorSettings.from_settings(settings),
        backoff=Backoff(settings.step_backoff_base_seconds, settings.step_backoff_max_seconds),
        on_exhausted=dead_letter_on_exhaustion(name),
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ejecutor de una acción del plan")
    parser.add_argument("action", choices=sorted(a.value for a in Action))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_settings()
    setup_logging(f"executor-{args.action}")
    provider = setup_tracing(f"executor-{args.action}", settings)
    handler = CommandHandler()
    executor = build_executor(args.action, OllamaClient(), handler, settings)
    runner = ConcurrentRunner(
        executor, partial(connect, settings.database_url), max_concurrency=settings.executor_max_concurrency,
        name=executor.name, inbox_enabled=settings.inbox_enabled,
        on_dead_letter=escalation_hook(handler, producer=executor.name),
    )
    consumer = make_consumer(settings.kafka_bootstrap_servers, group_id=executor.name)
    consumer.subscribe([settings.events_topic])
    try:
        while True:
            runner.run_once(consumer)
    finally:
        consumer.close()
        runner.close()
        if provider is not None:
            provider.shutdown()


if __name__ == "__main__":
    main()
