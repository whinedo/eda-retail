"""Proceso de un agente con LLM: `eda-agent interpreter|planner|replanner`.

Cada agente es un consumidor de Kafka con su propio `group.id` y nombre en el inbox
(design.md §3). Usa Ollama (`OLLAMA_HOST`, `LLM_MODEL`, `LLM_SEED`) y exporta trazas a
Jaeger como `agent-<nombre>`.

Si una decisión falla (vista sin actualizar, conflicto persistente), la excepción detiene
el proceso sin confirmar el offset; al reiniciar, Kafka vuelve a entregar el evento.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable

from eda_retail.agents.interpreter import Interpreter
from eda_retail.agents.planner import Planner
from eda_retail.agents.replanner import Replanner
from eda_retail.agents.runtime import AgentRunner
from eda_retail.command.handler import CommandHandler
from eda_retail.config import load_settings
from eda_retail.infra.consumer import EventConsumer
from eda_retail.infra.tracing import setup_tracing
from eda_retail.llm.generation import LlmClient
from eda_retail.llm.ollama_client import OllamaClient

AGENTS: dict[str, tuple[str, Callable[[LlmClient, CommandHandler], object]]] = {
    "interpreter": ("CarrierNoticeReceived", Interpreter),
    "planner": ("ShipmentDelayed", Planner),
    "replanner": ("StepFailed", Replanner),
}


def build_handlers(name: str, llm: LlmClient, handler: CommandHandler, *, inbox_enabled: bool = True) -> dict:
    event_type, factory = AGENTS[name]
    return {event_type: AgentRunner(factory(llm, handler), handler, inbox_enabled=inbox_enabled)}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Agente con LLM de la PoC")
    parser.add_argument("agent", choices=sorted(AGENTS))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    settings = load_settings()
    provider = setup_tracing(f"agent-{args.agent}", settings)
    handlers = build_handlers(args.agent, OllamaClient(), CommandHandler(), inbox_enabled=settings.inbox_enabled)
    consumer = EventConsumer.from_settings(settings, name=args.agent, handlers=handlers)
    try:
        consumer.run_forever()
    finally:
        consumer.close()
        if provider is not None:
            provider.shutdown()


if __name__ == "__main__":
    main()
