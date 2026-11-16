"""Proceso `eda-planner`: API de comandos del planificador y el relay de su outbox (`PLANNER_URL`).

Usa Ollama (`OLLAMA_HOST`, `LLM_MODEL`, `LLM_SEED`). La saga invoca `CreatePlan` (T7). Con `POC_VERSION=2`
consulta antes al agente del transportista por A2A (`CARRIER_AGENT_URL`, `A2A_CARRIER_TOKEN`).
"""

from __future__ import annotations

import argparse

from eda_retail.config import load_settings
from eda_retail.infra.component import serve
from eda_retail.llm.ollama_client import OllamaClient
from eda_retail.planner import carrier
from eda_retail.planner.handler import SCHEMA, specs
from eda_retail.rules import BusinessRules


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(description="Planificador (LLM + su command handler CreatePlan)").parse_args(argv)
    settings = load_settings()
    command_specs = specs(settings, OllamaClient(), BusinessRules(), carrier=carrier.from_settings(settings))
    serve("planner", SCHEMA, command_specs, settings.planner_url)


if __name__ == "__main__":
    main()
