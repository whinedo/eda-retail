"""Proceso `eda-planner`: API de comandos del planificador y el relay de su outbox (`PLANNER_URL`).

Usa Ollama (`OLLAMA_HOST`, `LLM_MODEL`, `LLM_SEED`). La saga invoca `CreatePlan` (T7).
"""

from __future__ import annotations

import argparse

from eda_retail.config import load_settings
from eda_retail.infra.component import serve
from eda_retail.llm.ollama_client import OllamaClient
from eda_retail.planner.handler import SCHEMA, specs
from eda_retail.rules import BusinessRules


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(description="Planificador (LLM + su command handler CreatePlan)").parse_args(argv)
    settings = load_settings()
    serve("planner", SCHEMA, specs(settings, OllamaClient(), BusinessRules()), settings.planner_url)


if __name__ == "__main__":
    main()
