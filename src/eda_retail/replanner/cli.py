"""Proceso `eda-replanner`: API de comandos del replanificador y el relay de su outbox (`REPLANNER_URL`).

Usa Ollama (`OLLAMA_HOST`, `LLM_MODEL`, `LLM_SEED`). La saga invoca `RevisePlan` (T7).
"""

from __future__ import annotations

import argparse

from eda_retail.config import load_settings
from eda_retail.infra.component import serve
from eda_retail.llm.ollama_client import OllamaClient
from eda_retail.replanner.handler import SCHEMA, specs
from eda_retail.rules import BusinessRules


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(description="Replanificador (LLM + su command handler RevisePlan)").parse_args(argv)
    settings = load_settings()
    serve("replanner", SCHEMA, specs(settings, OllamaClient(), BusinessRules()), settings.replanner_url)


if __name__ == "__main__":
    main()
