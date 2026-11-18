"""Proceso `eda-replanner`: API de comandos del replanificador y el relay de su outbox (`REPLANNER_URL`).

Usa Ollama (`OLLAMA_HOST`, `LLM_MODEL`, `LLM_SEED`), o el cliente que diga `LLM_CLIENT` (`llm/select.py`).
La saga invoca `RevisePlan` (T7).
"""

from __future__ import annotations

import argparse

from eda_retail.config import load_settings
from eda_retail.infra.component import serve
from eda_retail.llm.select import client_from_env
from eda_retail.replanner.handler import SCHEMA, specs
from eda_retail.rules import BusinessRules


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(description="Replanificador (LLM + su command handler RevisePlan)").parse_args(argv)
    settings = load_settings()
    serve("replanner", SCHEMA, specs(settings, client_from_env(), BusinessRules()), settings.replanner_url)


if __name__ == "__main__":
    main()
