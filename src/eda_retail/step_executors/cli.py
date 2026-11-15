"""Proceso de un ejecutor: `eda-step-executor <acción>`, la API de comandos y el relay de su outbox.

Escucha en `EXECUTOR_<ACCIÓN>_URL` (8111–8114) y atiende el `ExecuteStep` que invoca la saga. Los
de `notify_customer` y `file_claim` usan Ollama para el redactor (`OLLAMA_HOST`, `LLM_MODEL`).
`reserve_stock` y `release_stock` los atiende `eda-inventory`.
"""

from __future__ import annotations

import argparse

from eda_retail.config import load_settings
from eda_retail.infra.component import serve
from eda_retail.llm.ollama_client import OllamaClient
from eda_retail.rules import BusinessRules
from eda_retail.step_executors.actions import ACTIONS
from eda_retail.step_executors.component import specs, url


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ejecutor de una acción (command handler ExecuteStep)")
    parser.add_argument("action", choices=ACTIONS)
    args = parser.parse_args(argv)
    settings = load_settings()
    schema = f"executor_{args.action}"
    serve(f"executor-{args.action}", schema, specs(settings, args.action, OllamaClient(), BusinessRules()),
          url(settings, args.action))


if __name__ == "__main__":
    main()
