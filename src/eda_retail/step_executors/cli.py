"""Proceso de un ejecutor: `eda-step-executor <acción>`, la API de comandos y el relay de su outbox.

Escucha en `EXECUTOR_<ACCIÓN>_URL` (8111–8114) y atiende el `ExecuteStep` que invoca la saga. Los
de `notify_customer` y `file_claim` usan Ollama para el redactor (`OLLAMA_HOST`, `LLM_MODEL`).
`reserve_stock` y `release_stock` los atiende `eda-inventory`. El cliente LLM lo elige `LLM_CLIENT` (Ollama por
defecto; `llm/select.py`) y, con `SERVICE_EXECUTIONS_LOG`, el servicio simulado anota cada ejecución en ese fichero
(`step_executors/executions.py`).
"""

from __future__ import annotations

import argparse

from eda_retail.config import load_settings
from eda_retail.infra.component import serve
from eda_retail.llm.select import client_from_env
from eda_retail.rules import BusinessRules
from eda_retail.step_executors.actions import ACTIONS
from eda_retail.step_executors.component import specs, url
from eda_retail.step_executors.executions import service_from_env


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Ejecutor de una acción (command handler ExecuteStep)")
    parser.add_argument("action", choices=ACTIONS)
    args = parser.parse_args(argv)
    settings = load_settings()
    schema = f"executor_{args.action}"
    command_specs = specs(settings, args.action, client_from_env(), BusinessRules(),
                          service=service_from_env(args.action))
    serve(f"executor-{args.action}", schema, command_specs, url(settings, args.action),
          limit_concurrency=settings.executor_max_concurrency)


if __name__ == "__main__":
    main()
