"""Cliente LLM de un proceso, elegido por `LLM_CLIENT` (spec experiments, «Todos los componentes en procesos propios»).

- `ollama` (por defecto, también sin definir o vacía): `OllamaClient`, el de producción y `scripts/run-agents.sh`.
- `oracle`: el oráculo de los experimentos (`experiments/oracle.py`) sobre el conjunto
  `generate(ORACLE_SEED, ORACLE_INCIDENTS)`, el mismo que usa el harness; así un proceso responde como el harness en
  proceso y la ejecución no depende de Ollama.

- `replay`: las respuestas grabadas en `LLM_REPLAY_DIR` (`llm/replay.py`), sin contactar con ningún modelo.

Con `LLM_RECORD_DIR`, el cliente elegido (salvo `replay`) graba cada petición y su respuesta en ese directorio (spec
agent-harness, «Grabación y reproducción de llamadas»).

Cualquier otro valor es un error de configuración: el proceso no arranca.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

from eda_retail.llm.generation import LlmClient
from eda_retail.llm.ollama_client import OllamaClient
from eda_retail.llm.replay import RecordingClient, ReplayClient

LLM_CLIENTS = ("ollama", "oracle", "replay")


def _required_int(environ: Mapping[str, str], name: str) -> int:
    raw = environ.get(name, "").strip()
    if not raw:
        raise ValueError(f"LLM_CLIENT=oracle necesita {name}")
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name} debe ser un entero, no {raw!r}") from None


def client_from_env(environ: Mapping[str, str] = os.environ) -> LlmClient:
    """El cliente LLM que dice `LLM_CLIENT`; ValueError si el valor no es `ollama`, `oracle` ni `replay`."""
    raw = environ.get("LLM_CLIENT", "").strip() or "ollama"
    if raw == "replay":
        directory = environ.get("LLM_REPLAY_DIR", "").strip()
        if not directory:
            raise ValueError("LLM_CLIENT=replay necesita LLM_REPLAY_DIR")
        return ReplayClient(directory)
    client = _base_client(raw, environ)
    record = environ.get("LLM_RECORD_DIR", "").strip()
    return RecordingClient(client, record) if record else client


def _base_client(raw: str, environ: Mapping[str, str]) -> LlmClient:
    if raw == "ollama":
        return OllamaClient()
    if raw == "oracle":
        seed, incidents = _required_int(environ, "ORACLE_SEED"), _required_int(environ, "ORACLE_INCIDENTS")
        # Import diferido: el oráculo es de los experimentos y solo lo cargan los procesos que lo piden.
        from eda_retail.experiments.oracle import Oracle
        from eda_retail.synthetic.generator import generate

        return Oracle(generate(seed, incidents)).client()
    raise ValueError(f"LLM_CLIENT debe ser {', '.join(LLM_CLIENTS[:-1])} o {LLM_CLIENTS[-1]}, no {raw!r}")
