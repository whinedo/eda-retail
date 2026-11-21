"""Grabación y reproducción de llamadas al LLM (spec agent-harness, «Grabación y reproducción de llamadas»).

Con `LLM_RECORD_DIR`, `RecordingClient` guarda cada petición y su respuesta en `calls-<pid>.jsonl` y el harness guarda
cada ejecución (sus intentos, sus rechazos y su motivo) en `outcomes-<pid>.jsonl`, fuera del log y de la base. Con
`LLM_CLIENT=replay` y `LLM_REPLAY_DIR`, `ReplayClient` devuelve la respuesta grabada para la misma clave
`(agent, prompt_hash, hash de la petición renderizada)`, en el orden en que se grabó, sin contactar con ningún modelo.
Una petición sin grabación lanza `ReplayMiss` (`replay_miss`) antes de que cuente como generación del LLM.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from collections import defaultdict, deque
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any

from eda_retail.llm.generation import Generation, LlmClient, LlmRequest, LlmResponse

REPLAY_MISS = "replay_miss"


class ReplayMiss(RuntimeError):
    """La petición no está grabada: no se llama a ningún modelo."""

    code = REPLAY_MISS


def request_hash(request: LlmRequest) -> str:
    """Huella de la petición renderizada: prompt de sistema, prompt, esquema y `max_tokens` (sin la incidencia)."""
    digest = hashlib.sha256()
    for part in (request.system, request.prompt, json.dumps(request.schema, sort_keys=True), str(request.max_tokens)):
        digest.update(part.encode("utf-8"))
        digest.update(b"\0")
    return digest.hexdigest()


def request_key(request: LlmRequest) -> tuple[str, str | None, str]:
    return request.agent, request.prompt_hash, request_hash(request)


class _JsonlWriter:
    """Añade líneas JSON a un fichero por proceso; seguro entre hilos."""

    def __init__(self, directory: str | Path, prefix: str) -> None:
        self._path = Path(directory) / f"{prefix}-{os.getpid()}.jsonl"
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def write(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=False, sort_keys=True)
        with self._lock, self._path.open("a", encoding="utf-8") as f:
            f.write(line + "\n")


def read_jsonl(directory: str | Path, prefix: str) -> list[dict[str, Any]]:
    records = []
    for path in sorted(Path(directory).glob(f"{prefix}-*.jsonl")):
        with path.open(encoding="utf-8") as f:
            records += [json.loads(line) for line in f if line.strip()]
    return records


class RecordingClient:
    """Delega en el cliente real y graba cada petición con su respuesta."""

    def __init__(self, inner: LlmClient, directory: str | Path) -> None:
        self._inner = inner
        self._calls = _JsonlWriter(directory, "calls")

    @property
    def model(self) -> str:
        return getattr(self._inner, "model", "unknown")

    def complete(self, request: LlmRequest) -> LlmResponse:
        response = self._inner.complete(request)
        agent, prompt_hash, req_hash = request_key(request)
        self._calls.write({"agent": agent, "prompt_version": request.prompt_version, "prompt_hash": prompt_hash,
                           "request_hash": req_hash, "response": asdict(response)})
        return response


class ReplayClient:
    """Devuelve las respuestas grabadas en `directory`; nunca llama a un modelo."""

    model = "replay"

    def __init__(self, directory: str | Path) -> None:
        self._responses: dict[tuple[str, str | None, str], deque[LlmResponse]] = defaultdict(deque)
        for record in read_jsonl(directory, "calls"):
            key = (record["agent"], record["prompt_hash"], record["request_hash"])
            self._responses[key].append(LlmResponse(**record["response"]))
        self._lock = threading.Lock()

    def complete(self, request: LlmRequest) -> LlmResponse:
        key = request_key(request)
        with self._lock:
            queue = self._responses.get(key)
            if not queue:
                raise ReplayMiss(f"{REPLAY_MISS}: {request.agent} sin grabación para esta petición")
            return queue.popleft()


class OutcomeLog:
    """Graba cada ejecución del harness para `eda-agent-eval`: intentos, rechazos (código y texto completo, el
    mismo que se le devuelve al LLM) y motivo, sin prompt ni salida."""

    def __init__(self, directory: str | Path) -> None:
        self._outcomes = _JsonlWriter(directory, "outcomes")

    def __call__(self, request: LlmRequest, outcome: Generation[Any]) -> None:
        errors = iter(outcome.errors)  # el texto de cada rechazo, en el orden de los intentos rechazados
        self._outcomes.write({
            "agent": request.agent, "prompt_version": request.prompt_version, "prompt_hash": request.prompt_hash,
            "incident_id": request.incident_id, "accepted": outcome.value is not None, "reason": outcome.reason,
            "attempts": [{"attempt": c.attempt, "validation_passed": c.validation_passed, "rejection": c.rejection,
                          "error": None if c.validation_passed else next(errors, None),
                          "tokens_in": c.tokens_in, "tokens_out": c.tokens_out} for c in outcome.calls],
        })


def outcome_log_from_env(environ: Mapping[str, str] = os.environ) -> OutcomeLog | None:
    directory = environ.get("LLM_RECORD_DIR", "").strip()
    return OutcomeLog(directory) if directory else None
