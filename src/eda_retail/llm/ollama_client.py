"""Cliente de Ollama: una petición `/api/chat` sin herramientas, con salida estructurada.

El agente no recibe ninguna herramienta (spec llm-boundaries, "El LLM no ejecuta efectos").
El esquema JSON va en `format`; aun así, el código vuelve a validar la salida con pydantic.
`temperature` 0 y `seed` fija hacen las ejecuciones repetibles (experimentos E1 a E7).

Configuración: `OLLAMA_HOST` (por defecto http://localhost:11434), `LLM_MODEL`,
`LLM_SEED` y `LLM_TIMEOUT_SECONDS`. Si Ollama no responde, la excepción se propaga: el
consumidor no confirma el offset y el evento se vuelve a procesar.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

from eda_retail.llm.generation import LlmRequest, LlmResponse

DEFAULT_HOST = "http://localhost:11434"
DEFAULT_MODEL = "llama3.1"



class OllamaModelNotFound(RuntimeError):
    """Ollama responde 404: el modelo pedido no está descargado."""


Post = Callable[[str, dict[str, Any], float], dict[str, Any]]


def _http_post(url: str, body: dict[str, Any], timeout: float) -> dict[str, Any]:
    request = urllib.request.Request(
        url, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read())


class OllamaClient:
    def __init__(
        self,
        *,
        host: str | None = None,
        model: str | None = None,
        seed: int | None = None,
        timeout: float | None = None,
        post: Post | None = None,
    ) -> None:
        self.host = (host or os.environ.get("OLLAMA_HOST", DEFAULT_HOST)).rstrip("/")
        self.model = model or os.environ.get("LLM_MODEL", DEFAULT_MODEL)
        self.seed = seed if seed is not None else int(os.environ.get("LLM_SEED", "42"))
        self.timeout = timeout if timeout is not None else float(os.environ.get("LLM_TIMEOUT_SECONDS", "120"))
        self._post = post or _http_post

    def request_body(self, request: LlmRequest) -> dict[str, Any]:
        return {
            "model": self.model,
            "messages": [
                {"role": "system", "content": request.system},
                {"role": "user", "content": request.prompt},
            ],
            "format": request.schema,
            "stream": False,
            "options": {"temperature": 0, "seed": self.seed, "num_predict": request.max_tokens},
        }

    def complete(self, request: LlmRequest) -> LlmResponse:
        try:
            reply = self._post(f"{self.host}/api/chat", self.request_body(request), self.timeout)
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                raise OllamaModelNotFound(
                    f"Ollama ({self.host}) no tiene el modelo '{self.model}': descárgalo con "
                    f"`ollama pull {self.model}` o fija LLM_MODEL"
                ) from exc
            raise
        return LlmResponse(
            text=reply.get("message", {}).get("content", ""),
            model=reply.get("model", self.model),
            tokens_in=reply.get("prompt_eval_count", 0),
            tokens_out=reply.get("eval_count", 0),
            latency_ms=reply.get("total_duration", 0) // 1_000_000,
        )
