"""Cliente real de Claude: una petición sin herramientas, con salida estructurada.

El agente no recibe ninguna herramienta (spec llm-boundaries, "El LLM no ejecuta efectos").
El esquema JSON va en `output_config.format`; aun así, el código vuelve a validar la salida
con pydantic. Si los clasificadores rechazan la petición, `fallbacks="default"` la repite en
el modelo de respaldo que recomienda la API; si también se rechaza, la salida es inválida.
"""

from __future__ import annotations

import os
import time
from typing import Any

from eda_retail.llm.generation import LlmRequest, LlmResponse

FALLBACK_BETA = "server-side-fallback-2026-07-01"
DEFAULT_MODEL = "claude-opus-5-5"


class AnthropicClient:
    def __init__(self, *, model: str | None = None, effort: str | None = None, sdk: Any = None) -> None:
        self.model = model or os.environ.get("LLM_MODEL", DEFAULT_MODEL)
        self.effort = effort or os.environ.get("LLM_EFFORT", "medium")
        if sdk is None:
            import anthropic

            sdk = anthropic.Anthropic()
        self._sdk = sdk

    def request_kwargs(self, request: LlmRequest) -> dict[str, Any]:
        return {
            "model": self.model,
            "max_tokens": request.max_tokens,
            "system": request.system,
            "messages": [{"role": "user", "content": request.prompt}],
            "output_config": {
                "effort": self.effort,
                "format": {"type": "json_schema", "schema": request.schema},
            },
            "betas": [FALLBACK_BETA],
            "fallbacks": "default",
        }

    def complete(self, request: LlmRequest) -> LlmResponse:
        started = time.monotonic()
        message = self._sdk.beta.messages.create(**self.request_kwargs(request))
        latency_ms = int((time.monotonic() - started) * 1000)
        usage = message.usage
        tokens_in = (
            usage.input_tokens
            + (getattr(usage, "cache_creation_input_tokens", 0) or 0)
            + (getattr(usage, "cache_read_input_tokens", 0) or 0)
        )
        refused = message.stop_reason == "refusal"
        text = "" if refused else next((b.text for b in message.content if b.type == "text"), "")
        return LlmResponse(
            text=text, model=message.model, tokens_in=tokens_in, tokens_out=usage.output_tokens,
            latency_ms=latency_ms, refused=refused,
        )
