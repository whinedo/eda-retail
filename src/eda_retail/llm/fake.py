"""Cliente LLM simulado y determinista para tests y experimentos sin coste.

Devuelve, en orden, las salidas preparadas: un dict o un modelo pydantic (se serializa a
JSON), un texto tal cual, `REFUSAL` o una función que recibe la petición. Los tokens se
cuentan por palabras, así que el coste también es repetible.

`CountingLlmClient` envuelve cualquier cliente y cuenta sus llamadas por agente (E8).
"""

from __future__ import annotations

import json
import threading
from collections import Counter
from collections.abc import Callable, Iterable
from typing import Any

from pydantic import BaseModel

from eda_retail.llm.generation import LlmRequest, LlmResponse

Output = str | dict[str, Any] | BaseModel | Callable[[LlmRequest], Any]


class FakeLlmClient:
    REFUSAL = object()
    model = "fake-model"

    def __init__(self, outputs: Iterable[Any] = (), *, responder: Callable[[LlmRequest], Any] | None = None) -> None:
        self._outputs = list(outputs)
        self._responder = responder
        self.requests: list[LlmRequest] = []

    def complete(self, request: LlmRequest) -> LlmResponse:
        self.requests.append(request)
        if self._outputs:
            out = self._outputs.pop(0)
        elif self._responder is not None:
            out = self._responder
        else:
            raise AssertionError("FakeLlmClient: no quedan salidas preparadas")
        if callable(out) and out is not self.REFUSAL:
            out = out(request)
        tokens_in = len(request.system.split()) + len(request.prompt.split())
        if out is self.REFUSAL:
            return LlmResponse("", self.model, tokens_in, 0, latency_ms=1, refused=True)
        text = _to_text(out)
        return LlmResponse(text, self.model, tokens_in, len(text.split()), latency_ms=1)


class CountingLlmClient:
    """Cliente del LLM que delega en otro y cuenta sus llamadas por agente (`LlmRequest.agent`) en `calls`. Cuenta
    también las que no dejan rastro en la base: una caída `before_tx` se salta la transacción que escribe `llm_calls` y
    `LlmCallRecorded` (spec synthetic-data, «Caída en un punto de la transacción»). Seguro entre hilos."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self._lock = threading.Lock()
        self.calls: Counter[str] = Counter()

    @property
    def model(self) -> str:
        return self._inner.model

    def complete(self, request: LlmRequest) -> LlmResponse:
        with self._lock:
            self.calls[request.agent] += 1
        return self._inner.complete(request)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


def _to_text(out: Any) -> str:
    if isinstance(out, BaseModel):
        return out.model_dump_json()
    if isinstance(out, str):
        return out
    return json.dumps(out)
