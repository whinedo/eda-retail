"""Cliente LLM simulado y determinista para tests y experimentos sin coste.

Devuelve, en orden, las salidas preparadas: un dict o un modelo pydantic (se serializa a
JSON), un texto tal cual, `REFUSAL` o una función que recibe la petición. Los tokens se
cuentan por palabras, así que el coste también es repetible.
"""

from __future__ import annotations

import json
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


def _to_text(out: Any) -> str:
    if isinstance(out, BaseModel):
        return out.model_dump_json()
    if isinstance(out, str):
        return out
    return json.dumps(out)
