"""Frontera con el LLM (spec llm-boundaries): el LLM propone y el código dispone.

`generate` pide una salida al LLM, la valida con su esquema pydantic y con las reglas de
negocio del llamante, y repite como mucho `max_generations` veces. Antes de cada llamada
comprueba el presupuesto de la incidencia; si está agotado, no llama. No escribe nada: el
agente registra después cada llamada (`RecordLlmCall`) en su transacción.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from typing import Any, Protocol

from pydantic import BaseModel, ValidationError

from eda_retail.infra import tracing

LLM_OUTPUT_INVALID = "llm_output_invalid"
BUDGET_EXHAUSTED = "budget_exhausted"
LLM_REFUSED = "llm_refused"


@dataclass(frozen=True)
class LlmRequest:
    agent: str
    incident_id: str
    system: str
    prompt: str
    schema: dict[str, Any]
    max_tokens: int = 16_000


@dataclass(frozen=True)
class LlmResponse:
    text: str
    model: str
    tokens_in: int
    tokens_out: int
    latency_ms: int
    refused: bool = False


class LlmClient(Protocol):
    """Cliente sin herramientas: recibe texto y un esquema, devuelve texto."""

    def complete(self, request: LlmRequest) -> LlmResponse: ...


@dataclass(frozen=True)
class LlmCall:
    """Lo que se registra de una llamada (evento `LlmCallRecorded`): sin prompt ni salida."""

    agent: str
    model: str
    tokens_in: int
    tokens_out: int
    latency_ms: int
    validation_passed: bool
    attempt: int


@dataclass(frozen=True)
class Budget:
    """Presupuesto por incidencia (spec llm-boundaries, "Presupuesto por incidencia")."""

    tokens_used: int
    llm_calls: int
    max_tokens: int
    max_calls: int

    def exhausted(self) -> bool:
        return self.tokens_used >= self.max_tokens or self.llm_calls >= self.max_calls

    def charge(self, call: LlmCall) -> Budget:
        return replace(
            self, tokens_used=self.tokens_used + call.tokens_in + call.tokens_out, llm_calls=self.llm_calls + 1
        )


@dataclass(frozen=True)
class Generation[T: BaseModel]:
    value: T | None
    reason: str | None  # None si hay valor; llm_output_invalid o budget_exhausted si no
    calls: list[LlmCall] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def parse_output[T: BaseModel](response: LlmResponse, output: type[T]) -> tuple[T | None, str | None]:
    """Convierte la respuesta en el modelo pydantic; devuelve (valor, error)."""
    if response.refused:
        return None, LLM_REFUSED
    try:
        data = json.loads(response.text)
    except json.JSONDecodeError:
        return None, "salida que no es JSON"
    try:
        return output.model_validate(data), None
    except ValidationError as exc:
        fields = ", ".join(".".join(str(p) for p in e["loc"]) or "(raíz)" for e in exc.errors())
        return None, f"campos ausentes o inválidos: {fields}"


def generate[T: BaseModel](
    client: LlmClient,
    request: LlmRequest,
    output: type[T],
    *,
    max_generations: int,
    budget: Budget,
    check: Callable[[T], str | None] | None = None,
) -> Generation[T]:
    calls: list[LlmCall] = []
    errors: list[str] = []
    prompt = request.prompt
    for attempt in range(1, max_generations + 1):
        if budget.exhausted():
            return Generation(None, BUDGET_EXHAUSTED, calls, errors)
        current = replace(request, prompt=prompt)
        response = _call(client, current, attempt)
        value, error = parse_output(response, output)
        if value is not None and check is not None:
            error = check(value)
        call = LlmCall(
            agent=request.agent, model=response.model, tokens_in=response.tokens_in,
            tokens_out=response.tokens_out, latency_ms=response.latency_ms,
            validation_passed=error is None, attempt=attempt,
        )
        calls.append(call)
        budget = budget.charge(call)
        if error is None:
            return Generation(value, None, calls, errors)
        errors.append(error)
        prompt = f"{request.prompt}\n\nLa propuesta anterior se rechazó: {error}. Genera una nueva."
    return Generation(None, LLM_OUTPUT_INVALID, calls, errors)


def _call(client: LlmClient, request: LlmRequest, attempt: int) -> LlmResponse:
    with tracing.tracer().start_as_current_span(
        f"llm {request.agent}",
        attributes={"eda.agent": request.agent, "eda.incident_id": request.incident_id, "eda.attempt": attempt},
    ) as span:
        started = time.monotonic()
        response = client.complete(request)
        if response.latency_ms == 0:
            response = replace(response, latency_ms=int((time.monotonic() - started) * 1000))
        span.set_attributes({
            "eda.model": response.model, "eda.tokens_in": response.tokens_in,
            "eda.tokens_out": response.tokens_out, "eda.latency_ms": response.latency_ms,
            "eda.refused": response.refused,
        })
        return response
