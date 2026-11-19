"""Harness de agentes (spec agent-harness): la capa común alrededor de cada llamada al LLM.

Cada agente con LLM declara su `AgentSpec` (nombre, versión y huella del prompt, modelo de salida, comprobación
de negocio y `max_tokens`). `AgentHarness.run` acota la entrada no confiable, pide la salida con `generate`
y la valida en orden: esquema, grounding de las referencias y reglas de negocio. No escribe, no publica y no
ejecuta efectos: devuelve un `AgentOutcome` y el componente registra las llamadas en su transacción
(`eda_retail.llm.recording.record_llm_calls`).
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import cached_property
from typing import Any

from pydantic import BaseModel

from eda_retail.llm.generation import UNGROUNDED_REFERENCE, Budget, Generation, LlmClient, LlmRequest, generate
from eda_retail.llm.schemas import strict_schema

INPUT_TOO_LONG = "input_too_long"

# El resultado de una ejecución del harness: el valor aceptado o el motivo de escalado, más las llamadas.
AgentOutcome = Generation

Check = Callable[[Any], str | None]


class UnknownAgent(LookupError):
    """Error de configuración: el agente no tiene `AgentSpec` registrado."""


@dataclass(frozen=True)
class AgentSpec[T: BaseModel]:
    """El contrato de un agente con LLM. `templates` son las plantillas con las que construye su prompt."""

    agent: str
    prompt_version: str
    system: str
    templates: tuple[str, ...]
    output: type[T]
    max_tokens: int = 16_000
    check: Callable[[T], str | None] | None = None

    @cached_property
    def prompt_hash(self) -> str:
        """SHA-256 del prompt de sistema y de las plantillas, sin la entrada."""
        digest = hashlib.sha256()
        for part in (self.system, *self.templates):
            digest.update(part.encode("utf-8"))
            digest.update(b"\0")
        return digest.hexdigest()

    def request(self, incident_id: str, prompt: str) -> LlmRequest:
        return LlmRequest(
            agent=self.agent, incident_id=incident_id, system=self.system, prompt=prompt,
            schema=strict_schema(self.output), max_tokens=self.max_tokens,
            prompt_version=self.prompt_version, prompt_hash=self.prompt_hash,
        )


def spec_for(agent: str) -> AgentSpec[Any]:
    from eda_retail.llm.agents import AGENTS  # import diferido: los prompts importan este módulo

    try:
        return AGENTS[agent]
    except KeyError:
        raise UnknownAgent(f"el agente {agent!r} no tiene AgentSpec registrado") from None


def delimit(tag: str, text: str) -> str:
    """El texto no confiable como un único bloque `<tag>`: neutraliza las etiquetas `tag` que traiga dentro."""
    pattern = re.compile(rf"<\s*(/?)\s*{re.escape(tag)}\s*>", re.IGNORECASE)
    safe = pattern.sub(lambda m: f"[{m.group(1)}{tag}]", text)
    return f"<{tag}>\n{safe}\n</{tag}>"


def ground_in_text(refs: Iterable[str], text: str) -> str | None:
    """`ungrounded_reference` si alguna referencia no aparece literalmente en el texto."""
    missing = [r for r in refs if not re.search(rf"(?<![\w-]){re.escape(r)}(?![\w-])", text)]
    return f"{UNGROUNDED_REFERENCE}: {', '.join(missing)} no aparece en el texto de entrada" if missing else None


def ground_in(refs: Iterable[str], allowed: Iterable[str]) -> str | None:
    """`ungrounded_reference` si alguna referencia no es de las permitidas."""
    known = set(allowed)
    missing = [r for r in refs if r not in known]
    return f"{UNGROUNDED_REFERENCE}: {', '.join(missing)} no es de la incidencia" if missing else None


OutcomeHook = Callable[[LlmRequest, Generation[Any]], None]


class AgentHarness:
    """Ejecuta un agente: acota la entrada, genera y valida. Sin escrituras."""

    def __init__(self, llm: LlmClient, *, max_generations: int, max_input_chars: int | None = None,
                 on_outcome: OutcomeHook | None = None) -> None:
        self._llm = llm
        self._max_generations = max_generations
        self._max_input_chars = max_input_chars
        self._on_outcome = on_outcome

    def run(self, request: LlmRequest, *, budget: Budget, check: Check | None = None,
            ground: Check | None = None, untrusted: str | None = None) -> AgentOutcome[Any]:
        spec = spec_for(request.agent)
        if request.prompt_hash != spec.prompt_hash:
            raise ValueError(f"la petición de {request.agent} no se construyó con su AgentSpec")
        if untrusted is not None and self._max_input_chars is not None and len(untrusted) > self._max_input_chars:
            outcome: Generation[Any] = Generation(None, INPUT_TOO_LONG)  # sin llamar al LLM
        else:
            checks = [c for c in (ground, spec.check, check) if c is not None]  # grounding antes que negocio

            def validate(value: Any) -> str | None:
                for c in checks:
                    if (error := c(value)) is not None:
                        return error
                return None

            outcome = generate(self._llm, request, spec.output, max_generations=self._max_generations,
                               budget=budget, check=validate)
        if self._on_outcome is not None:
            self._on_outcome(request, outcome)
        return outcome
