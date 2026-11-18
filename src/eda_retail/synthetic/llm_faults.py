"""Degradación del LLM (spec synthetic-data, «Degradación del LLM»; E15).

`DegradedLlm` envuelve un cliente LLM y degrada las llamadas de los agentes elegidos (por defecto, `planner` y
`replanner`):

- `invalid`: con probabilidad `probability`, la respuesta no es JSON o es un objeto sin los campos del esquema (una
  llamada de cada dos de las elegidas, alternando).
- `slow`: cada llamada tarda `delay_seconds` más (se duerme antes de llamar al cliente).
- `error`: con probabilidad `probability`, la llamada lanza `LlmUnavailable`, como un timeout de Ollama.

La elección es una función de la semilla, el modo, el agente, la incidencia y el número de llamada de ese agente en esa
incidencia (`should_fail`), no de un generador compartido: con la misma semilla se degradan las mismas llamadas aunque
cambie el orden entre incidencias. Seguro entre hilos (cada componente llama desde el hilo de su worker).
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import replace

from eda_retail.llm.generation import LlmClient, LlmRequest, LlmResponse
from eda_retail.synthetic.faults import should_fail

LLM_MODES = ("invalid", "slow", "error")
DEFAULT_AGENTS = ("planner", "replanner")
NOT_JSON = "lo siento, no puedo responder en JSON"
MISSING_FIELDS = "{}"


class LlmUnavailable(RuntimeError):
    """El LLM no responde (inyectado)."""


class DegradedLlm:
    def __init__(self, client: LlmClient, mode: str, *, seed: int = 0, probability: float = 0.0,
                 delay_seconds: float = 0.0, agents: tuple[str, ...] = DEFAULT_AGENTS,
                 sleep: Callable[[float], object] = time.sleep) -> None:
        if mode not in LLM_MODES:
            raise ValueError(f"modo de degradación desconocido: {mode}")
        if not 0.0 <= probability <= 1.0:
            raise ValueError(f"probabilidad fuera de [0, 1]: {probability}")
        if delay_seconds < 0:
            raise ValueError(f"delay_seconds no puede ser negativo: {delay_seconds}")
        self._client = client
        self._mode = mode
        self._seed = seed
        self._probability = probability
        self._delay = delay_seconds
        self._agents = frozenset(agents)
        self._sleep = sleep
        self._lock = threading.Lock()
        self._calls: Counter[tuple[str, str]] = Counter()
        self.injected: Counter[str] = Counter()  # degradaciones aplicadas por agente

    @property
    def model(self) -> str:
        return getattr(self._client, "model", "unknown")

    def _chosen(self, request: LlmRequest, n: int) -> bool:
        key = f"{request.agent}:{request.incident_id}:{n}"
        return should_fail(self._seed, self._mode, key, self._probability)

    def complete(self, request: LlmRequest) -> LlmResponse:
        if request.agent not in self._agents:
            return self._client.complete(request)
        with self._lock:
            self._calls[(request.agent, request.incident_id)] += 1
            n = self._calls[(request.agent, request.incident_id)]
        if self._mode == "slow":
            self._count(request.agent)
            self._sleep(self._delay)
            response = self._client.complete(request)
            return replace(response, latency_ms=response.latency_ms + round(self._delay * 1000))
        if not self._chosen(request, n):
            return self._client.complete(request)
        self._count(request.agent)
        if self._mode == "error":
            raise LlmUnavailable(f"LLM sin respuesta ({request.agent}, {request.incident_id}, llamada {n})")
        response = self._client.complete(request)
        return replace(response, text=NOT_JSON if n % 2 else MISSING_FIELDS)

    def _count(self, agent: str) -> None:
        with self._lock:
            self.injected[agent] += 1
