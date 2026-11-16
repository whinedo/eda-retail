"""Espera exponencial con jitter completo (spec resilience, «Reintentos con espera exponencial»).

La espera antes del intento `n + 1` es un valor aleatorio en [0, min(máximo, base · 2^(n-1))].
El generador lleva semilla para que los experimentos sean repetibles.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field


@dataclass
class Backoff:
    base_seconds: float = 0.5
    max_seconds: float = 10.0
    seed: int | None = None
    _rng: random.Random = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._rng = random.Random(self.seed)

    def cap(self, attempt: int) -> float:
        """Tope de la espera tras el intento `attempt` (1, 2, …)."""
        return min(self.max_seconds, self.base_seconds * 2 ** (attempt - 1))

    def delay(self, attempt: int) -> float:
        return self._rng.uniform(0, self.cap(attempt))
