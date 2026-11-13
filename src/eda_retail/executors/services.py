"""Servicios externos simulados (supuesto A8): correo, transportista, aseguradora y pagos.

Cada llamada lleva la clave del paso: si el ejecutor cae entre el efecto y el registro de
la clave y lo repite, el servicio devuelve el resultado anterior en lugar de duplicarlo
(spec idempotency, «Caída entre el efecto y el registro»). Para los tests y los
experimentos se pueden programar caídas (`down`), tiempos de espera vencidos
(`timeouts`, el número de llamadas que fallarán) y rechazos de negocio (`reject`).
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field

from eda_retail.executors.base import BusinessError, Transient


@dataclass
class FakeService:
    name: str
    prefix: str
    down: bool = False
    timeouts: int = 0
    reject: str | None = None  # error de negocio que devolverá cada llamada
    calls: int = 0
    _done: dict[str, str] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def call(self, key: str) -> str:
        """Ejecuta la operación con la clave del paso y devuelve su identificador externo."""
        with self._lock:
            self.calls += 1
            if self.down:
                raise Transient("timeout")
            if self.timeouts > 0:
                self.timeouts -= 1
                raise Transient("timeout")
            if self.reject is not None:
                raise BusinessError(self.reject)
            if key not in self._done:
                self._done[key] = f"{self.prefix}-{uuid.uuid4().hex[:8]}"
            return self._done[key]

    @property
    def executed(self) -> int:
        return len(self._done)


@dataclass
class Services:
    mail: FakeService = field(default_factory=lambda: FakeService("mail", "M"))
    claims: FakeService = field(default_factory=lambda: FakeService("claims", "C"))
    carrier: FakeService = field(default_factory=lambda: FakeService("carrier", "SH"))
    payments: FakeService = field(default_factory=lambda: FakeService("payments", "RF"))
