"""Servicios externos simulados (supuesto A8): correo, transportista, aseguradora y pagos.

Cada llamada lleva la clave del paso: si el ejecutor cae entre el efecto y el registro de
la clave y lo repite, el servicio devuelve el resultado anterior en lugar de duplicarlo
(spec idempotency, «Caída entre el efecto y el registro»). Para los tests y los
experimentos se pueden programar caídas (`down`), tiempos de espera vencidos
(`timeouts`, el número de llamadas que fallarán) y rechazos de negocio (`reject`).

Con `idempotent=False` (experimento E1 sin idempotencia) el servicio no reconoce la clave y
repite el efecto; `executions` cuenta cuántas veces se ejecutó cada clave y `on_execute`, si se da, recibe
cada ejecución (servicio, clave, identificador externo; `executions.ExecutionLog` la anota en un fichero).
"""

from __future__ import annotations

import threading
import uuid
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field

from eda_retail.step_executors.errors import BusinessError, Transient

# Servicio simulado (atributo de `Services`) de cada acción de un ejecutor.
SERVICE_OF = {"notify_customer": "mail", "file_claim": "claims", "reissue_shipment": "carrier",
              "issue_refund": "payments"}


@dataclass
class FakeService:
    name: str
    prefix: str
    down: bool = False
    timeouts: int = 0
    reject: str | None = None  # error de negocio que devolverá cada llamada
    idempotent: bool = True
    calls: int = 0
    executions: Counter[str] = field(default_factory=Counter)
    on_execute: Callable[[str, str, str], None] | None = field(default=None, repr=False)
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
            if self.idempotent and key in self._done:
                return self._done[key]
            self._done[key] = f"{self.prefix}-{uuid.uuid4().hex[:8]}"
            self.executions[key] += 1
            if self.on_execute is not None:
                self.on_execute(self.name, key, self._done[key])
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
