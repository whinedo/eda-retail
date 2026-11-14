"""Inyección de fallos configurable y reproducible (spec synthetic-data).

- Entrega (`FaultyDelivery`): cada mensaje se entrega `duplicates` veces al consumidor antes
  de confirmar el offset (entrega al menos una vez), puede retenerse `delay_seconds` y el
  consumidor puede caer tras procesar K mensajes, sin confirmar el último offset.
- Pasos (`FailingAction`): un tipo de paso falla con una probabilidad dada. La elección es
  una función de la semilla, la acción y la `idempotency_key` del paso (`should_fail`), no
  de un generador compartido: con la misma semilla fallan los mismos pasos aunque cambie el
  orden en que se ejecutan. El fallo es de negocio (no reintentable): fuerza la revisión
  del plan, y el efecto no se ejecuta.
- Existencias (`empty_warehouses`): deja sin stock los almacenes indicados.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

import psycopg

from eda_retail.events.catalog import Action
from eda_retail.executors.base import BusinessError, Effect, Prepared, StepAction, StepRequest

INJECTED_FAILURE = "injected_failure"


@dataclass(frozen=True)
class FaultPlan:
    seed: int = 0
    duplicates: int = 1  # entregas de cada mensaje (1 = sin duplicados)
    delay_seconds: float = 0.0
    fail_probability: Mapping[str, float] = field(default_factory=dict)  # acción -> probabilidad
    crash_after: Mapping[str, int] = field(default_factory=dict)  # consumidor -> cae tras K mensajes
    empty_warehouses: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.duplicates < 1:
            raise ValueError("duplicates debe ser al menos 1")
        if self.delay_seconds < 0:
            raise ValueError("delay_seconds no puede ser negativo")
        allowed = {a.value for a in Action}
        for action, p in self.fail_probability.items():
            if action not in allowed:
                raise ValueError(f"acción fuera de la lista permitida: {action}")
            if not 0.0 <= p <= 1.0:
                raise ValueError(f"probabilidad fuera de [0, 1] para {action}: {p}")
        if any(k < 1 for k in self.crash_after.values()):
            raise ValueError("crash_after debe ser al menos 1")


def should_fail(seed: int, action: str, key: str, probability: float) -> bool:
    """Decide de forma determinista si el paso con esa clave falla."""
    if probability <= 0.0:
        return False
    digest = hashlib.sha256(f"{seed}:{action}:{key}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64 < probability


class FailingAction:
    """Envuelve una acción: los pasos elegidos por `should_fail` fallan sin ejecutar el efecto."""

    def __init__(self, inner: StepAction, plan: FaultPlan) -> None:
        self.inner = inner
        self.action = inner.action
        self._seed = plan.seed
        self._probability = plan.fail_probability.get(inner.action, 0.0)

    def prepare(self, conn: psycopg.Connection, request: StepRequest) -> Prepared:
        return self.inner.prepare(conn, request)

    def execute(self, conn: psycopg.Connection, request: StepRequest, prepared: Prepared) -> Effect:
        if should_fail(self._seed, self.action, request.idempotency_key, self._probability):
            raise BusinessError(INJECTED_FAILURE)
        return self.inner.execute(conn, request, prepared)


class ConsumerCrashed(RuntimeError):
    """El consumidor cae a propósito: el último mensaje queda sin confirmar."""


class _Consumer(Protocol):
    name: str

    def poll(self, timeout: float) -> Any: ...
    def handle_message(self, msg: Any) -> Any: ...
    def commit(self, msg: Any) -> None: ...


class FaultyDelivery:
    """Bucle de entrega de un consumidor con duplicados, retraso y caída.

    `crashed` guarda qué consumidores ya cayeron: al reiniciar con el mismo plan (proceso
    nuevo), se pasa para que no vuelvan a caer.
    """

    def __init__(
        self, consumer: _Consumer, plan: FaultPlan, *, sleep: Callable[[float], None] = time.sleep,
        crashed: set[str] | None = None,
    ) -> None:
        self.consumer = consumer
        self.plan = plan
        self.crashed = crashed if crashed is not None else set()
        self._sleep = sleep
        self._handled = 0
        self.current: Any = None  # último mensaje recibido (el arnés lo salta si es veneno)

    def run_once(self, timeout: float = 1.0) -> Any:
        msg = self.consumer.poll(timeout)
        if msg is None:
            return None
        self.current = msg
        if self.plan.delay_seconds:
            self._sleep(self.plan.delay_seconds)
        outcome = None
        for _ in range(self.plan.duplicates):
            outcome = self.consumer.handle_message(msg)
        self._handled += 1
        name = self.consumer.name
        limit = self.plan.crash_after.get(name)
        if limit is not None and name not in self.crashed and self._handled >= limit:
            self.crashed.add(name)
            raise ConsumerCrashed(f"{name} cae tras {self._handled} mensajes")
        self.consumer.commit(msg)
        return outcome


def empty_warehouses(conn: psycopg.Connection, warehouses: list[str] | tuple[str, ...]) -> int:
    """Deja sin existencias los almacenes indicados. Devuelve cuántas filas cambió."""
    with conn.transaction():
        return conn.execute(
            "UPDATE stock SET available = 0 WHERE warehouse = ANY(%s)", (list(warehouses),)
        ).rowcount
