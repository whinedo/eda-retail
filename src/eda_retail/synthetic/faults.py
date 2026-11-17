"""Inyección de fallos configurable y reproducible (spec synthetic-data).

- Entrega (`FaultyDelivery`): cada mensaje se entrega `duplicates` veces al consumidor antes
  de confirmar el offset (entrega al menos una vez), puede retenerse `delay_seconds` y el
  consumidor puede caer tras procesar K mensajes, sin confirmar el último offset.
- Pasos (`should_fail`): un tipo de paso falla con una probabilidad dada. La elección es una
  función de la semilla, la acción y la `idempotency_key` del paso, no de un generador
  compartido: con la misma semilla fallan los mismos pasos aunque cambie el orden en que se
  ejecutan. El fallo es de negocio (no reintentable): fuerza la revisión del plan, y el
  efecto no se ejecuta (el arnés envuelve el `ExecuteStep` con `experiments.harness.failing`).
- Existencias (`empty_warehouses` del plan): el arnés siembra a 0 esos almacenes.
- Offsets (`reset_group_offsets`): con el sistema parado, lleva los grupos de consumidores al principio
  del topic para reproducir el log completo (E21); `group_lag` dice cuánto les falta por leer.
- Resultados retrasados (`step_delay_seconds`, E11): el efecto del paso se ejecuta a su hora, pero sus
  eventos de resultado se añaden al outbox del ejecutor `step_delay` segundos después (`base` por un jitter
  determinista dada la semilla, la acción y la `idempotency_key`; el arnés envuelve el `ExecuteStep` con
  `experiments.harness.delaying`).
- Mensajes veneno (E18): se publican directamente en el topic con `synthetic.poison`.
- Proyectores (E17): el arnés los para y los reanuda (`Harness.pause_projectors`, `resume_projectors`).
- Entrega concurrente (`concurrent_commands`, E22): el comando de ese tipo se entrega desde N hilos a la vez, cada
  uno con su conexión y el mismo `command_id` (`Harness` e `InProcessCommands`).
- Reinvocación (`reinvoke_steps`, E22): el `ExecuteStep` de esas acciones llega a la vez con su `command_id` y con
  otro nuevo, con la misma `idempotency_key` (como una reinvocación de la saga tras un plazo).
- Caída entre el efecto y el registro (`crash_between_effect_and_record`, E22): en el primer intento de cada clave de
  esas acciones, el ejecutor llama al servicio y cae antes de la transacción que registra la clave
  (`EffectRecordCrashes`; el arnés envuelve el `ExecuteStep` con `experiments.harness.crashing`). El trabajo se
  reintenta con el mismo `command_id` y la misma clave.
"""

from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition

from eda_retail.events.catalog import Action

INJECTED_FAILURE = "injected_failure"
# Comandos que admiten entrega concurrente (spec synthetic-data, «Entrega concurrente de comandos»)
CONCURRENT_COMMANDS = frozenset({"ExecuteStep", "CreatePlan", "RevisePlan", "RegisterCarrierNotice", "SeedSampleData"})


@dataclass(frozen=True)
class FaultPlan:
    seed: int = 0
    duplicates: int = 1  # entregas de cada mensaje (1 = sin duplicados)
    delay_seconds: float = 0.0
    fail_probability: Mapping[str, float] = field(default_factory=dict)  # acción -> probabilidad
    crash_after: Mapping[str, int] = field(default_factory=dict)  # consumidor -> cae tras K mensajes
    empty_warehouses: tuple[str, ...] = ()
    step_delay_seconds: Mapping[str, float] = field(default_factory=dict)  # acción -> retraso base del resultado
    concurrent_commands: Mapping[str, int] = field(default_factory=dict)  # tipo de comando -> hilos a la vez
    reinvoke_steps: tuple[str, ...] = ()  # acciones cuyo ExecuteStep llega también con otro command_id
    crash_between_effect_and_record: tuple[str, ...] = ()  # acciones que caen tras el efecto en su primer intento

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
        for action, seconds in self.step_delay_seconds.items():
            if action not in allowed:
                raise ValueError(f"acción fuera de la lista permitida: {action}")
            if seconds < 0:
                raise ValueError(f"step_delay_seconds no puede ser negativo para {action}: {seconds}")
        for command_type, threads in self.concurrent_commands.items():
            if command_type not in CONCURRENT_COMMANDS:
                raise ValueError(f"comando sin entrega concurrente: {command_type}")
            if threads < 1:
                raise ValueError(f"concurrent_commands debe ser al menos 1 para {command_type}")
        for action in (*self.reinvoke_steps, *self.crash_between_effect_and_record):
            if action not in allowed:
                raise ValueError(f"acción fuera de la lista permitida: {action}")


class EffectRecordCrash(RuntimeError):
    """El ejecutor cae a propósito tras el efecto externo y antes de registrar su clave."""


class EffectRecordCrashes:
    """Decide la caída entre el efecto y el registro: el primer intento de cada clave de las acciones del plan.
    `crashed` guarda las claves que ya cayeron (con su acción). Seguro entre hilos (varios workers)."""

    def __init__(self, plan: FaultPlan) -> None:
        self._actions = frozenset(plan.crash_between_effect_and_record)
        self._lock = threading.Lock()
        self.crashed: dict[str, str] = {}

    def should_crash(self, action: str, key: str) -> bool:
        if action not in self._actions:
            return False
        with self._lock:
            if key in self.crashed:
                return False
            self.crashed[key] = action
            return True


def step_delay(seed: int, action: str, key: str, base: float) -> float:
    """Retraso del resultado del paso con esa clave: `base` por un jitter determinista en [0,5, 1,5)."""
    if base <= 0.0:
        return 0.0
    digest = hashlib.sha256(f"{seed}:delay:{action}:{key}".encode()).digest()
    return base * (0.5 + int.from_bytes(digest[:8], "big") / 2**64)


def should_fail(seed: int, action: str, key: str, probability: float) -> bool:
    """Decide de forma determinista si el paso con esa clave falla."""
    if probability <= 0.0:
        return False
    digest = hashlib.sha256(f"{seed}:{action}:{key}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64 < probability


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


def reset_group_offsets(admin: Any, groups: Iterable[str], topic: str, partitions: Iterable[int],
                        timeout: float = 30.0) -> None:
    """Lleva los offsets de cada grupo al offset 0 de cada partición de `topic` (spec synthetic-data,
    «Reinicio de offsets de un grupo»). Con el sistema parado: Kafka no cambia los offsets de un grupo
    con miembros activos. Una petición por grupo, que es lo que admite la API de administración."""
    parts = [TopicPartition(topic, p, 0) for p in sorted(partitions)]
    for group in groups:
        for future in admin.alter_consumer_group_offsets([ConsumerGroupTopicPartitions(group, parts)]).values():
            future.result(timeout=timeout)


def group_lag(admin: Any, group: str, topic: str, highs: Mapping[int, int], timeout: float = 30.0) -> int:
    """Mensajes que le faltan al grupo hasta las marcas de agua altas (`highs`, por partición)."""
    request = ConsumerGroupTopicPartitions(group, [TopicPartition(topic, p) for p in sorted(highs)])
    [future] = admin.list_consumer_group_offsets([request]).values()
    committed = {tp.partition: max(tp.offset, 0) for tp in future.result(timeout=timeout).topic_partitions}
    return sum(max(0, high - committed.get(p, 0)) for p, high in highs.items())
