"""Caída del broker (spec synthetic-data, «Caída del broker»; E9).

`BrokerOutage` corta el broker de Kafka sobre su contenedor de Docker (`KAFKA_CONTAINER`, por defecto `eda-kafka`, el
`container_name` de docker-compose.yml): `stopped` lo para y lo arranca (las conexiones se rechazan) y `paused` lo
congela y lo descongela (las conexiones quedan abiertas sin respuesta). Se usa como contexto. Con `duration_s`, un
temporizador devuelve el broker a su estado a los `duration_s` segundos de cortarlo, aunque el hilo que lo usa esté
bloqueado en una llamada a Kafka; así el corte dura lo previsto. Al salir, también por una excepción, lo devuelve si aún
no lo estaba y espera a que responda (los metadatos del topic), de modo que una ejecución fallida no deja sin Kafka a
los tests que vienen después. Si el propio corte falla, se intenta devolver el broker antes de propagar el error.

`ContainerOutage` es el corte genérico de un contenedor; `BrokerOutage` lo concreta para Kafka y
`synthetic.pg_faults.PostgresOutage` para PostgreSQL (E10).
"""

from __future__ import annotations

import contextlib
import logging
import os
import subprocess
import threading
import time
from collections.abc import Callable, Sequence
from types import TracebackType
from typing import Any, Self

from confluent_kafka.admin import AdminClient

log = logging.getLogger(__name__)

MODES = ("stopped", "paused")
CUT = {"stopped": "stop", "paused": "pause"}
RESTORE = {"stopped": "start", "paused": "unpause"}
DEFAULT_CONTAINER = "eda-kafka"
READY_TIMEOUT_S = 120.0
READY_POLL_S = 1.0


def kafka_container() -> str:
    return os.environ.get("KAFKA_CONTAINER", DEFAULT_CONTAINER)


def docker(args: Sequence[str]) -> None:
    subprocess.run(["docker", *args], check=True, capture_output=True, timeout=120)


def broker_ready(bootstrap_servers: str, topic: str) -> Callable[[], bool]:
    """True cuando el broker devuelve los metadatos del topic."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    def ready() -> bool:
        try:
            metadata = admin.list_topics(topic=topic, timeout=2)
        except Exception:  # noqa: BLE001 — broker aún sin responder
            return False
        info = metadata.topics.get(topic)
        return info is not None and info.error is None and bool(info.partitions)

    return ready


class ContainerOutage:
    """Corte de un contenedor de Docker durante el bloque `with`. `run` y `ready` solo los cambian los tests. Cada
    subclase da su variable de entorno con el contenedor (`container_env`), su contenedor por defecto y cómo nombra
    lo que corta en los logs y errores (`cut_label`, «del broker»; `subject`, «el broker»)."""

    container_env = ""
    default_container = ""
    cut_label = "del contenedor"
    subject = "el contenedor"

    def __init__(self, mode: str, *, ready: Callable[[], bool], container: str | None = None,
                 run: Callable[[Sequence[str]], Any] = docker, ready_timeout_s: float = READY_TIMEOUT_S,
                 clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep,
                 duration_s: float | None = None, timer: Callable[..., Any] = threading.Timer) -> None:
        if mode not in MODES:
            raise ValueError(f"modo de corte desconocido: {mode}")
        self.mode = mode
        self.container = container or os.environ.get(self.container_env, self.default_container)
        self._ready = ready
        self._run = run
        self._ready_timeout_s = ready_timeout_s
        self._clock = clock
        self._sleep = sleep
        self._duration_s = duration_s
        self._timer_factory = timer
        self._timer: Any = None
        self._lock = threading.Lock()
        self.restoring = False  # se marca antes de devolverlo: tras `docker unpause` responde antes de volver
        self.cut_at: float | None = None
        self.restored_at: float | None = None
        self.ready_at: float | None = None

    @property
    def restored(self) -> bool:
        return self.restored_at is not None

    def __enter__(self) -> Self:
        log.warning("corte %s: docker %s %s", self.cut_label, CUT[self.mode], self.container)
        try:
            self._run([CUT[self.mode], self.container])
        except BaseException:
            with contextlib.suppress(Exception):  # el corte pudo aplicarse antes de fallar (p. ej., un timeout)
                self._run([RESTORE[self.mode], self.container])
            raise
        self.cut_at = self._clock()
        if self._duration_s is not None:
            self._timer = self._timer_factory(self._duration_s, self._restore_in_timer)
            self._timer.daemon = True
            self._timer.start()
        return self

    def _restore(self) -> None:
        with self._lock:
            if self.restored_at is not None:
                return
            log.warning("fin del corte %s: docker %s %s", self.cut_label, RESTORE[self.mode], self.container)
            self.restoring = True
            self._run([RESTORE[self.mode], self.container])
            self.restored_at = self._clock()

    def _restore_in_timer(self) -> None:
        try:
            self._restore()
        except Exception:  # se reintenta al salir del contexto
            log.exception("no se pudo devolver %s a su estado desde el temporizador", self.subject)

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                 tb: TracebackType | None) -> None:
        if self._timer is not None:
            self._timer.cancel()
        try:
            self._restore()
        except Exception:
            if exc is None:
                raise
            log.exception("no se pudo devolver %s a su estado", self.subject)  # la excepción original sigue su curso
            return
        deadline = self.restored_at + self._ready_timeout_s
        while not self._ready():
            if self._clock() > deadline:
                if exc is None:
                    raise TimeoutError(f"{self.subject} no responde {self._ready_timeout_s} s después del corte")
                return
            self._sleep(READY_POLL_S)
        self.ready_at = self._clock()

    def report(self) -> dict[str, Any]:
        def span(a: float | None, b: float | None) -> float | None:
            return round(b - a, 3) if a is not None and b is not None else None

        return {"mode": self.mode, "cut_seconds": span(self.cut_at, self.restored_at),
                "ready_seconds": span(self.restored_at, self.ready_at)}


class BrokerOutage(ContainerOutage):
    """Corte del broker de Kafka (`KAFKA_CONTAINER`, por defecto `eda-kafka`)."""

    container_env = "KAFKA_CONTAINER"
    default_container = DEFAULT_CONTAINER
    cut_label = "del broker"
    subject = "el broker"
