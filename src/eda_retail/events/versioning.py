"""Conversores de versión al leer (spec event-backbone, «Versionado de eventos»).

El log nunca se reescribe: quien lee un evento de una versión anterior lo convierte a la
que espera encadenando conversores v→v+1. Cada lector (p. ej. un proyector) usa su
registro.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from eda_retail.events.envelope import EventEnvelope

Converter = Callable[[dict[str, Any]], dict[str, Any]]


class UpcastError(ValueError):
    """No hay conversores para llevar el evento a la versión pedida."""


class Upcasters:
    def __init__(self) -> None:
        self._converters: dict[tuple[str, int], Converter] = {}

    def register(self, event_type: str, from_version: int) -> Callable[[Converter], Converter]:
        """Registra el conversor de `from_version` a `from_version + 1`."""

        def decorate(fn: Converter) -> Converter:
            self._converters[(event_type, from_version)] = fn
            return fn

        return decorate

    def upcast(self, event: EventEnvelope, target_version: int) -> EventEnvelope:
        """Devuelve una copia en `target_version`; el evento original no cambia."""
        if target_version < event.event_version:
            raise UpcastError(
                f"{event.event_type}: no se baja de la versión {event.event_version} a {target_version}"
            )
        payload = dict(event.payload)
        for version in range(event.event_version, target_version):
            converter = self._converters.get((event.event_type, version))
            if converter is None:
                raise UpcastError(f"{event.event_type}: falta el conversor de la versión {version}")
            payload = converter(payload)
        return event.model_copy(update={"payload": payload, "event_version": target_version})
