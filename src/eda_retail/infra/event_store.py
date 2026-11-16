"""Validación de la estructura común de un evento antes de añadirlo al outbox de un componente
(`infra.outbox.append`)."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError

from eda_retail.events.envelope import EventEnvelope


class InvalidEventError(ValueError):
    """Evento rechazado porque no cumple la estructura común."""


def _validate(event: EventEnvelope | Mapping[str, Any]) -> EventEnvelope:
    if isinstance(event, EventEnvelope):
        return event
    try:
        return EventEnvelope.model_validate(event)
    except ValidationError as exc:
        fields = ", ".join(".".join(str(p) for p in err["loc"]) for err in exc.errors())
        raise InvalidEventError(f"evento rechazado, campos ausentes o inválidos: {fields}") from exc


validate_event = _validate
