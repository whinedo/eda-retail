"""Estructura común de todo evento (spec event-backbone)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class EventEnvelope(BaseModel):
    """Campos obligatorios de todo evento. Ninguno tiene valor por defecto: si falta, se rechaza."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: uuid.UUID
    event_type: str = Field(min_length=1)
    event_version: int = Field(ge=1)
    occurred_at: datetime
    correlation_id: str = Field(min_length=1)
    causation_id: uuid.UUID | None
    producer: str = Field(min_length=1)
    payload: dict[str, Any]


def new_event(
    *,
    event_type: str,
    correlation_id: str,
    causation_id: uuid.UUID | None,
    producer: str,
    payload: dict[str, Any],
    event_version: int = 1,
) -> EventEnvelope:
    """Crea un evento nuevo con `event_id` y `occurred_at` generados."""
    return EventEnvelope(
        event_id=uuid.uuid4(),
        event_type=event_type,
        event_version=event_version,
        occurred_at=datetime.now(UTC),
        correlation_id=correlation_id,
        causation_id=causation_id,
        producer=producer,
        payload=payload,
    )
