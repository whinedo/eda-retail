"""Catálogo de comandos (docs/README.md). Solo existen dentro del command handler.

Todo comando lleva `command_id` (el `event_id` del evento que lo originó, que será el
`causation_id` de los eventos resultantes) y `expected_version` (concurrencia optimista).
"""

from __future__ import annotations

import uuid
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class Command(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    command_id: uuid.UUID
    expected_version: int = Field(ge=0)
    producer: str = Field(min_length=1)

    @property
    def name(self) -> str:
        return type(self).__name__


class RegisterCarrierNotice(Command):
    """Entrada del sistema: no hay evento previo, así que no hay `causation_id`."""

    carrier_notice_id: str = Field(min_length=1)
    carrier: str = Field(min_length=1)
    raw_text: str


class IncidentCommand(Command):
    incident_id: str = Field(min_length=1)


class OpenIncident(IncidentCommand):
    carrier_notice_id: str
    order_ids: list[str]
    cause: str
    estimated_delay_hours: int
    severity: str


class CreatePlan(IncidentCommand):
    # Pasos sin tipar: el handler distingue cada motivo de rechazo (acción, compensación…).
    steps: list[dict[str, Any]]


class RevisePlan(IncidentCommand):
    steps: list[dict[str, Any]]
    reason: str = Field(min_length=1)


class RequestStep(IncidentCommand):
    step_id: str = Field(min_length=1)


class RecordStepResult(IncidentCommand):
    step_id: str = Field(min_length=1)
    outcome: Literal["completed", "failed"]
    result: dict[str, Any] | None = None
    error: str | None = None
    retryable: bool | None = None
    effect: dict[str, Any] | None = None  # payload del evento de efecto (CustomerNotified…)


class CloseIncident(IncidentCommand):
    outcome: Literal["resolved", "refunded"]


class EscalateIncident(IncidentCommand):
    reason: str = Field(min_length=1)
